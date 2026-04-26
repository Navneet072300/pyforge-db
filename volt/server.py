"""
Volt TCP server — speaks the RESP2 protocol on the wire.

Fully compatible with redis-cli and any Redis client library.

    python3 -m volt.server --port 6399
    redis-cli -p 6399 PING

SUBSCRIBE/PUBLISH are handled specially: subscribed clients enter a
push-mode where the server delivers messages via asyncio queues.
"""
from __future__ import annotations

import asyncio
import os
import signal
from typing import Optional, Set

from .engine import VoltDB
from .protocol.resp import RESPParser, ok, error, array, bulk, integer, simple


DEFAULT_PORT = 6399
DEFAULT_HOST = '127.0.0.1'


class VoltClientHandler:
    """
    Per-connection state machine.
    Normal mode:  request → response
    Subscribe mode: server pushes messages; only SUBSCRIBE/UNSUBSCRIBE/PING/QUIT
                    are accepted.
    """

    def __init__(self, db: VoltDB,
                 reader: asyncio.StreamReader,
                 writer: asyncio.StreamWriter):
        self._db     = db
        self._reader = reader
        self._writer = writer
        self._parser = RESPParser()
        self._subscriptions: Set[str] = set()
        self._queue: asyncio.Queue = asyncio.Queue()
        self._subscribe_mode = False

    async def run(self):
        try:
            await asyncio.gather(
                self._read_loop(),
                self._write_loop(),
            )
        except (asyncio.CancelledError, ConnectionResetError, BrokenPipeError):
            pass
        finally:
            self._cleanup()

    async def _read_loop(self):
        while True:
            try:
                data = await self._reader.read(4096)
            except Exception:
                break
            if not data:
                break
            self._parser.feed(data)
            while True:
                cmd = self._parser.get_command()
                if cmd is None:
                    break
                await self._dispatch(cmd)

    async def _write_loop(self):
        while True:
            msg = await self._queue.get()
            if msg is None:
                break
            try:
                self._writer.write(msg)
                await self._writer.drain()
            except Exception:
                break

    async def _dispatch(self, parts):
        if not parts:
            return
        name = parts[0].upper()

        if name == 'QUIT':
            await self._send(simple('OK'))
            await self._queue.put(None)
            return

        if self._subscribe_mode:
            if name == 'SUBSCRIBE':
                for ch in parts[1:]:
                    await self._subscribe(ch)
            elif name == 'UNSUBSCRIBE':
                channels = parts[1:] if parts[1:] else list(self._subscriptions)
                for ch in channels:
                    await self._unsubscribe(ch)
                if not self._subscriptions:
                    self._subscribe_mode = False
            elif name == 'PING':
                await self._send(array(['pong', parts[1] if len(parts) > 1 else '']))
            # All other commands are silently ignored in subscribe mode
            return

        if name == 'SUBSCRIBE':
            self._subscribe_mode = True
            for ch in parts[1:]:
                await self._subscribe(ch)
            return

        if name == 'PUBLISH':
            result = self._db.execute(parts)
            await self._send(result)
            return

        result = self._db.execute(parts)
        await self._send(result)

    async def _subscribe(self, channel: str):
        self._subscriptions.add(channel)
        count = self._db.pubsub.subscribe(channel, self._on_message)
        await self._send(array(['subscribe', channel, count]))

    async def _unsubscribe(self, channel: str):
        if channel in self._subscriptions:
            self._subscriptions.discard(channel)
            self._db.pubsub.unsubscribe(channel, self._on_message)
        count = len(self._subscriptions)
        await self._send(array(['unsubscribe', channel, count]))

    def _on_message(self, channel: str, message: str):
        """Called from the pubsub thread — enqueue for the write loop."""
        msg = array(['message', channel, message])
        asyncio.get_event_loop().call_soon_threadsafe(
            self._queue.put_nowait, msg)

    async def _send(self, data: bytes):
        await self._queue.put(data)

    def _cleanup(self):
        self._db.pubsub.unsubscribe_all(self._on_message)
        asyncio.get_event_loop().call_soon_threadsafe(
            self._queue.put_nowait, None)
        try:
            self._writer.close()
        except Exception:
            pass


class VoltServer:
    def __init__(self, host: str = DEFAULT_HOST,
                 port: int = DEFAULT_PORT,
                 aof_path: Optional[str] = None,
                 fsync: str = 'everysec'):
        self._host = host
        self._port = port
        self._db   = VoltDB(aof_path=aof_path, fsync=fsync)
        self._server = None

    async def start(self):
        self._server = await asyncio.start_server(
            self._handle_client, self._host, self._port)
        addr = self._server.sockets[0].getsockname()
        print(f'Volt listening on {addr[0]}:{addr[1]}')
        async with self._server:
            await self._server.serve_forever()

    async def _handle_client(self, reader, writer):
        handler = VoltClientHandler(self._db, reader, writer)
        await handler.run()

    def shutdown(self):
        if self._server:
            self._server.close()
        self._db.shutdown()


def run_server(host=DEFAULT_HOST, port=DEFAULT_PORT,
               aof_path=None, fsync='everysec'):
    server = VoltServer(host, port, aof_path, fsync)
    try:
        asyncio.run(server.start())
    except KeyboardInterrupt:
        pass
    finally:
        server.shutdown()


if __name__ == '__main__':
    import argparse
    p = argparse.ArgumentParser(description='Volt server')
    p.add_argument('--host', default=DEFAULT_HOST)
    p.add_argument('--port', type=int, default=DEFAULT_PORT)
    p.add_argument('--aof',  default=None, help='AOF file path')
    p.add_argument('--fsync', default='everysec',
                   choices=['always', 'everysec', 'no'])
    args = p.parse_args()
    run_server(args.host, args.port, args.aof, args.fsync)
