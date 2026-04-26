"""
Volt interactive REPL — direct in-process access (no TCP).

    python3 -m volt                          # in-memory (ephemeral)
    python3 -m volt --aof /tmp/volt.aof      # with AOF persistence
    python3 -m volt --port 6399 --server     # start TCP server instead

Supports the same command set as the TCP server.
SUBSCRIBE works interactively: messages are printed as they arrive.
"""
from __future__ import annotations

import argparse
import queue
import sys
import threading

from .engine import VoltDB, _tokenise
from .protocol import resp


def _decode_resp_for_print(raw: bytes) -> str:
    """Turn RESP bytes into a human-readable string for the REPL."""
    from .protocol.resp import decode as _decode
    try:
        val, _ = _decode(raw, 0)
        return _fmt(val)
    except Exception:
        return raw.decode('utf-8', errors='replace').strip()


def _fmt(val, indent: int = 0) -> str:
    pad = '  ' * indent
    if val is None:
        return '(nil)'
    if isinstance(val, int):
        return f'(integer) {val}'
    if isinstance(val, str):
        if val.startswith('ERR ') or val.startswith('WRONGTYPE'):
            return f'(error) {val}'
        return f'"{val}"'
    if isinstance(val, list):
        if not val:
            return '(empty array)'
        lines = []
        for i, item in enumerate(val, 1):
            lines.append(f'{pad}{i}) {_fmt(item, indent + 1)}')
        return '\n'.join(lines)
    return str(val)


class REPL:
    PROMPT = 'volt> '

    def __init__(self, db: VoltDB):
        self._db  = db
        self._sub_mode = False
        self._sub_queue: queue.Queue = queue.Queue()

    def run(self):
        print("Volt in-process shell  (CTRL+C or QUIT to exit)")
        print()
        while True:
            try:
                line = input(self.PROMPT).strip()
            except (EOFError, KeyboardInterrupt):
                print()
                break

            if not line:
                continue
            if line.lower() in ('quit', 'exit'):
                break

            parts = _tokenise(line)
            if not parts:
                continue

            name = parts[0].upper()

            if name == 'SUBSCRIBE':
                self._handle_subscribe(parts[1:])
                continue

            raw = self._db.execute(parts)
            print(_decode_resp_for_print(raw))

    def _handle_subscribe(self, channels: list):
        if not channels:
            print('(error) ERR wrong number of arguments for subscribe')
            return

        def _cb(ch, msg):
            self._sub_queue.put((ch, msg))

        for ch in channels:
            count = self._db.pubsub.subscribe(ch, _cb)
            print(f'1) "subscribe"\n2) "{ch}"\n3) (integer) {count}')

        print("Waiting for messages. CTRL+C to unsubscribe.")
        try:
            while True:
                ch, msg = self._sub_queue.get(timeout=0.2)
                print(f'1) "message"\n2) "{ch}"\n3) "{msg}"')
        except queue.Empty:
            pass
        except KeyboardInterrupt:
            print()
        finally:
            for ch in channels:
                self._db.pubsub.unsubscribe(ch, _cb)


def main():
    parser = argparse.ArgumentParser(description='Volt shell')
    parser.add_argument('--aof', default=None,
                        help='Path to AOF file for persistence')
    parser.add_argument('--fsync', default='everysec',
                        choices=['always', 'everysec', 'no'])
    parser.add_argument('--server', action='store_true',
                        help='Start TCP server instead of interactive shell')
    parser.add_argument('--host', default='127.0.0.1')
    parser.add_argument('--port', type=int, default=6399)
    args = parser.parse_args()

    if args.server:
        from .server import run_server
        run_server(args.host, args.port, args.aof, args.fsync)
        return

    db   = VoltDB(aof_path=args.aof, fsync=args.fsync)
    repl = REPL(db)
    try:
        repl.run()
    finally:
        db.shutdown()
    print('bye.')


if __name__ == '__main__':
    main()
