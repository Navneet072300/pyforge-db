"""
Append-Only File (AOF) — write-ahead log for Volt persistence.

Every write command is serialised as a RESP array and appended.
On startup, the file is replayed to reconstruct in-memory state.

Fsync policy:
  'always'   — fsync after every write  (safest)
  'everysec' — fsync once per second    (default, good balance)
  'no'       — let the OS decide        (fastest)
"""
from __future__ import annotations

import os
import threading
import time
from typing import Callable, List


class AOF:
    def __init__(self, path: str, fsync: str = 'everysec'):
        self.path   = path
        self._fsync = fsync
        os.makedirs(os.path.dirname(path) if os.path.dirname(path) else '.', exist_ok=True)
        self._f = open(path, 'ab')
        self._lock = threading.Lock()
        self._last_sync = time.monotonic()

        if fsync == 'everysec':
            self._sync_thread = threading.Thread(
                target=self._bg_sync, daemon=True, name='aof-sync')
            self._sync_thread.start()

    # ── write ─────────────────────────────────────────────────────────────────

    def log(self, *args: str):
        """Append a command (as RESP array) to the AOF."""
        buf = _encode_resp(args)
        with self._lock:
            self._f.write(buf)
            if self._fsync == 'always':
                self._f.flush()
                os.fsync(self._f.fileno())

    # ── replay ────────────────────────────────────────────────────────────────

    def replay(self, executor: Callable[[List[str]], None]):
        """Read existing AOF and call executor(cmd_parts) for each entry."""
        if not os.path.exists(self.path):
            return
        with open(self.path, 'rb') as f:
            data = f.read()
        pos = 0
        while pos < len(data):
            try:
                cmd, pos = _decode_resp(data, pos)
                if cmd:
                    executor(cmd)
            except Exception:
                break   # truncated / corrupted tail — stop here

    # ── rewrite ───────────────────────────────────────────────────────────────

    def rewrite(self, snapshot_commands: List[List[str]]):
        """Replace AOF with a minimal snapshot (BGREWRITEAOF)."""
        tmp = self.path + '.tmp'
        with open(tmp, 'wb') as f:
            for cmd in snapshot_commands:
                f.write(_encode_resp(cmd))
        with self._lock:
            self._f.close()
            os.replace(tmp, self.path)
            self._f = open(self.path, 'ab')

    def close(self):
        with self._lock:
            self._f.flush()
            os.fsync(self._f.fileno())
            self._f.close()

    # ── internals ─────────────────────────────────────────────────────────────

    def _bg_sync(self):
        while True:
            time.sleep(1)
            with self._lock:
                try:
                    self._f.flush()
                    os.fsync(self._f.fileno())
                except OSError:
                    pass


def _encode_resp(args) -> bytes:
    parts = [f'*{len(args)}\r\n'.encode()]
    for a in args:
        b = str(a).encode('utf-8')
        parts.append(f'${len(b)}\r\n'.encode() + b + b'\r\n')
    return b''.join(parts)


def _decode_resp(data: bytes, pos: int):
    """Parse one RESP array starting at pos. Returns (list[str], new_pos)."""
    assert data[pos:pos+1] == b'*'
    end = data.index(b'\r\n', pos)
    count = int(data[pos+1:end])
    pos = end + 2
    items = []
    for _ in range(count):
        assert data[pos:pos+1] == b'$'
        end = data.index(b'\r\n', pos)
        length = int(data[pos+1:end])
        pos = end + 2
        items.append(data[pos:pos+length].decode('utf-8'))
        pos += length + 2   # skip \r\n
    return items, pos
