"""
RESP2 (Redis Serialization Protocol) encoder / decoder.

Wire format:
  Simple string : +OK\r\n
  Error         : -ERR message\r\n
  Integer       : :42\r\n
  Bulk string   : $6\r\nfoobar\r\n    | $-1\r\n  (nil)
  Array         : *2\r\n$3\r\nfoo\r\n$3\r\nbar\r\n | *-1\r\n (nil)
"""
from __future__ import annotations
from typing import Any, List, Optional, Tuple


# ── Python → RESP bytes ───────────────────────────────────────────────────────

def encode(value: Any) -> bytes:
    if value is None:
        return b'$-1\r\n'
    if isinstance(value, bool):
        return encode(int(value))
    if isinstance(value, int):
        return f':{value}\r\n'.encode()
    if isinstance(value, float):
        return encode(str(value))
    if isinstance(value, bytes):
        return b'$' + str(len(value)).encode() + b'\r\n' + value + b'\r\n'
    if isinstance(value, str):
        b = value.encode('utf-8')
        return b'$' + str(len(b)).encode() + b'\r\n' + b + b'\r\n'
    if isinstance(value, (list, tuple)):
        if value is None:
            return b'*-1\r\n'
        parts = [f'*{len(value)}\r\n'.encode()]
        for item in value:
            parts.append(encode(item))
        return b''.join(parts)
    return encode(str(value))


def ok() -> bytes:
    return b'+OK\r\n'


def simple(s: str) -> bytes:
    return f'+{s}\r\n'.encode()


def error(msg: str) -> bytes:
    return f'-ERR {msg}\r\n'.encode()


def wrong_type() -> bytes:
    return b'-WRONGTYPE Operation against a key holding the wrong kind of value\r\n'


def nil_array() -> bytes:
    return b'*-1\r\n'


def nil_bulk() -> bytes:
    return b'$-1\r\n'


def integer(n: int) -> bytes:
    return f':{n}\r\n'.encode()


def bulk(s: Optional[str]) -> bytes:
    if s is None:
        return b'$-1\r\n'
    b = s.encode('utf-8')
    return b'$' + str(len(b)).encode() + b'\r\n' + b + b'\r\n'


def array(items: Optional[List]) -> bytes:
    if items is None:
        return b'*-1\r\n'
    parts = [f'*{len(items)}\r\n'.encode()]
    for item in items:
        parts.append(encode(item))
    return b''.join(parts)


# ── RESP bytes → Python ───────────────────────────────────────────────────────

class RESPError(Exception):
    pass


class Incomplete(Exception):
    """Raised when the buffer does not yet contain a full message."""
    pass


def decode(data: bytes, pos: int = 0) -> Tuple[Any, int]:
    """Parse one RESP value starting at `pos`. Returns (value, new_pos)."""
    if pos >= len(data):
        raise Incomplete()
    t = chr(data[pos])
    if t == '+':
        end = data.index(b'\r\n', pos)
        return data[pos+1:end].decode(), end + 2
    if t == '-':
        end = data.index(b'\r\n', pos)
        raise RESPError(data[pos+1:end].decode())
    if t == ':':
        end = data.index(b'\r\n', pos)
        return int(data[pos+1:end]), end + 2
    if t == '$':
        end = data.index(b'\r\n', pos)
        length = int(data[pos+1:end])
        if length == -1:
            return None, end + 2
        start = end + 2
        if start + length + 2 > len(data):
            raise Incomplete()
        return data[start:start+length].decode('utf-8'), start + length + 2
    if t == '*':
        end = data.index(b'\r\n', pos)
        count = int(data[pos+1:end])
        if count == -1:
            return None, end + 2
        pos = end + 2
        items = []
        for _ in range(count):
            item, pos = decode(data, pos)
            items.append(item)
        return items, pos
    raise RESPError(f"Unknown RESP type byte: {t!r}")


class RESPParser:
    """Incremental parser — feed() chunks of bytes, get_command() pops one."""

    def __init__(self):
        self._buf = b''
        self._cmds: List[List[str]] = []

    def feed(self, data: bytes):
        self._buf += data
        pos = 0
        while pos < len(self._buf):
            try:
                val, new_pos = decode(self._buf, pos)
                if isinstance(val, list):
                    self._cmds.append([str(v) for v in val])
                pos = new_pos
            except Incomplete:
                break
            except Exception:
                pos += 1   # skip bad byte
        self._buf = self._buf[pos:]

    def get_command(self) -> Optional[List[str]]:
        return self._cmds.pop(0) if self._cmds else None
