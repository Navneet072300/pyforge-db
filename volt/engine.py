"""
VoltDB — the in-memory key-value store.

Manages:
  - Multiple logical databases (SELECT 0..15)
  - Per-key TTL with lazy + periodic active expiry
  - Type tagging  (string / list / hash / set / zset)
  - AOF persistence (optional)
  - Pub/Sub hub (shared across all DBs)
"""
from __future__ import annotations

import threading
import time
from typing import Any, Dict, List, Optional, Set

from .commands import COMMANDS
from .persistence.aof import AOF
from .protocol import resp
from .pubsub import PubSub
from .types import VOLT_STRING


_NUM_DBS = 16
_ACTIVE_EXPIRY_INTERVAL = 0.1   # seconds between active expiry sweeps
_ACTIVE_EXPIRY_SAMPLE   = 20    # keys checked per sweep


class _Database:
    """One logical database (SELECT n)."""

    def __init__(self):
        self._store:   Dict[str, Any]   = {}
        self._types:   Dict[str, str]   = {}
        self._expires: Dict[str, float] = {}   # key → deadline (unix time)

    # ── write ──────────────────────────────────────────────────────────────────

    def put(self, key: str, value: Any, type_: str):
        self._store[key]  = value
        self._types[key]  = type_

    def delete(self, key: str) -> bool:
        existed = key in self._store
        self._store.pop(key, None)
        self._types.pop(key, None)
        self._expires.pop(key, None)
        return existed

    # ── read ───────────────────────────────────────────────────────────────────

    def get(self, key: str) -> Optional[Any]:
        if self._is_expired(key):
            self.delete(key)
            return None
        return self._store.get(key)

    def type_of(self, key: str) -> Optional[str]:
        if self._is_expired(key):
            self.delete(key)
            return None
        return self._types.get(key)

    def all_keys(self) -> List[str]:
        now = time.time()
        expired = [k for k, dl in self._expires.items() if dl <= now]
        for k in expired:
            self.delete(k)
        return list(self._store.keys())

    # ── expiry ─────────────────────────────────────────────────────────────────

    def expire(self, key: str, seconds: int):
        self._expires[key] = time.time() + seconds

    def pexpire(self, key: str, ms: int):
        self._expires[key] = time.time() + ms / 1000

    def expireat(self, key: str, unix_ts: int):
        self._expires[key] = float(unix_ts)

    def persist(self, key: str) -> bool:
        return self._expires.pop(key, None) is not None

    def ttl(self, key: str) -> int:
        if key not in self._expires:
            return -1
        remaining = self._expires[key] - time.time()
        return max(0, int(remaining)) if remaining > 0 else -2

    def pttl(self, key: str) -> int:
        if key not in self._expires:
            return -1
        remaining = self._expires[key] - time.time()
        return max(0, int(remaining * 1000)) if remaining > 0 else -2

    def flush(self):
        self._store.clear()
        self._types.clear()
        self._expires.clear()

    # ── active expiry ──────────────────────────────────────────────────────────

    def active_expiry_pass(self, sample: int = _ACTIVE_EXPIRY_SAMPLE):
        if not self._expires:
            return
        import random
        keys = random.sample(list(self._expires), min(sample, len(self._expires)))
        now = time.time()
        for k in keys:
            if self._expires.get(k, float('inf')) <= now:
                self.delete(k)

    # ── internals ──────────────────────────────────────────────────────────────

    def _is_expired(self, key: str) -> bool:
        deadline = self._expires.get(key)
        return deadline is not None and deadline <= time.time()

    def snapshot_commands(self) -> List[List[str]]:
        """Generate minimal SET/LPUSH/... commands to rebuild current state."""
        cmds = []
        from .types import VOLT_LIST, VOLT_HASH, VOLT_SET, VOLT_ZSET
        for key in self.all_keys():
            t = self._types.get(key)
            v = self._store.get(key)
            if t == VOLT_STRING:
                cmds.append(['SET', key, str(v)])
            elif t == VOLT_LIST:
                if v:
                    cmds.append(['RPUSH', key] + list(v))
            elif t == VOLT_HASH:
                if v:
                    pairs = []
                    for f, fv in v.items():
                        pairs += [f, str(fv)]
                    cmds.append(['HSET', key] + pairs)
            elif t == VOLT_SET:
                if v:
                    cmds.append(['SADD', key] + sorted(v))
            elif t == VOLT_ZSET:
                for member, score in v.all_members():
                    cmds.append(['ZADD', key, f'{score:g}', member])
            # Re-apply TTL
            ttl_s = self.ttl(key)
            if ttl_s > 0:
                cmds.append(['EXPIRE', key, str(ttl_s)])
        return cmds


# ═══════════════════════════════════════════════════════════════════════════════

class VoltDB:
    """
    Top-level engine: multiple DBs, pub/sub, AOF, active expiry thread.

    This is the object passed into every command handler.
    """

    def __init__(self, aof_path: Optional[str] = None,
                 fsync: str = 'everysec',
                 num_dbs: int = _NUM_DBS):
        self._dbs: List[_Database] = [_Database() for _ in range(num_dbs)]
        self._db_idx = 0
        self.pubsub  = PubSub()
        self.start_time = time.time()

        self.aof: Optional[AOF] = None
        if aof_path:
            self.aof = AOF(aof_path, fsync)
            self._replay_aof()

        self._stop_expiry = threading.Event()
        self._expiry_thread = threading.Thread(
            target=self._active_expiry_loop, daemon=True, name='volt-expiry')
        self._expiry_thread.start()

    # ── current-db shortcut ───────────────────────────────────────────────────

    @property
    def _db(self) -> _Database:
        return self._dbs[self._db_idx]

    def select(self, idx: int):
        if not (0 <= idx < len(self._dbs)):
            raise ValueError(f"DB index out of range: {idx}")
        self._db_idx = idx

    # ── delegate to current DB ────────────────────────────────────────────────

    def put(self, key: str, value: Any, type_: str):
        self._db.put(key, value, type_)

    def get(self, key: str) -> Optional[Any]:
        return self._db.get(key)

    def delete(self, key: str) -> bool:
        return self._db.delete(key)

    def type_of(self, key: str) -> Optional[str]:
        return self._db.type_of(key)

    def all_keys(self) -> List[str]:
        return self._db.all_keys()

    def expire(self, key: str, seconds: int):
        self._db.expire(key, seconds)

    def pexpire(self, key: str, ms: int):
        self._db.pexpire(key, ms)

    def expireat(self, key: str, unix_ts: int):
        self._db.expireat(key, unix_ts)

    def persist(self, key: str) -> bool:
        return self._db.persist(key)

    def ttl(self, key: str) -> int:
        return self._db.ttl(key)

    def pttl(self, key: str) -> int:
        return self._db.pttl(key)

    def flushdb(self):
        self._db.flush()

    def flushall(self):
        for db in self._dbs:
            db.flush()

    # ── command dispatch ──────────────────────────────────────────────────────

    def execute(self, parts: List[str]) -> bytes:
        """Execute a command given as a list of strings. Returns RESP bytes."""
        if not parts:
            return resp.error("empty command")
        name = parts[0].upper()
        fn = COMMANDS.get(name)
        if fn is None:
            return resp.error(f"unknown command '{parts[0]}'")
        try:
            result = fn(self, parts[1:])
            # Log write commands to AOF
            if self.aof and name not in _READ_ONLY:
                self.aof.log(*parts)
            return result
        except Exception as e:
            return resp.error(str(e))

    def execute_str(self, line: str) -> bytes:
        """Parse a space-tokenised command string and execute it."""
        parts = _tokenise(line)
        return self.execute(parts)

    # ── persistence ───────────────────────────────────────────────────────────

    def save(self):
        if self.aof:
            cmds = []
            for i, db in enumerate(self._dbs):
                if db.all_keys():
                    cmds.append(['SELECT', str(i)])
                    cmds.extend(db.snapshot_commands())
            self.aof.rewrite(cmds)

    def _replay_aof(self):
        def _exec(parts):
            if parts:
                name = parts[0].upper()
                fn = COMMANDS.get(name)
                if fn:
                    if name == 'SELECT':
                        n, _ = parts[1], None
                        try:
                            self.select(int(n))
                        except Exception:
                            pass
                    else:
                        fn(self, parts[1:])
        self.aof.replay(_exec)

    # ── active expiry ─────────────────────────────────────────────────────────

    def _active_expiry_loop(self):
        while not self._stop_expiry.wait(_ACTIVE_EXPIRY_INTERVAL):
            for db in self._dbs:
                db.active_expiry_pass()

    def shutdown(self):
        self._stop_expiry.set()
        if self.aof:
            self.aof.close()


# ── helpers ────────────────────────────────────────────────────────────────────

_READ_ONLY = frozenset({
    'GET', 'MGET', 'KEYS', 'SCAN', 'EXISTS', 'TYPE', 'TTL', 'PTTL',
    'STRLEN', 'GETRANGE', 'LRANGE', 'LLEN', 'LINDEX',
    'HGET', 'HMGET', 'HGETALL', 'HLEN', 'HKEYS', 'HVALS', 'HEXISTS',
    'SMEMBERS', 'SISMEMBER', 'SMISMEMBER', 'SCARD',
    'ZRANGE', 'ZREVRANGE', 'ZRANGEBYSCORE', 'ZREVRANGEBYSCORE',
    'ZRANK', 'ZREVRANK', 'ZSCORE', 'ZCARD', 'ZCOUNT',
    'PING', 'ECHO', 'DBSIZE', 'INFO', 'CONFIG', 'OBJECT', 'WAIT',
    'PUBSUB',
})


def _tokenise(line: str) -> List[str]:
    """Naive tokeniser that respects single and double quotes."""
    parts = []
    buf   = []
    in_q  = None
    for ch in line.strip():
        if in_q:
            if ch == in_q:
                in_q = None
            else:
                buf.append(ch)
        elif ch in ('"', "'"):
            in_q = ch
        elif ch == ' ':
            if buf:
                parts.append(''.join(buf))
                buf = []
        else:
            buf.append(ch)
    if buf:
        parts.append(''.join(buf))
    return parts
