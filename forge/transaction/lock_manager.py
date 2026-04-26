"""
Row-level lock manager with shared / exclusive modes and deadlock detection
via a wait-for graph (cycle detection using DFS).

Lock key: (table_name, page_id, slot_id)
"""
import threading
from collections import defaultdict
from typing import Dict, Optional, Set, Tuple

LockKey = Tuple[str, int, int]   # (table, page_id, slot_id)


class _Lock:
    __slots__ = ('holders', 'exclusive_holder', 'waiters', 'cond')

    def __init__(self):
        self.holders:          Set[int]          = set()   # shared holders (xids)
        self.exclusive_holder: Optional[int]     = None
        self.waiters:          Dict[int, str]    = {}      # xid → 'S' | 'X'
        self.cond              = threading.Condition(threading.Lock())


class LockManager:
    def __init__(self):
        self._locks: Dict[LockKey, _Lock]     = defaultdict(_Lock)
        self._held:  Dict[int, Set[LockKey]]  = defaultdict(set)  # xid → keys held
        self._waits: Dict[int, int]           = {}                # xid → blocking xid
        self._mu     = threading.Lock()

    # ── public ───────────────────────────────────────────────────────────────

    def acquire_shared(self, xid: int, key: LockKey, timeout: float = 5.0) -> bool:
        return self._acquire(xid, key, exclusive=False, timeout=timeout)

    def acquire_exclusive(self, xid: int, key: LockKey, timeout: float = 5.0) -> bool:
        return self._acquire(xid, key, exclusive=True, timeout=timeout)

    def release_all(self, xid: int):
        with self._mu:
            for key in list(self._held.get(xid, [])):
                lock = self._locks.get(key)
                if lock:
                    with lock.cond:
                        lock.holders.discard(xid)
                        if lock.exclusive_holder == xid:
                            lock.exclusive_holder = None
                        lock.waiters.pop(xid, None)
                        lock.cond.notify_all()
            self._held.pop(xid, None)
            self._waits.pop(xid, None)

    # ── internals ────────────────────────────────────────────────────────────

    def _acquire(self, xid: int, key: LockKey,
                 exclusive: bool, timeout: float) -> bool:
        lock = self._locks[key]
        mode = 'X' if exclusive else 'S'

        with lock.cond:
            deadline = None
            import time
            deadline = time.monotonic() + timeout

            while True:
                if self._can_grant(lock, xid, exclusive):
                    if exclusive:
                        lock.exclusive_holder = xid
                        lock.holders.discard(xid)
                    else:
                        lock.holders.add(xid)
                    lock.waiters.pop(xid, None)
                    break

                # Record wait-for for deadlock detection
                blocker = lock.exclusive_holder
                if blocker is None and lock.holders:
                    blocker = next(iter(lock.holders - {xid}))
                if blocker and self._has_cycle(xid, blocker):
                    return False          # deadlock — caller should abort

                lock.waiters[xid] = mode
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    lock.waiters.pop(xid, None)
                    return False
                lock.cond.wait(timeout=remaining)

        with self._mu:
            self._held[xid].add(key)
            self._waits.pop(xid, None)
        return True

    @staticmethod
    def _can_grant(lock: _Lock, xid: int, exclusive: bool) -> bool:
        if exclusive:
            # Exclusive: no other holders and no other exclusive
            other_holders = lock.holders - {xid}
            other_excl    = lock.exclusive_holder not in (None, xid)
            return not other_holders and not other_excl
        else:
            # Shared: no exclusive held by someone else
            return lock.exclusive_holder in (None, xid)

    def _has_cycle(self, start: int, blocked_by: int) -> bool:
        """DFS cycle detection in wait-for graph."""
        visited: Set[int] = {start}
        stack   = [blocked_by]
        while stack:
            node = stack.pop()
            if node == start:
                return True
            if node in visited:
                continue
            visited.add(node)
            waiter = self._waits.get(node)
            if waiter:
                stack.append(waiter)
        return False
