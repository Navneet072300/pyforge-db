"""
Transaction manager: allocates XIDs, tracks status, and drives MVCC visibility.

Isolation level: Read Committed (default) — readers see the latest committed
version.  MVCC means readers never block writers and writers never block
readers.  Row-level locking is used only between concurrent writers.

Visibility rule for xid T reading row with (xmin, xmax):
  visible = (xmin committed OR xmin == T.xid)
            AND
            (xmax == 0
             OR (xmax not committed AND xmax != T.xid))
"""
import threading
from enum import Enum
from typing import Optional, Set


class TxStatus(Enum):
    ACTIVE    = 'ACTIVE'
    COMMITTED = 'COMMITTED'
    ABORTED   = 'ABORTED'


class Transaction:
    __slots__ = ('xid', 'status', 'dirty_pages')

    def __init__(self, xid: int):
        self.xid:         int       = xid
        self.status:      TxStatus  = TxStatus.ACTIVE
        self.dirty_pages: set       = set()   # (filepath, page_id) touched

    def __repr__(self):
        return f"Txn(xid={self.xid}, status={self.status.value})"


class TransactionManager:
    def __init__(self):
        self._lock:       threading.Lock  = threading.Lock()
        self._next_xid:   int             = 1
        self._active:     dict            = {}   # xid → Transaction
        self._committed:  Set[int]        = set()

    # ── lifecycle ─────────────────────────────────────────────────────────────

    def begin(self) -> Transaction:
        with self._lock:
            xid = self._next_xid
            self._next_xid += 1
            txn = Transaction(xid)
            self._active[xid] = txn
        return txn

    def commit(self, txn: Transaction):
        with self._lock:
            txn.status = TxStatus.COMMITTED
            self._committed.add(txn.xid)
            self._active.pop(txn.xid, None)

    def abort(self, txn: Transaction):
        with self._lock:
            txn.status = TxStatus.ABORTED
            self._active.pop(txn.xid, None)

    def get(self, xid: int) -> Optional[Transaction]:
        with self._lock:
            return self._active.get(xid)

    def is_committed(self, xid: int) -> bool:
        return xid in self._committed

    # ── MVCC visibility ───────────────────────────────────────────────────────

    def is_visible(self, row: dict, txn: Transaction) -> bool:
        """Return True if this row version is visible to txn."""
        xmin = row.get('__xmin__', 0)
        xmax = row.get('__xmax__', 0)

        # Row must have been created by a committed or our own txn
        creator_ok = (xmin == txn.xid) or self.is_committed(xmin)
        if not creator_ok:
            return False

        # Row must not be deleted
        if xmax == 0:
            return True

        # If deleted by our own txn or a committed txn → not visible
        deleted = (xmax == txn.xid) or self.is_committed(xmax)
        return not deleted

    # ── auto-commit helper ───────────────────────────────────────────────────

    def autocommit(self):
        """Return a fresh transaction that callers commit right after use."""
        return self.begin()
