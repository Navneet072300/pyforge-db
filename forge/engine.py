"""
DatabaseEngine — the top-level coordinator for Forge — PostgreSQL-like relational engine.

Responsibilities
────────────────
• Initialise storage, WAL, buffer pool, catalog, transaction manager, lock manager
• Provide a single execute(sql, connection) entry point
• Manage connection-level transaction state (auto-commit vs explicit)
• Perform WAL-based crash recovery on startup
"""
from __future__ import annotations

import os
import time
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

from .catalog.catalog import Catalog
from .query.executor import Executor
from .query.parser import (
    parse, ParseError,
    SelectStmt, InsertStmt, UpdateStmt, DeleteStmt,
    CreateTableStmt, DropTableStmt, CreateIndexStmt,
    BeginStmt, CommitStmt, RollbackStmt,
)
from .storage.buffer_pool import BufferPool
from .storage.wal import WAL
from .transaction.lock_manager import LockManager
from .transaction.transaction_manager import Transaction, TransactionManager, TxStatus


@dataclass
class Connection:
    """Per-client state: current transaction + auto-commit flag."""
    conn_id:  int
    txn:      Optional[Transaction] = None
    autocommit: bool = True


class QueryResult:
    def __init__(self, rows: List[dict], message: str = '',
                 columns: Optional[List[str]] = None,
                 elapsed: float = 0.0):
        self.rows    = rows
        self.message = message
        self.columns = columns or (list(rows[0].keys()) if rows else [])
        self.elapsed = elapsed

    def __repr__(self):
        return f"QueryResult({len(self.rows)} rows, '{self.message}')"


class DatabaseEngine:
    def __init__(self, data_dir: str = './data', buffer_capacity: int = 512):
        self.data_dir = data_dir
        os.makedirs(data_dir, exist_ok=True)

        wal_path = os.path.join(data_dir, 'wal.log')

        self.catalog = Catalog(data_dir)
        self.buf     = BufferPool(buffer_capacity)
        self.wal     = WAL(wal_path)
        self.txm     = TransactionManager()
        self.lm      = LockManager()
        self.executor = Executor(self.catalog, self.buf, self.wal, self.txm, self.lm)

        # Crash recovery
        self.executor.recover()

        self._next_conn = 1

    # ── connection factory ────────────────────────────────────────────────────

    def connect(self) -> Connection:
        cid  = self._next_conn
        self._next_conn += 1
        return Connection(cid)

    # ── execute ───────────────────────────────────────────────────────────────

    def execute(self, sql: str, conn: Optional[Connection] = None) -> QueryResult:
        """Parse and execute a SQL statement, returning a QueryResult."""
        if conn is None:
            conn = self.connect()

        sql = sql.strip()
        if not sql:
            return QueryResult([], 'Empty statement.')

        t0 = time.perf_counter()

        try:
            ast = parse(sql)
        except Exception as e:
            return QueryResult([], f"Parse error: {e}")

        try:
            result = self._dispatch(ast, conn)
        except Exception as e:
            # On error in explicit txn — rollback
            if conn.txn and conn.txn.status == TxStatus.ACTIVE:
                self._do_rollback(conn)
            return QueryResult([], f"Error: {e}")

        elapsed = time.perf_counter() - t0
        rows    = result.get('rows', [])
        msg     = result.get('message', '')
        cols    = list(rows[0].keys()) if rows else []
        # Strip internal MVCC keys from output
        clean_rows = [{k: v for k, v in r.items() if not k.startswith('__')}
                      for r in rows]
        clean_cols  = [c for c in cols if not c.startswith('__')]
        return QueryResult(clean_rows, msg, clean_cols, elapsed)

    # ── dispatch ─────────────────────────────────────────────────────────────

    def _dispatch(self, ast, conn: Connection) -> dict:
        # Transaction control
        if isinstance(ast, BeginStmt):
            return self._do_begin(conn)
        if isinstance(ast, CommitStmt):
            return self._do_commit(conn)
        if isinstance(ast, RollbackStmt):
            return self._do_rollback(conn)

        # Ensure we have an active transaction
        txn = self._ensure_txn(conn)

        if isinstance(ast, CreateTableStmt):
            result = self.executor.create_table(ast, txn)
        elif isinstance(ast, DropTableStmt):
            result = self.executor.drop_table(ast, txn)
        elif isinstance(ast, CreateIndexStmt):
            result = self.executor.create_index(ast, txn)
        elif isinstance(ast, InsertStmt):
            result = self.executor.insert(ast, txn)
        elif isinstance(ast, UpdateStmt):
            result = self.executor.update(ast, txn)
        elif isinstance(ast, DeleteStmt):
            result = self.executor.delete(ast, txn)
        elif isinstance(ast, SelectStmt):
            result = self.executor.select(ast, txn)
        else:
            raise RuntimeError(f"Unhandled statement type: {type(ast)}")

        # Auto-commit after each statement if not in explicit txn
        if conn.autocommit:
            self._do_commit(conn)

        return result

    # ── transaction helpers ───────────────────────────────────────────────────

    def _ensure_txn(self, conn: Connection) -> Transaction:
        if conn.txn is None or conn.txn.status != TxStatus.ACTIVE:
            conn.txn = self.txm.begin()
            self.wal.append(conn.txn.xid, __import__(
                'forge.storage.wal', fromlist=['WALType']).WALType.BEGIN, {})
        return conn.txn

    def _do_begin(self, conn: Connection) -> dict:
        if conn.txn and conn.txn.status == TxStatus.ACTIVE:
            return {'message': 'Already in a transaction.', 'rows': []}
        conn.autocommit = False
        conn.txn = self.txm.begin()
        from .storage.wal import WALType
        self.wal.append(conn.txn.xid, WALType.BEGIN, {})
        return {'message': f'Transaction started (xid={conn.txn.xid}).', 'rows': []}

    def _do_commit(self, conn: Connection) -> dict:
        if conn.txn is None or conn.txn.status != TxStatus.ACTIVE:
            conn.autocommit = True
            return {'message': 'No active transaction.', 'rows': []}
        from .storage.wal import WALType
        self.wal.append(conn.txn.xid, WALType.COMMIT, {})
        self.buf.flush_all()
        self.txm.commit(conn.txn)
        self.lm.release_all(conn.txn.xid)
        xid = conn.txn.xid
        conn.txn        = None
        conn.autocommit = True
        return {'message': f'Transaction {xid} committed.', 'rows': []}

    def _do_rollback(self, conn: Connection) -> dict:
        if conn.txn is None or conn.txn.status != TxStatus.ACTIVE:
            conn.autocommit = True
            return {'message': 'No active transaction.', 'rows': []}
        from .storage.wal import WALType
        self.wal.append(conn.txn.xid, WALType.ABORT, {})
        self.txm.abort(conn.txn)
        self.lm.release_all(conn.txn.xid)
        xid = conn.txn.xid
        conn.txn        = None
        conn.autocommit = True
        return {'message': f'Transaction {xid} rolled back.', 'rows': []}

    # ── utilities ─────────────────────────────────────────────────────────────

    def flush(self):
        """Flush all dirty pages to disk."""
        self.buf.flush_all()

    def close(self):
        self.flush()
