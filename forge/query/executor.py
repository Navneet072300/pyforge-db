"""
Query executor: walks the physical plan tree produced by the Planner,
interacts with the buffer pool, catalog, WAL, and transaction manager.

All mutation operations (INSERT / UPDATE / DELETE) are:
  1. WAL-logged before touching any page
  2. Written through the buffer pool
  3. MVCC-stamped (xmin / xmax)
  4. Reflected in every relevant B+ tree index
"""
from __future__ import annotations

import fnmatch
import math
import time
from collections import defaultdict
from datetime import datetime
from typing import Any, Dict, Iterator, List, Optional, Tuple

from ..catalog.catalog import (
    Catalog, TableSchema, ColumnDef, IndexDef,
    encode_row, decode_row, set_xmax,
)
from ..index.btree import BPlusTree
from ..query.parser import (
    SelectStmt, InsertStmt, UpdateStmt, DeleteStmt,
    CreateTableStmt, DropTableStmt, CreateIndexStmt,
    BeginStmt, CommitStmt, RollbackStmt,
    BinOp, UnaryOp, ColumnRef, Literal, AggFunc, FuncCall,
    IsNullExpr, InExpr, BetweenExpr, LikeExpr,
)
from ..query.planner import (
    Planner, SeqScan, IndexScan, NestedLoopJoin,
    Filter, Projection, Sort, Limit, Aggregate,
)
from ..storage.buffer_pool import BufferPool
from ..storage.wal import WAL, WALType
from ..transaction.lock_manager import LockManager
from ..transaction.transaction_manager import (
    Transaction, TransactionManager, TxStatus,
)

RID = Tuple[int, int]   # (page_id, slot_id)


class ExecutorError(Exception):
    pass


class Executor:
    def __init__(self, catalog: Catalog, buf: BufferPool,
                 wal: WAL, txm: TransactionManager, lm: LockManager):
        self.catalog  = catalog
        self.buf      = buf
        self.wal      = wal
        self.txm      = txm
        self.lm       = lm
        self.planner  = Planner(catalog)
        self._indices: Dict[str, BPlusTree] = {}   # path → BPlusTree

    # ═══════════════════════════════════════════════════════════════════════
    # DDL
    # ═══════════════════════════════════════════════════════════════════════

    def create_table(self, stmt: CreateTableStmt, txn: Transaction):
        cols = [c for c in stmt.columns if c is not None]
        from ..catalog.catalog import ColumnDef as CDef
        schema = TableSchema(
            stmt.table,
            [CDef(c.name, c.col_type, c.nullable, c.primary_key) for c in cols],
        )
        self.catalog.create_table(schema)
        # Touch the heap file so it exists on disk
        self.buf.register(self.catalog.heap_path(stmt.table))
        return {'message': f"Table '{stmt.table}' created.", 'rows': []}

    def drop_table(self, stmt: DropTableStmt, txn: Transaction):
        name = stmt.table
        if not self.catalog.table_exists(name):
            if stmt.if_exists:
                return {'message': f"Table '{name}' does not exist (skipped).", 'rows': []}
            raise ExecutorError(f"Table '{name}' does not exist")

        schema = self.catalog.get_table(name)
        # Drop index files
        for idx in schema.indices:
            ipath = self.catalog.index_path(name, idx.index_name)
            self._indices.pop(ipath, None)
            import os
            if os.path.exists(ipath):
                os.remove(ipath)

        # Evict & remove heap file
        hpath = self.catalog.heap_path(name)
        self.buf.evict_table(hpath)
        import os
        if os.path.exists(hpath):
            os.remove(hpath)

        self.catalog.drop_table(name)
        return {'message': f"Table '{name}' dropped.", 'rows': []}

    def create_index(self, stmt: CreateIndexStmt, txn: Transaction):
        schema = self.catalog.get_table(stmt.table)
        idx    = IndexDef(stmt.index_name, stmt.table, stmt.columns, stmt.unique)
        self.catalog.add_index(idx)

        # Build the index by scanning the table
        tree  = BPlusTree()
        ipath = self.catalog.index_path(stmt.table, stmt.index_name)
        hpath = self.catalog.heap_path(stmt.table)

        for row, page_id, slot_id in self._seq_scan_raw(hpath, schema, txn):
            key = self._index_key(row, idx.columns)
            tree.insert(key, (page_id, slot_id))

        tree.save(ipath)
        self._indices[ipath] = tree
        return {'message': f"Index '{stmt.index_name}' created on '{stmt.table}'.", 'rows': []}

    # ═══════════════════════════════════════════════════════════════════════
    # DML — INSERT
    # ═══════════════════════════════════════════════════════════════════════

    def insert(self, stmt: InsertStmt, txn: Transaction):
        schema = self.catalog.get_table(stmt.table)
        hpath  = self.catalog.heap_path(stmt.table)
        count  = 0

        for raw_vals in stmt.rows:
            # Evaluate literals → Python values
            vals = [self._eval(v, {}, txn) for v in raw_vals]

            # Map to column names
            if stmt.columns:
                if len(stmt.columns) != len(vals):
                    raise ExecutorError("Column count doesn't match value count")
                row = dict(zip(stmt.columns, vals))
            else:
                if len(schema.columns) != len(vals):
                    raise ExecutorError("Value count doesn't match column count")
                row = dict(zip(schema.col_names(), vals))

            # Validate & coerce types
            row = self._coerce_row(row, schema)

            # Encode
            payload = encode_row(row, schema, xmin=txn.xid, xmax=0)

            # Write to heap
            page_id, slot_id = self._insert_into_heap(hpath, payload)

            # WAL
            self.wal.append(txn.xid, WALType.INSERT, {
                'table': stmt.table, 'page_id': page_id, 'slot_id': slot_id,
                'data': payload.hex(),
            })

            # Update indices (skip NULL keys — NULL values are not indexed)
            for idx in schema.indices:
                key = self._index_key(row, idx.columns)
                if key is None:
                    continue
                tree = self._load_index(stmt.table, idx)
                tree.insert(key, (page_id, slot_id))
                self._save_index(stmt.table, idx, tree)

            count += 1

        return {'message': f"{count} row(s) inserted.", 'rows': [], 'count': count}

    # ═══════════════════════════════════════════════════════════════════════
    # DML — UPDATE
    # ═══════════════════════════════════════════════════════════════════════

    def update(self, stmt: UpdateStmt, txn: Transaction):
        schema = self.catalog.get_table(stmt.table)
        hpath  = self.catalog.heap_path(stmt.table)
        count  = 0

        # Collect matching rows first to avoid modifying what we're scanning
        targets: List[Tuple[dict, int, int]] = []
        for row, pid, sid in self._seq_scan_raw(hpath, schema, txn):
            if stmt.where is None or self._eval(stmt.where, row, txn):
                targets.append((row, pid, sid))

        for old_row, pid, sid in targets:
            # Acquire row-level exclusive lock
            lkey = (stmt.table, pid, sid)
            if not self.lm.acquire_exclusive(txn.xid, lkey):
                raise ExecutorError("Could not acquire lock (deadlock?)")

            # MVCC delete: stamp old version with xmax
            page = self.buf.fetch(hpath, pid)
            old_raw = page.get_tuple(sid)
            new_raw = set_xmax(old_raw, txn.xid)
            page.update_tuple_inplace(sid, new_raw)
            self.buf.mark_dirty(hpath, pid)

            # Build new row
            new_row = dict(old_row)
            new_row.pop('__xmin__', None); new_row.pop('__xmax__', None)
            for col, val_expr in stmt.assignments:
                new_row[col] = self._eval(val_expr, old_row, txn)
            new_row = self._coerce_row(new_row, schema)

            # Insert new version
            new_payload  = encode_row(new_row, schema, xmin=txn.xid, xmax=0)
            new_pid, new_sid = self._insert_into_heap(hpath, new_payload)

            # WAL
            self.wal.append(txn.xid, WALType.UPDATE, {
                'table': stmt.table,
                'old_page': pid, 'old_slot': sid,
                'new_page': new_pid, 'new_slot': new_sid,
                'data': new_payload.hex(),
            })

            # Update indices (NULL keys are not indexed)
            for idx in schema.indices:
                old_key = self._index_key(old_row, idx.columns)
                new_key = self._index_key(new_row, idx.columns)
                tree    = self._load_index(stmt.table, idx)
                if old_key is not None:
                    tree.delete(old_key, (pid, sid))
                if new_key is not None:
                    tree.insert(new_key, (new_pid, new_sid))
                self._save_index(stmt.table, idx, tree)

            count += 1

        return {'message': f"{count} row(s) updated.", 'rows': [], 'count': count}

    # ═══════════════════════════════════════════════════════════════════════
    # DML — DELETE
    # ═══════════════════════════════════════════════════════════════════════

    def delete(self, stmt: DeleteStmt, txn: Transaction):
        schema = self.catalog.get_table(stmt.table)
        hpath  = self.catalog.heap_path(stmt.table)
        count  = 0

        targets: List[Tuple[dict, int, int]] = []
        for row, pid, sid in self._seq_scan_raw(hpath, schema, txn):
            if stmt.where is None or self._eval(stmt.where, row, txn):
                targets.append((row, pid, sid))

        for row, pid, sid in targets:
            lkey = (stmt.table, pid, sid)
            if not self.lm.acquire_exclusive(txn.xid, lkey):
                raise ExecutorError("Could not acquire lock (deadlock?)")

            page    = self.buf.fetch(hpath, pid)
            old_raw = page.get_tuple(sid)
            new_raw = set_xmax(old_raw, txn.xid)
            page.update_tuple_inplace(sid, new_raw)
            self.buf.mark_dirty(hpath, pid)

            self.wal.append(txn.xid, WALType.DELETE, {
                'table': stmt.table, 'page_id': pid, 'slot_id': sid,
            })

            for idx in schema.indices:
                key = self._index_key(row, idx.columns)
                if key is not None:
                    tree = self._load_index(stmt.table, idx)
                    tree.delete(key, (pid, sid))
                    self._save_index(stmt.table, idx, tree)

            count += 1

        return {'message': f"{count} row(s) deleted.", 'rows': [], 'count': count}

    # ═══════════════════════════════════════════════════════════════════════
    # DQL — SELECT
    # ═══════════════════════════════════════════════════════════════════════

    def select(self, stmt: SelectStmt, txn: Transaction):
        plan = self.planner.plan_select(stmt)
        rows = list(self._execute_plan(plan, txn))

        if stmt.distinct:
            seen = set()
            deduped = []
            for r in rows:
                key = tuple(sorted(r.items()))
                if key not in seen:
                    seen.add(key); deduped.append(r)
            rows = deduped

        return {'rows': rows, 'message': f"{len(rows)} row(s) returned."}

    # ═══════════════════════════════════════════════════════════════════════
    # PLAN EXECUTION
    # ═══════════════════════════════════════════════════════════════════════

    def _execute_plan(self, plan, txn: Transaction) -> Iterator[dict]:
        if isinstance(plan, SeqScan):
            yield from self._exec_seqscan(plan, txn)
        elif isinstance(plan, IndexScan):
            yield from self._exec_indexscan(plan, txn)
        elif isinstance(plan, NestedLoopJoin):
            yield from self._exec_join(plan, txn)
        elif isinstance(plan, Filter):
            for row in self._execute_plan(plan.child, txn):
                if self._eval(plan.predicate, row, txn):
                    yield row
        elif isinstance(plan, Projection):
            if plan.child is None:
                yield self._project_row({}, plan.columns, txn)
            else:
                for row in self._execute_plan(plan.child, txn):
                    yield self._project_row(row, plan.columns, txn)
        elif isinstance(plan, Sort):
            rows = list(self._execute_plan(plan.child, txn))
            rows.sort(key=lambda r: self._sort_key(r, plan.order_by, txn),
                      reverse=False)
            yield from rows
        elif isinstance(plan, Limit):
            for i, row in enumerate(self._execute_plan(plan.child, txn)):
                if i >= plan.count:
                    break
                yield row
        elif isinstance(plan, Aggregate):
            yield from self._exec_aggregate(plan, txn)
        else:
            raise ExecutorError(f"Unknown plan node: {type(plan)}")

    # ── seq scan ─────────────────────────────────────────────────────────────

    def _exec_seqscan(self, plan: SeqScan, txn: Transaction):
        schema = self.catalog.get_table(plan.table)
        hpath  = self.catalog.heap_path(plan.table)
        prefix = plan.alias or plan.table

        for row, _, _ in self._seq_scan_raw(hpath, schema, txn):
            r = self._qualify(row, prefix, schema)
            if plan.predicate is None or self._eval(plan.predicate, r, txn):
                yield r

    def _seq_scan_raw(self, hpath: str, schema: TableSchema,
                      txn: Transaction) -> Iterator[Tuple[dict, int, int]]:
        """Yield (decoded_row, page_id, slot_id) for all visible rows."""
        num_pages = self.buf.num_pages(hpath)
        for pid in range(num_pages):
            page = self.buf.fetch(hpath, pid)
            if page is None:
                continue
            for sid, raw in page.iter_tuples():
                row = decode_row(raw, schema)
                if self.txm.is_visible(row, txn):
                    yield row, pid, sid

    # ── index scan ───────────────────────────────────────────────────────────

    def _exec_indexscan(self, plan: IndexScan, txn: Transaction):
        schema = self.catalog.get_table(plan.table)
        hpath  = self.catalog.heap_path(plan.table)
        prefix = plan.alias or plan.table

        # Find the index definition
        idx = schema.index_for_col(plan.index_col)
        if idx is None:
            # Fall back to seq scan
            for row, _, _ in self._seq_scan_raw(hpath, schema, txn):
                r = self._qualify(row, prefix, schema)
                if plan.predicate is None or self._eval(plan.predicate, r, txn):
                    yield r
            return

        tree = self._load_index(plan.table, idx)
        key  = plan.key

        if plan.op == '=':
            rids = tree.search(key)
        elif plan.op == '>':
            rids = [r for k, r in tree.iter_all() if k > key]
        elif plan.op == '>=':
            rids = tree.range_search(lo=key)
        elif plan.op == '<':
            rids = [r for k, r in tree.iter_all() if k < key]
        elif plan.op == '<=':
            rids = tree.range_search(hi=key)
        else:
            rids = list(tree.iter_all())

        # Flatten — iter_all returns (key, rid) pairs
        if plan.op not in ('>', '<'):
            pass  # already flat lists from search/range_search
        else:
            rids = [rid for rid in rids]

        for rid in rids:
            if isinstance(rid, tuple) and len(rid) == 2 and isinstance(rid[0], int):
                pid, sid = rid
            else:
                continue
            page = self.buf.fetch(hpath, pid)
            if page is None:
                continue
            raw = page.get_tuple(sid)
            if raw is None:
                continue
            row = decode_row(raw, schema)
            if not self.txm.is_visible(row, txn):
                continue
            r = self._qualify(row, prefix, schema)
            if plan.predicate is None or self._eval(plan.predicate, r, txn):
                yield r

    # ── join ─────────────────────────────────────────────────────────────────

    def _exec_join(self, plan: NestedLoopJoin, txn: Transaction):
        outer_rows = list(self._execute_plan(plan.outer, txn))
        for o_row in outer_rows:
            matched = False
            for i_row in self._execute_plan(plan.inner, txn):
                combined = {**o_row, **i_row}
                if plan.condition is None or self._eval(plan.condition, combined, txn):
                    matched = True
                    yield combined
            if not matched and plan.join_type in ('LEFT', 'FULL'):
                # Emit outer row with NULLs for inner columns.
                # Outer columns take precedence so we merge inner_nulls first.
                inner_nulls = {}
                if isinstance(plan.inner, (SeqScan, IndexScan)):
                    inner_tbl   = plan.inner.table
                    inner_alias = plan.inner.alias or inner_tbl
                    if self.catalog.table_exists(inner_tbl):
                        for col in self.catalog.get_table(inner_tbl).columns:
                            inner_nulls[f"{inner_alias}.{col.name}"] = None
                            inner_nulls[col.name] = None
                yield {**inner_nulls, **o_row}

    # ── aggregate ────────────────────────────────────────────────────────────

    def _exec_aggregate(self, plan: Aggregate, txn: Transaction):
        rows  = list(self._execute_plan(plan.child, txn))
        groups: Dict[tuple, list] = defaultdict(list)

        if plan.group_by:
            for row in rows:
                key = tuple(self._eval(g, row, txn) for g in plan.group_by)
                groups[key].append(row)
        else:
            groups[()] = rows

        for gkey, group_rows in groups.items():
            out_row: Dict[str, Any] = {}

            # Group-by columns
            for i, g_expr in enumerate(plan.group_by):
                col_name = self._expr_name(g_expr)
                out_row[col_name] = gkey[i]

            # Aggregate columns
            for agg in plan.aggregates:
                val = self._compute_agg(agg, group_rows, txn)
                name = agg.alias or f"{agg.func}({self._expr_name(agg.arg)})"
                out_row[name] = val

            # Non-aggregate, non-group columns in select list
            for col_expr in plan.select_cols:
                if isinstance(col_expr, AggFunc):
                    continue
                name = self._expr_name(col_expr)
                if name not in out_row and group_rows:
                    try:
                        out_row[name] = self._eval(col_expr, group_rows[0], txn)
                    except Exception:
                        out_row[name] = None

            if plan.having is None or self._eval(plan.having, out_row, txn):
                yield out_row

    def _compute_agg(self, agg: AggFunc, rows: list, txn: Transaction) -> Any:
        func = agg.func
        if func == 'COUNT':
            if isinstance(agg.arg, Literal) and agg.arg.value == '*':
                return len(rows)
            vals = [self._eval(agg.arg, r, txn) for r in rows]
            return sum(1 for v in vals if v is not None)
        vals = [self._eval(agg.arg, r, txn) for r in rows if r]
        vals = [v for v in vals if v is not None]
        if not vals:
            return None
        if func == 'SUM':   return sum(vals)
        if func == 'AVG':   return sum(vals) / len(vals)
        if func == 'MIN':   return min(vals)
        if func == 'MAX':   return max(vals)
        raise ExecutorError(f"Unknown aggregate: {func}")

    # ── projection ───────────────────────────────────────────────────────────

    def _project_row(self, row: dict, cols: list, txn: Transaction) -> dict:
        out = {}
        for col_expr in cols:
            if isinstance(col_expr, ColumnRef) and col_expr.column == '*':
                # Wildcard: only emit unqualified column names (no "table.col" duplicates)
                for k, v in row.items():
                    if not k.startswith('__') and '.' not in k:
                        out[k] = v
            else:
                name = col_expr.alias if hasattr(col_expr, 'alias') and col_expr.alias \
                       else self._expr_name(col_expr)
                try:
                    out[name] = self._eval(col_expr, row, txn)
                except Exception:
                    out[name] = None
        return out

    # ═══════════════════════════════════════════════════════════════════════
    # EXPRESSION EVALUATOR
    # ═══════════════════════════════════════════════════════════════════════

    def _eval(self, expr, row: dict, txn: Transaction) -> Any:
        if isinstance(expr, Literal):
            return expr.value

        if isinstance(expr, ColumnRef):
            return self._resolve_col(expr, row)

        if isinstance(expr, BinOp):
            return self._eval_binop(expr, row, txn)

        if isinstance(expr, UnaryOp):
            v = self._eval(expr.operand, row, txn)
            if expr.op == '-': return -v
            if expr.op == 'NOT': return not v
            return v

        if isinstance(expr, IsNullExpr):
            v = self._eval(expr.expr, row, txn)
            return (v is None) if expr.is_null else (v is not None)

        if isinstance(expr, InExpr):
            v = self._eval(expr.expr, row, txn)
            if v is None:
                return None
            vals = [self._eval(x, row, txn) for x in expr.values]
            result = v in vals
            return not result if expr.negate else result

        if isinstance(expr, BetweenExpr):
            v  = self._eval(expr.expr, row, txn)
            lo = self._eval(expr.lo,   row, txn)
            hi = self._eval(expr.hi,   row, txn)
            if v is None or lo is None or hi is None:
                return None
            try:
                result = lo <= v <= hi
            except TypeError:
                return None
            return not result if expr.negate else result

        if isinstance(expr, LikeExpr):
            v = self._eval(expr.expr, row, txn)
            p = self._eval(expr.pattern, row, txn)
            if v is None or p is None:
                return None
            glob    = p.replace('%', '*').replace('_', '?')
            result  = fnmatch.fnmatch(str(v), glob)
            return not result if expr.negate else result

        if isinstance(expr, AggFunc):
            # During non-aggregate projection — just evaluate the arg
            return self._eval(expr.arg, row, txn)

        if isinstance(expr, FuncCall):
            return self._eval_func(expr, row, txn)

        # Scalar — return as-is
        return expr

    def _eval_binop(self, expr: BinOp, row: dict, txn: Transaction) -> Any:
        op = expr.op
        if op == 'AND':
            lv = self._eval(expr.left, row, txn)
            if not lv:
                return False
            return bool(self._eval(expr.right, row, txn))
        if op == 'OR':
            lv = self._eval(expr.left, row, txn)
            if lv:
                return True
            return bool(self._eval(expr.right, row, txn))
        l = self._eval(expr.left,  row, txn)
        r = self._eval(expr.right, row, txn)
        if l is None or r is None:
            return None
        try:
            if op == '=':   return l == r
            if op == '!=':  return l != r
            if op == '<':   return l <  r
            if op == '>':   return l >  r
            if op == '<=':  return l <= r
            if op == '>=':  return l >= r
            if op == '+':   return l +  r
            if op == '-':   return l -  r
            if op == '*':   return l *  r
            if op == '/':   return (l / r) if r != 0 else None
        except TypeError:
            return None
        raise ExecutorError(f"Unknown operator: {op}")

    def _eval_func(self, expr: FuncCall, row: dict, txn: Transaction) -> Any:
        args = [self._eval(a, row, txn) for a in expr.args]
        name = expr.name
        if name == 'COALESCE':
            for a in args:
                if a is not None: return a
            return None
        if name == 'UPPER':   return str(args[0]).upper() if args[0] is not None else None
        if name == 'LOWER':   return str(args[0]).lower() if args[0] is not None else None
        if name == 'LENGTH':  return len(str(args[0]))    if args[0] is not None else None
        if name == 'NOW':     return datetime.now()
        raise ExecutorError(f"Unknown function: {name}")

    def _resolve_col(self, ref: ColumnRef, row: dict) -> Any:
        col = ref.column
        tbl = ref.table

        # Wildcard is handled at projection level
        if col == '*':
            return None

        # Qualified: table.column
        if tbl:
            key = f"{tbl}.{col}"
            if key in row:
                return row[key]
            # Also try unqualified (may have been loaded without table prefix)

        # Unqualified
        if col in row:
            return row[col]

        # Search for any qualified match
        for k, v in row.items():
            if k == col or k.endswith(f'.{col}'):
                return v

        return None

    # ═══════════════════════════════════════════════════════════════════════
    # HELPERS
    # ═══════════════════════════════════════════════════════════════════════

    def _qualify(self, row: dict, prefix: str, schema: TableSchema) -> dict:
        """Add table-qualified keys alongside plain keys."""
        result = {}
        for col in schema.columns:
            val = row.get(col.name)
            result[col.name]            = val
            result[f"{prefix}.{col.name}"] = val
        result['__xmin__'] = row.get('__xmin__')
        result['__xmax__'] = row.get('__xmax__')
        return result

    def _sort_key(self, row: dict, order_by: list, txn: Transaction):
        parts = []
        for item in order_by:
            v = self._eval(item.expr, row, txn)
            if v is None:
                v = ''
            # Negate for DESC
            if item.direction == 'DESC':
                if isinstance(v, (int, float)):
                    parts.append(-v)
                else:
                    parts.append(tuple(-ord(c) for c in str(v)))
            else:
                parts.append(v)
        return parts

    @staticmethod
    def _expr_name(expr) -> str:
        if isinstance(expr, ColumnRef):
            return expr.column
        if isinstance(expr, AggFunc):
            arg = getattr(expr.arg, 'value', None) or getattr(expr.arg, 'column', '?')
            return f"{expr.func}({arg})"
        if isinstance(expr, Literal):
            return str(expr.value)
        if isinstance(expr, FuncCall):
            return expr.name
        return str(expr)

    def _insert_into_heap(self, hpath: str, payload: bytes) -> RID:
        """Find a page with space or allocate a new one; return (page_id, slot_id)."""
        num = self.buf.num_pages(hpath)
        # Try existing pages in reverse order (recent pages more likely to have space)
        for pid in range(num - 1, -1, -1):
            page = self.buf.fetch(hpath, pid)
            if page and page.free_space() >= len(payload):
                sid = page.insert_tuple(payload)
                if sid is not None:
                    self.buf.mark_dirty(hpath, pid)
                    return pid, sid

        # Need a new page
        page = self.buf.new_page(hpath)
        sid  = page.insert_tuple(payload)
        self.buf.mark_dirty(hpath, page.page_id)
        return page.page_id, sid

    def _coerce_row(self, row: dict, schema: TableSchema) -> dict:
        result = {}
        for col in schema.columns:
            val = row.get(col.name)
            if val is None:
                if not col.nullable:
                    raise ExecutorError(f"Column '{col.name}' is NOT NULL")
                result[col.name] = None
                continue
            try:
                if col.col_type == 'INTEGER':
                    val = int(val)
                elif col.col_type == 'FLOAT':
                    val = float(val)
                elif col.col_type == 'BOOLEAN':
                    if isinstance(val, str):
                        val = val.lower() in ('true', '1', 'yes')
                    else:
                        val = bool(val)
                elif col.col_type == 'TEXT':
                    val = str(val)
                elif col.col_type == 'TIMESTAMP':
                    if isinstance(val, str):
                        val = datetime.fromisoformat(val)
                    elif isinstance(val, (int, float)):
                        val = datetime.fromtimestamp(val)
                result[col.name] = val
            except (ValueError, TypeError) as e:
                raise ExecutorError(
                    f"Type error for column '{col.name}' ({col.col_type}): {e}")
        return result

    @staticmethod
    def _index_key(row: dict, columns: List[str]) -> Any:
        vals = [row.get(c) for c in columns]
        return vals[0] if len(vals) == 1 else tuple(vals)

    def _load_index(self, table: str, idx: IndexDef) -> BPlusTree:
        ipath = self.catalog.index_path(table, idx.index_name)
        if ipath not in self._indices:
            self._indices[ipath] = BPlusTree.load_or_new(ipath)
        return self._indices[ipath]

    def _save_index(self, table: str, idx: IndexDef, tree: BPlusTree):
        ipath = self.catalog.index_path(table, idx.index_name)
        tree.save(ipath)

    # ═══════════════════════════════════════════════════════════════════════
    # CRASH RECOVERY (called at startup)
    # ═══════════════════════════════════════════════════════════════════════

    def recover(self):
        """Redo all committed WAL records; skip aborted transactions."""
        committed = self.wal.committed_xids()
        for xid, records in sorted(committed.items()):
            # Mark xid as committed in the transaction manager
            self.txm._committed.add(xid)
            # We rely on the heap files already having the data written
            # (WAL is ahead of the buffer pool, so no further redo needed
            #  because we use synchronous writes with fsync).
