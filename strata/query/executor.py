"""
CQL Executor — maps parsed AST nodes to Coordinator / LSMTree operations.

The Executor is stateless with respect to keyspace selection; the caller
(StrataEngine) injects the current keyspace via the `keyspace` parameter
on each execute() call.
"""
from __future__ import annotations

import time
from typing import Any, Dict, List, Optional, Tuple, TYPE_CHECKING

from .parser import (
    parse,
    CreateKeyspaceStmt, DropKeyspaceStmt, UseStmt,
    CreateTableStmt, DropTableStmt, TruncateStmt,
    InsertStmt, SelectStmt, UpdateStmt, DeleteStmt,
    DescribeStmt, WhereClause, ColDef,
)
from ..catalog import Catalog, TableSchema, ColumnDef, VALID_TYPES
from ..consistency import ConsistencyLevel, Coordinator

if TYPE_CHECKING:
    from ..engine import StrataNode


_CL_MAP = {
    'ONE':    ConsistencyLevel.ONE,
    'QUORUM': ConsistencyLevel.QUORUM,
    'ALL':    ConsistencyLevel.ALL,
}


class ExecutionError(Exception):
    pass


class QueryResult:
    def __init__(self, rows: Optional[List[dict]] = None, message: str = ''):
        self.rows    = rows or []
        self.message = message

    def __repr__(self):
        if self.rows:
            return f"QueryResult({len(self.rows)} rows)"
        return f"QueryResult(ok: {self.message!r})"


class Executor:
    """
    Executes parsed CQL AST nodes against a StrataEngine.

    Parameters
    ----------
    catalog     : shared Catalog instance
    coordinator : Coordinator for routing reads/writes across nodes
    local_node  : the node this executor runs on (for DDL / admin ops)
    """

    def __init__(self, catalog: Catalog, coordinator: Coordinator,
                 local_node: 'StrataNode'):
        self._catalog     = catalog
        self._coord       = coordinator
        self._local_node  = local_node
        self._keyspace: Optional[str] = None

    # ── public entry point ────────────────────────────────────────────────────

    def execute(self, cql: str,
                keyspace: Optional[str] = None,
                consistency: str = 'QUORUM') -> QueryResult:
        self._keyspace = keyspace
        cl = _CL_MAP.get(consistency.upper(), ConsistencyLevel.QUORUM)
        stmt = parse(cql)

        # Keyspace DDL
        if isinstance(stmt, CreateKeyspaceStmt):
            return self._create_keyspace(stmt)
        if isinstance(stmt, DropKeyspaceStmt):
            return self._drop_keyspace(stmt)
        if isinstance(stmt, UseStmt):
            self._keyspace = stmt.keyspace
            return QueryResult(message=f"Using keyspace {stmt.keyspace!r}")
        if isinstance(stmt, DescribeStmt):
            return self._describe(stmt)

        # Table DDL
        if isinstance(stmt, CreateTableStmt):
            return self._create_table(stmt)
        if isinstance(stmt, DropTableStmt):
            return self._drop_table(stmt)
        if isinstance(stmt, TruncateStmt):
            return self._truncate(stmt)

        # DML
        if isinstance(stmt, InsertStmt):
            return self._insert(stmt, cl)
        if isinstance(stmt, SelectStmt):
            return self._select(stmt, cl)
        if isinstance(stmt, UpdateStmt):
            return self._update(stmt, cl)
        if isinstance(stmt, DeleteStmt):
            return self._delete(stmt, cl)

        raise ExecutionError(f"Unsupported statement type: {type(stmt).__name__}")

    # ── DDL ──────────────────────────────────────────────────────────────────

    def _create_keyspace(self, stmt: CreateKeyspaceStmt) -> QueryResult:
        self._catalog.create_keyspace(
            stmt.name, stmt.replication_class, stmt.replication_factor)
        self._local_node.on_keyspace_created(stmt.name)
        return QueryResult(
            message=f"Keyspace {stmt.name!r} created "
                    f"(rf={stmt.replication_factor})")

    def _drop_keyspace(self, stmt: DropKeyspaceStmt) -> QueryResult:
        self._catalog.drop_keyspace(stmt.name)
        self._local_node.on_keyspace_dropped(stmt.name)
        return QueryResult(message=f"Keyspace {stmt.name!r} dropped")

    def _create_table(self, stmt: CreateTableStmt) -> QueryResult:
        ks = self._resolve_keyspace(stmt.keyspace)
        cols = []
        for cd in stmt.columns:                    # cd is ColDef(name, col_type, static)
            ctype = cd.col_type.upper()
            if ctype not in VALID_TYPES:
                raise ExecutionError(f"Unknown column type {ctype!r}")
            cols.append(ColumnDef(cd.name, ctype, cd.static))
        schema = TableSchema(
            ks, stmt.name, cols,
            stmt.partition_keys, stmt.clustering_keys,
        )
        self._catalog.create_table(schema)
        self._local_node.on_table_created(ks, stmt.name)
        return QueryResult(message=f"Table {ks}.{stmt.name} created")

    def _drop_table(self, stmt: DropTableStmt) -> QueryResult:
        ks = self._resolve_keyspace(stmt.keyspace)
        self._catalog.drop_table(ks, stmt.name)
        self._local_node.on_table_dropped(ks, stmt.name)
        return QueryResult(message=f"Table {ks}.{stmt.name} dropped")

    def _truncate(self, stmt: TruncateStmt) -> QueryResult:
        ks = self._resolve_keyspace(stmt.keyspace)
        self._local_node.local_truncate(ks, stmt.name)
        return QueryResult(message=f"Table {ks}.{stmt.name} truncated")

    # ── DML ──────────────────────────────────────────────────────────────────

    def _insert(self, stmt: InsertStmt, cl: ConsistencyLevel) -> QueryResult:
        ks     = self._resolve_keyspace(stmt.keyspace)
        schema = self._catalog.get_table(ks, stmt.table)
        ts     = _now_micros()

        values = dict(zip(stmt.columns, stmt.values))
        pk     = self._build_pk(schema, values)
        ck     = self._build_ck(schema, values)
        cols   = self._build_value_cols(schema, values)

        self._coord.write(ks, stmt.table, pk, ck, cols, ts,
                          tombstone=False, consistency=cl)
        return QueryResult(message="INSERT 1")

    def _select(self, stmt: SelectStmt, cl: ConsistencyLevel) -> QueryResult:
        ks     = self._resolve_keyspace(stmt.keyspace)
        schema = self._catalog.get_table(ks, stmt.table)
        where  = stmt.where

        pk = self._extract_pk(schema, where)
        if pk is None:
            raise ExecutionError(
                "SELECT requires an equality condition on all partition key columns "
                "(add ALLOW FILTERING for full scans if supported)")

        ck, ck_lo, ck_hi, ck_lo_ex, ck_hi_ex = self._extract_ck_range(schema, where)

        order_desc = (stmt.order_by is not None and stmt.order_by[1] == 'DESC')

        rows = self._coord.read(
            ks, stmt.table, pk, ck,
            ck_lo=ck_lo, ck_hi=ck_hi,
            ck_lo_exclusive=ck_lo_ex, ck_hi_exclusive=ck_hi_ex,
            reversed_=order_desc,
            limit=stmt.limit,
            consistency=cl,
        )

        # COUNT(*) shortcut
        if stmt.count_only:
            return QueryResult(rows=[{'count': len(rows)}])

        # Unpack __pk__ / __ck__ back to named columns
        rows = [self._unpack_row(r, schema) for r in rows]

        # Apply non-key ALLOW FILTERING predicates (ck_conds on non-CK cols)
        ck_set = set(schema.clustering_keys)
        filter_conds = [(col, op, val)
                        for col, op, val in (where.ck_conditions if where else [])
                        if col not in ck_set]
        if filter_conds:
            rows = [r for r in rows if self._eval_filters(r, filter_conds)]

        projected = [self._project(row, stmt.columns, schema) for row in rows]
        return QueryResult(rows=projected)

    def _update(self, stmt: UpdateStmt, cl: ConsistencyLevel) -> QueryResult:
        ks     = self._resolve_keyspace(stmt.keyspace)
        schema = self._catalog.get_table(ks, stmt.table)
        ts     = _now_micros()
        where  = stmt.where

        pk = self._extract_pk(schema, where)
        if pk is None:
            raise ExecutionError(
                "UPDATE requires equality conditions on all partition key columns")

        ck, _, _, _, _ = self._extract_ck_range(schema, where)

        raw_rows = self._coord.read(ks, stmt.table, pk, ck,
                                    consistency=cl)
        if not raw_rows:
            cols = {}
            for col, val in stmt.assignments:
                col_def = schema.get_col(col)
                if col_def:
                    val = self._catalog.coerce(val, col_def.col_type)
                cols[col] = val
            self._coord.write(ks, stmt.table, pk, ck or '', cols, ts,
                              consistency=cl)
        else:
            for raw_row in raw_rows:
                row_ck = raw_row.get('__ck__', '')
                cols = {k: v for k, v in raw_row.items()
                        if not k.startswith('__')}
                for col, val in stmt.assignments:
                    col_def = schema.get_col(col)
                    if col_def:
                        val = self._catalog.coerce(val, col_def.col_type)
                    cols[col] = val
                self._coord.write(ks, stmt.table, pk, row_ck, cols, ts,
                                  consistency=cl)

        return QueryResult(message=f"UPDATE {len(raw_rows) or 1}")

    def _delete(self, stmt: DeleteStmt, cl: ConsistencyLevel) -> QueryResult:
        ks     = self._resolve_keyspace(stmt.keyspace)
        schema = self._catalog.get_table(ks, stmt.table)
        ts     = _now_micros()
        where  = stmt.where

        pk = self._extract_pk(schema, where)
        if pk is None:
            raise ExecutionError(
                "DELETE requires equality conditions on all partition key columns")

        ck, ck_lo, ck_hi, ck_lo_ex, ck_hi_ex = self._extract_ck_range(schema, where)

        if ck is not None:
            self._coord.write(ks, stmt.table, pk, ck, {}, ts,
                              tombstone=True, consistency=cl)
            return QueryResult(message="DELETE 1")

        rows = self._coord.read(
            ks, stmt.table, pk, None,
            ck_lo=ck_lo, ck_hi=ck_hi,
            ck_lo_exclusive=ck_lo_ex, ck_hi_exclusive=ck_hi_ex,
            consistency=cl,
        )
        for row in rows:
            self._coord.write(ks, stmt.table, pk,
                              row.get('__ck__', ''), {}, ts,
                              tombstone=True, consistency=cl)
        return QueryResult(message=f"DELETE {len(rows)}")

    def _describe(self, stmt: DescribeStmt) -> QueryResult:
        if stmt.target == 'KEYSPACES':
            names = self._catalog.all_keyspaces()
            return QueryResult(rows=[{'keyspace': n} for n in names])

        if stmt.target == 'TABLES':
            ks = self._resolve_keyspace(None)
            tbls = self._catalog.all_tables(ks)
            return QueryResult(rows=[{'table': t} for t in tbls])

        if stmt.target == 'KEYSPACE':
            ks_def = self._catalog.get_keyspace(stmt.name)
            return QueryResult(rows=[{
                'keyspace': ks_def.name,
                'replication_class': ks_def.replication_class,
                'replication_factor': ks_def.replication_factor,
            }])

        # TABLE
        ks_name  = stmt.keyspace or self._keyspace
        tbl_name = stmt.name
        if not ks_name or not tbl_name:
            raise ExecutionError("DESCRIBE TABLE requires keyspace context")
        schema = self._catalog.get_table(ks_name, tbl_name)
        rows = []
        for col in schema.columns:
            role = ('partition_key' if schema.is_partition_key(col.name) else
                    'clustering_key' if schema.is_clustering_key(col.name) else
                    'regular')
            rows.append({'column': col.name, 'type': col.col_type,
                         'role': role, 'static': col.static})
        return QueryResult(rows=rows)

    # ── helpers ──────────────────────────────────────────────────────────────

    def _resolve_keyspace(self, explicit: Optional[str]) -> str:
        ks = explicit or self._keyspace
        if not ks:
            raise ExecutionError("No keyspace selected (USE <keyspace> first)")
        if not self._catalog.keyspace_exists(ks):
            raise ExecutionError(f"Keyspace {ks!r} does not exist")
        return ks

    def _build_pk(self, schema: TableSchema, values: dict) -> str:
        parts = []
        for pk_col in schema.partition_keys:
            v = values.get(pk_col)
            if v is None:
                raise ExecutionError(
                    f"Missing partition key column {pk_col!r}")
            parts.append(str(v))
        return ':'.join(parts)

    def _build_ck(self, schema: TableSchema, values: dict) -> str:
        parts = []
        for ck_col in schema.clustering_keys:
            v = values.get(ck_col, '')
            parts.append(str(v) if v is not None else '')
        return ':'.join(parts)

    def _build_value_cols(self, schema: TableSchema, values: dict) -> dict:
        pk_set = set(schema.partition_keys) | set(schema.clustering_keys)
        cols = {}
        for col_def in schema.columns:
            if col_def.name in pk_set:
                continue
            if col_def.name in values:
                cols[col_def.name] = self._catalog.coerce(
                    values[col_def.name], col_def.col_type)
        return cols

    def _extract_pk(self, schema: TableSchema,
                    where: Optional[WhereClause]) -> Optional[str]:
        if not where:
            return None
        parts = []
        for pk_col in schema.partition_keys:
            val = where.pk_conditions.get(pk_col)
            if val is None:
                return None
            parts.append(str(val))
        return ':'.join(parts)

    def _extract_ck_range(self, schema: TableSchema,
                          where: Optional[WhereClause]
                          ) -> Tuple[Optional[str], Optional[str],
                                     Optional[str], bool, bool]:
        """Returns (ck_exact, ck_lo, ck_hi, lo_exclusive, hi_exclusive)."""
        if not where or not schema.clustering_keys:
            return (None, None, None, False, False)

        first_ck = schema.clustering_keys[0]
        ck_set   = set(schema.clustering_keys)

        # Build per-column conditions from ck_conditions list
        ck_by_col: Dict[str, Dict[str, Any]] = {}
        for col, op, val in where.ck_conditions:
            if col in ck_set:
                ck_by_col.setdefault(col, {})[op] = val

        # Also check pk_conditions for CK cols with '=' (parser puts all '=' there)
        for col in schema.clustering_keys:
            if col in where.pk_conditions:
                ck_by_col.setdefault(col, {})['='] = where.pk_conditions[col]

        # Exact equality on first CK → point lookup
        first_conds = ck_by_col.get(first_ck, {})
        if '=' in first_conds:
            return (str(first_conds['=']), None, None, False, False)

        # Range
        ck_lo = ck_hi = None
        lo_ex = hi_ex = False
        for ck_col in schema.clustering_keys:
            cond = ck_by_col.get(ck_col, {})
            for op, val in cond.items():
                sv = str(val)
                if op == '>':
                    ck_lo, lo_ex = sv, True
                elif op == '>=':
                    ck_lo, lo_ex = sv, False
                elif op == '<':
                    ck_hi, hi_ex = sv, True
                elif op == '<=':
                    ck_hi, hi_ex = sv, False

        return (None, ck_lo, ck_hi, lo_ex, hi_ex)

    @staticmethod
    def _unpack_row(row: dict, schema: TableSchema) -> dict:
        """Expand __pk__ and __ck__ composite strings back into named columns."""
        out = {k: v for k, v in row.items() if not k.startswith('__')}

        pk_raw = row.get('__pk__', '')
        pk_parts = pk_raw.split(':') if pk_raw else []
        for i, col in enumerate(schema.partition_keys):
            out[col] = pk_parts[i] if i < len(pk_parts) else None

        ck_raw = row.get('__ck__', '')
        ck_parts = ck_raw.split(':') if ck_raw else []
        for i, col in enumerate(schema.clustering_keys):
            out[col] = ck_parts[i] if i < len(ck_parts) else None

        return out

    def _project(self, row: dict, columns: List[str],
                 schema: TableSchema) -> dict:
        if columns == ['*']:
            return {k: v for k, v in row.items() if not k.startswith('__')}
        return {col: row.get(col) for col in columns}

    def _eval_filters(self, row: dict, conditions: list) -> bool:
        for col, op, val in conditions:
            rv = row.get(col)
            try:
                if op == '=':    result = rv == val
                elif op == '!=': result = rv != val
                elif op == '>':  result = rv >  val
                elif op == '>=': result = rv >= val
                elif op == '<':  result = rv <  val
                elif op == '<=': result = rv <= val
                else:            result = True
            except TypeError:
                result = False
            if not result:
                return False
        return True


def _now_micros() -> int:
    return int(time.time() * 1_000_000)
