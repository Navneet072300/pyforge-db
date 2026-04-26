"""
Query planner: converts an AST into a physical plan.

Cost model (simplified):
  seq_scan  cost = num_pages
  idx_scan  cost = log2(num_rows) + selectivity * num_rows
  nested_loop_join cost = outer_cost * inner_rows

A predicate on an indexed column triggers an index scan when it uses
an equality or range operator (=, <, >, <=, >=).
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, List, Optional

from ..query.parser import (
    SelectStmt, BinOp, ColumnRef, Literal, AggFunc, FuncCall,
    JoinClause, OrderByItem,
)


# ═══════════════════════════════════════════════════════════════════════════
# Plan nodes
# ═══════════════════════════════════════════════════════════════════════════

@dataclass
class SeqScan:
    table: str
    alias: Optional[str] = None
    predicate: Optional[Any] = None


@dataclass
class IndexScan:
    table:     str
    alias:     Optional[str]
    index_col: str
    op:        str        # '=', '<', '>', '<=', '>='
    key:       Any
    predicate: Optional[Any] = None   # residual filter after index lookup


@dataclass
class NestedLoopJoin:
    outer:     Any
    inner:     Any
    condition: Optional[Any]
    join_type: str = 'INNER'


@dataclass
class Filter:
    child:     Any
    predicate: Any


@dataclass
class Projection:
    child:   Any
    columns: List[Any]


@dataclass
class Sort:
    child:    Any
    order_by: List[OrderByItem]


@dataclass
class Limit:
    child: Any
    count: int


@dataclass
class Aggregate:
    child:      Any
    group_by:   List[Any]
    aggregates: List[AggFunc]
    having:     Optional[Any]
    select_cols: List[Any]


# ═══════════════════════════════════════════════════════════════════════════
# Planner
# ═══════════════════════════════════════════════════════════════════════════

class Planner:
    def __init__(self, catalog):
        self.catalog = catalog

    # ── public ───────────────────────────────────────────────────────────────
    def plan_select(self, stmt: SelectStmt) -> Any:
        # Build the access plan for each table in FROM / JOIN
        if stmt.from_table is None:
            # SELECT without FROM (e.g. SELECT 1+1)
            return Projection(None, stmt.columns)

        if stmt.joins:
            # With JOINs: never push the WHERE into individual table scans —
            # cross-table predicates can't be evaluated before the join.
            plan = self._scan(stmt.from_table.name, stmt.from_table.alias, None)
            for jc in stmt.joins:
                inner = self._scan(jc.table.name, jc.table.alias, None)
                plan  = NestedLoopJoin(plan, inner, jc.on, jc.join_type)
            if stmt.where:
                plan = Filter(plan, stmt.where)
        else:
            # No JOINs: try to push WHERE into an index scan on the single table.
            plan = self._scan(stmt.from_table.name, stmt.from_table.alias, stmt.where)
            if stmt.where and not isinstance(plan, IndexScan):
                plan = Filter(plan, stmt.where)
            elif stmt.where and isinstance(plan, IndexScan) and plan.predicate:
                plan = Filter(plan, plan.predicate)

        # GROUP BY / aggregates
        aggs = self._extract_aggs(stmt.columns)
        if stmt.group_by or aggs:
            plan = Aggregate(plan, stmt.group_by, aggs, stmt.having, stmt.columns)
        else:
            plan = Projection(plan, stmt.columns)

        if stmt.order_by:
            plan = Sort(plan, stmt.order_by)
        if stmt.limit is not None:
            plan = Limit(plan, stmt.limit)

        return plan

    # ── internals ────────────────────────────────────────────────────────────

    def _scan(self, table: str, alias: Optional[str],
              where: Optional[Any]) -> Any:
        """Choose between SeqScan and IndexScan."""
        if not self.catalog.table_exists(table):
            raise RuntimeError(f"Table '{table}' does not exist")

        schema = self.catalog.get_table(table)

        # Try to push an equality / range predicate into an index scan
        if where is not None:
            hit = self._find_index_predicate(where, schema, alias or table)
            if hit:
                col, op, key, residual = hit
                return IndexScan(table, alias, col, op, key, residual)

        return SeqScan(table, alias, where)

    def _find_index_predicate(self, expr, schema, table_alias):
        """
        Detect a predicate of the form  col OP value  where col has an index.
        Returns (col_name, op, key_value, residual_expr) or None.
        """
        if not isinstance(expr, BinOp):
            return None

        op = expr.op
        if op not in {'=', '<', '>', '<=', '>='}:
            if op == 'AND':
                # Try left branch first
                hit = self._find_index_predicate(expr.left, schema, table_alias)
                if hit:
                    # residual = the AND of the other branch
                    col, iop, key, inner_res = hit
                    residual = expr.right
                    if inner_res:
                        residual = BinOp('AND', inner_res, expr.right)
                    return col, iop, key, residual
                hit = self._find_index_predicate(expr.right, schema, table_alias)
                if hit:
                    col, iop, key, inner_res = hit
                    residual = expr.left
                    if inner_res:
                        residual = BinOp('AND', inner_res, expr.left)
                    return col, iop, key, residual
            return None

        # col OP literal
        left, right = expr.left, expr.right
        col_ref, key_expr = None, None
        actual_op = op

        if isinstance(left, ColumnRef) and isinstance(right, Literal):
            col_ref, key_expr = left, right
        elif isinstance(right, ColumnRef) and isinstance(left, Literal):
            col_ref, key_expr = right, left
            # Flip operator
            actual_op = {'<': '>', '>': '<', '<=': '>=', '>=': '<=', '=': '='}.get(op, op)
        else:
            return None

        # Does the column name match an indexed column?
        col_name = col_ref.column
        if col_ref.table and col_ref.table.lower() not in (
                schema.name.lower(), (table_alias or '').lower()):
            return None
        try:
            schema.col_index(col_name)
        except KeyError:
            return None

        if not schema.index_for_col(col_name):
            return None

        return col_name, actual_op, key_expr.value, None

    @staticmethod
    def _extract_aggs(cols: list) -> List[AggFunc]:
        aggs = []
        for c in cols:
            if isinstance(c, AggFunc):
                aggs.append(c)
        return aggs
