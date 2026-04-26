"""
SQL parser: tokeniser + recursive-descent parser → AST nodes.

Supported syntax
----------------
CREATE TABLE t (col type [NOT NULL] [PRIMARY KEY], ...)
DROP TABLE t
CREATE [UNIQUE] INDEX name ON t (col, ...)
INSERT INTO t [(col,...)] VALUES (v,...)[, (v,...)]
SELECT [DISTINCT] col|* [, ...] FROM t [alias] [JOIN ...]
       [WHERE expr] [GROUP BY col,...] [HAVING expr]
       [ORDER BY col [ASC|DESC],...] [LIMIT n]
UPDATE t SET col=expr [, ...] [WHERE expr]
DELETE FROM t [WHERE expr]
BEGIN / COMMIT / ROLLBACK
"""
from __future__ import annotations

import re
from dataclasses import dataclass, field
from enum import Enum, auto
from typing import Any, List, Optional, Tuple


# ═══════════════════════════════════════════════════════════════════════════
# TOKENS
# ═══════════════════════════════════════════════════════════════════════════

class TT(Enum):
    # Literals
    NUMBER = auto(); STRING = auto(); IDENT = auto()
    # Punctuation
    LPAREN = auto(); RPAREN = auto(); COMMA = auto()
    SEMICOLON = auto(); DOT = auto(); STAR = auto()
    # Operators
    EQ = auto(); NEQ = auto(); LT = auto(); GT = auto()
    LTE = auto(); GTE = auto(); PLUS = auto(); MINUS = auto()
    SLASH = auto()
    # Keywords
    SELECT = auto(); FROM = auto(); WHERE = auto()
    INSERT = auto(); INTO = auto(); VALUES = auto()
    UPDATE = auto(); SET = auto(); DELETE = auto()
    CREATE = auto(); DROP = auto(); TABLE = auto()
    INDEX = auto(); UNIQUE = auto(); ON = auto()
    JOIN = auto(); INNER = auto(); LEFT = auto(); RIGHT = auto()
    OUTER = auto(); CROSS = auto(); FULL = auto()
    ORDER = auto(); BY = auto(); LIMIT = auto()
    GROUP = auto(); HAVING = auto(); DISTINCT = auto()
    AS = auto(); ASC = auto(); DESC = auto()
    AND = auto(); OR = auto(); NOT = auto()
    IS = auto(); IN = auto(); BETWEEN = auto(); LIKE = auto()
    NULL = auto(); TRUE = auto(); FALSE = auto()
    BEGIN = auto(); COMMIT = auto(); ROLLBACK = auto()
    PRIMARY = auto(); KEY = auto()
    # Aggregate / function names treated as keywords for easy matching
    COUNT = auto(); SUM = auto(); AVG = auto(); MIN = auto(); MAX = auto()
    COALESCE = auto(); UPPER = auto(); LOWER = auto(); LENGTH = auto()
    NOW = auto()
    # Types
    INTEGER = auto(); FLOAT = auto(); BOOLEAN = auto()
    TEXT = auto(); TIMESTAMP = auto(); VARCHAR = auto()
    EOF = auto()


_KW: dict = {
    'select': TT.SELECT, 'from': TT.FROM, 'where': TT.WHERE,
    'insert': TT.INSERT, 'into': TT.INTO, 'values': TT.VALUES,
    'update': TT.UPDATE, 'set': TT.SET, 'delete': TT.DELETE,
    'create': TT.CREATE, 'drop': TT.DROP, 'table': TT.TABLE,
    'index': TT.INDEX, 'unique': TT.UNIQUE, 'on': TT.ON,
    'join': TT.JOIN, 'inner': TT.INNER, 'left': TT.LEFT,
    'right': TT.RIGHT, 'outer': TT.OUTER, 'cross': TT.CROSS,
    'full': TT.FULL, 'order': TT.ORDER, 'by': TT.BY,
    'limit': TT.LIMIT, 'group': TT.GROUP, 'having': TT.HAVING,
    'distinct': TT.DISTINCT, 'as': TT.AS, 'asc': TT.ASC, 'desc': TT.DESC,
    'and': TT.AND, 'or': TT.OR, 'not': TT.NOT, 'is': TT.IS,
    'in': TT.IN, 'between': TT.BETWEEN, 'like': TT.LIKE,
    'null': TT.NULL, 'true': TT.TRUE, 'false': TT.FALSE,
    'begin': TT.BEGIN, 'commit': TT.COMMIT, 'rollback': TT.ROLLBACK,
    'primary': TT.PRIMARY, 'key': TT.KEY,
    'count': TT.COUNT, 'sum': TT.SUM, 'avg': TT.AVG,
    'min': TT.MIN, 'max': TT.MAX,
    'coalesce': TT.COALESCE, 'upper': TT.UPPER,
    'lower': TT.LOWER, 'length': TT.LENGTH, 'now': TT.NOW,
    'integer': TT.INTEGER, 'int': TT.INTEGER, 'bigint': TT.INTEGER,
    'float': TT.FLOAT, 'double': TT.FLOAT, 'real': TT.FLOAT, 'numeric': TT.FLOAT,
    'boolean': TT.BOOLEAN, 'bool': TT.BOOLEAN,
    'text': TT.TEXT, 'varchar': TT.VARCHAR, 'char': TT.TEXT,
    'timestamp': TT.TIMESTAMP, 'datetime': TT.TIMESTAMP,
}

_AGG_KW = {TT.COUNT, TT.SUM, TT.AVG, TT.MIN, TT.MAX}
_TYPE_KW = {TT.INTEGER, TT.FLOAT, TT.BOOLEAN, TT.TEXT, TT.VARCHAR, TT.TIMESTAMP}


@dataclass
class Token:
    tt:    TT
    val:   str
    line:  int = 0


# ═══════════════════════════════════════════════════════════════════════════
# AST NODES
# ═══════════════════════════════════════════════════════════════════════════

@dataclass
class ColumnRef:
    table:  Optional[str]
    column: str
    alias:  Optional[str] = None

    def __repr__(self):
        t = f"{self.table}." if self.table else ""
        return f"ColRef({t}{self.column})"


@dataclass
class Literal:
    value: Any

    def __repr__(self):
        return f"Lit({self.value!r})"


@dataclass
class BinOp:
    op:    str
    left:  Any
    right: Any


@dataclass
class UnaryOp:
    op:      str
    operand: Any


@dataclass
class AggFunc:
    func:     str
    arg:      Any          # ColumnRef | Literal('*')
    distinct: bool = False
    alias:    Optional[str] = None


@dataclass
class FuncCall:
    name:  str
    args:  List[Any]
    alias: Optional[str] = None


@dataclass
class IsNullExpr:
    expr:    Any
    is_null: bool   # True=IS NULL, False=IS NOT NULL


@dataclass
class InExpr:
    expr:   Any
    values: List[Any]
    negate: bool = False


@dataclass
class BetweenExpr:
    expr: Any
    lo:   Any
    hi:   Any
    negate: bool = False


@dataclass
class LikeExpr:
    expr:    Any
    pattern: Any
    negate:  bool = False


@dataclass
class TableRef:
    name:  str
    alias: Optional[str] = None


@dataclass
class JoinClause:
    join_type: str                   # INNER / LEFT / RIGHT / CROSS / FULL
    table:     TableRef
    on:        Optional[Any] = None  # ON expr


@dataclass
class OrderByItem:
    expr:      Any
    direction: str = 'ASC'


@dataclass
class ColumnDef:
    name:        str
    col_type:    str
    nullable:    bool = True
    primary_key: bool = False


# ── statement nodes ──────────────────────────────────────────────────────────

@dataclass
class SelectStmt:
    columns:    List[Any]              # exprs (ColumnRef, AggFunc, Literal…)
    from_table: Optional[TableRef]
    joins:      List[JoinClause]       = field(default_factory=list)
    where:      Optional[Any]          = None
    group_by:   List[Any]              = field(default_factory=list)
    having:     Optional[Any]          = None
    order_by:   List[OrderByItem]      = field(default_factory=list)
    limit:      Optional[int]          = None
    distinct:   bool                   = False


@dataclass
class InsertStmt:
    table:   str
    columns: Optional[List[str]]
    rows:    List[List[Any]]           # list of value-lists


@dataclass
class UpdateStmt:
    table:       str
    assignments: List[Tuple[str, Any]]
    where:       Optional[Any] = None


@dataclass
class DeleteStmt:
    table: str
    where: Optional[Any] = None


@dataclass
class CreateTableStmt:
    table:   str
    columns: List[ColumnDef]


@dataclass
class DropTableStmt:
    table:     str
    if_exists: bool = False


@dataclass
class CreateIndexStmt:
    index_name: str
    table:      str
    columns:    List[str]
    unique:     bool = False


@dataclass
class BeginStmt:    pass

@dataclass
class CommitStmt:   pass

@dataclass
class RollbackStmt: pass


# ═══════════════════════════════════════════════════════════════════════════
# LEXER
# ═══════════════════════════════════════════════════════════════════════════

class Lexer:
    def __init__(self, src: str):
        self._src   = src
        self._pos   = 0
        self._line  = 1
        self.tokens: List[Token] = []
        self._scan()

    def _scan(self):
        src = self._src
        pos = 0
        line = 1

        def advance():
            nonlocal pos, line
            c = src[pos]; pos += 1
            if c == '\n': line += 1
            return c

        def peek(offset=0):
            p = pos + offset
            return src[p] if p < len(src) else ''

        tok = self.tokens.append

        while pos < len(src):
            c = src[pos]

            # whitespace
            if c in ' \t\r\n':
                advance(); continue

            # single-line comment
            if c == '-' and peek(1) == '-':
                while pos < len(src) and src[pos] != '\n':
                    pos += 1
                continue

            # block comment
            if c == '/' and peek(1) == '*':
                pos += 2
                while pos < len(src) - 1:
                    if src[pos] == '*' and src[pos+1] == '/':
                        pos += 2; break
                    if src[pos] == '\n': line += 1
                    pos += 1
                continue

            # string literal
            if c == "'":
                pos += 1; buf = []
                while pos < len(src):
                    ch = src[pos]; pos += 1
                    if ch == "'":
                        if pos < len(src) and src[pos] == "'":
                            buf.append("'"); pos += 1
                        else:
                            break
                    else:
                        buf.append(ch)
                tok(Token(TT.STRING, ''.join(buf), line))
                continue

            # number
            if c.isdigit() or (c == '-' and peek(1).isdigit() and
                               (not self.tokens or
                                self.tokens[-1].tt in {TT.LPAREN, TT.COMMA, TT.EQ,
                                                       TT.NEQ, TT.LT, TT.GT,
                                                       TT.LTE, TT.GTE, TT.AND,
                                                       TT.OR, TT.NOT})):
                start = pos
                if src[pos] == '-': pos += 1
                while pos < len(src) and (src[pos].isdigit() or src[pos] == '.'):
                    pos += 1
                if pos < len(src) and src[pos] in 'eE':
                    pos += 1
                    if pos < len(src) and src[pos] in '+-': pos += 1
                    while pos < len(src) and src[pos].isdigit(): pos += 1
                tok(Token(TT.NUMBER, src[start:pos], line))
                continue

            # identifier / keyword
            if c.isalpha() or c == '_':
                start = pos
                while pos < len(src) and (src[pos].isalnum() or src[pos] == '_'):
                    pos += 1
                word = src[start:pos]
                tt   = _KW.get(word.lower(), TT.IDENT)
                tok(Token(tt, word, line))
                continue

            # operators / punctuation
            two = src[pos:pos+2]
            if two == '!=': tok(Token(TT.NEQ,  '!=', line)); pos += 2; continue
            if two == '<>': tok(Token(TT.NEQ,  '<>', line)); pos += 2; continue
            if two == '<=': tok(Token(TT.LTE,  '<=', line)); pos += 2; continue
            if two == '>=': tok(Token(TT.GTE,  '>=', line)); pos += 2; continue

            single = {
                '=': TT.EQ, '<': TT.LT, '>': TT.GT,
                '+': TT.PLUS, '-': TT.MINUS, '*': TT.STAR, '/': TT.SLASH,
                '(': TT.LPAREN, ')': TT.RPAREN,
                ',': TT.COMMA, ';': TT.SEMICOLON, '.': TT.DOT,
            }
            if c in single:
                tok(Token(single[c], c, line)); pos += 1; continue

            raise SyntaxError(f"Unexpected character {c!r} at line {line}")

        tok(Token(TT.EOF, '', line))


# ═══════════════════════════════════════════════════════════════════════════
# PARSER
# ═══════════════════════════════════════════════════════════════════════════

class ParseError(Exception):
    pass


class Parser:
    def __init__(self, tokens: List[Token]):
        self._toks = tokens
        self._pos  = 0

    # ── token helpers ─────────────────────────────────────────────────────────
    def _cur(self) -> Token:
        return self._toks[self._pos]

    def _peek(self, offset: int = 1) -> Token:
        p = self._pos + offset
        return self._toks[min(p, len(self._toks) - 1)]

    def _match(self, *types: TT) -> bool:
        return self._cur().tt in types

    def _eat(self, *types: TT) -> Token:
        t = self._cur()
        if types and t.tt not in types:
            expected = ' or '.join(x.name for x in types)
            raise ParseError(
                f"Expected {expected} but got {t.tt.name}({t.val!r}) at line {t.line}")
        self._pos += 1
        return t

    def _try_eat(self, *types: TT) -> Optional[Token]:
        if self._cur().tt in types:
            return self._eat()
        return None

    # ── entry point ───────────────────────────────────────────────────────────
    def parse(self):
        stmt = self._stmt()
        self._try_eat(TT.SEMICOLON)
        return stmt

    def _stmt(self):
        t = self._cur().tt
        if t == TT.SELECT:   return self._select()
        if t == TT.INSERT:   return self._insert()
        if t == TT.UPDATE:   return self._update()
        if t == TT.DELETE:   return self._delete()
        if t == TT.CREATE:   return self._create()
        if t == TT.DROP:     return self._drop()
        if t == TT.BEGIN:    self._eat(); return BeginStmt()
        if t == TT.COMMIT:   self._eat(); return CommitStmt()
        if t == TT.ROLLBACK: self._eat(); return RollbackStmt()
        raise ParseError(f"Unknown statement starting with {self._cur().val!r}")

    # ── CREATE ────────────────────────────────────────────────────────────────
    def _create(self):
        self._eat(TT.CREATE)
        unique = bool(self._try_eat(TT.UNIQUE))
        if self._match(TT.TABLE):
            self._eat(TT.TABLE)
            name = self._eat(TT.IDENT).val
            self._eat(TT.LPAREN)
            cols = []
            while not self._match(TT.RPAREN):
                cols.append(self._col_def())
                self._try_eat(TT.COMMA)
            self._eat(TT.RPAREN)
            return CreateTableStmt(name, cols)
        if self._match(TT.INDEX):
            self._eat(TT.INDEX)
            idx_name = self._eat(TT.IDENT).val
            self._eat(TT.ON)
            tname = self._eat(TT.IDENT).val
            self._eat(TT.LPAREN)
            icols = [self._eat(TT.IDENT).val]
            while self._try_eat(TT.COMMA):
                icols.append(self._eat(TT.IDENT).val)
            self._eat(TT.RPAREN)
            return CreateIndexStmt(idx_name, tname, icols, unique)
        raise ParseError("Expected TABLE or INDEX after CREATE")

    def _col_def(self) -> ColumnDef:
        # Handle inline PRIMARY KEY constraint shorthand
        if self._match(TT.PRIMARY):
            self._eat(TT.PRIMARY); self._eat(TT.KEY)
            self._eat(TT.LPAREN)
            _ = self._eat(TT.IDENT).val   # column name (ignored here)
            self._eat(TT.RPAREN)
            return None                    # caller should skip None

        name     = self._eat(TT.IDENT).val
        type_tok = self._eat(*_TYPE_KW)
        col_type = self._sql_type(type_tok.tt)

        # optional length specifier e.g. VARCHAR(255) — consume and ignore
        if self._match(TT.LPAREN):
            self._eat(TT.LPAREN)
            self._eat(TT.NUMBER)
            self._eat(TT.RPAREN)

        nullable    = True
        primary_key = False

        while self._match(TT.NOT, TT.PRIMARY, TT.UNIQUE):
            if self._try_eat(TT.NOT):
                self._eat(TT.NULL)
                nullable = False
            elif self._try_eat(TT.PRIMARY):
                self._eat(TT.KEY)
                primary_key = True
                nullable    = False
            elif self._try_eat(TT.UNIQUE):
                pass  # noted but not specially handled

        return ColumnDef(name, col_type, nullable, primary_key)

    @staticmethod
    def _sql_type(tt: TT) -> str:
        return {
            TT.INTEGER: 'INTEGER', TT.FLOAT: 'FLOAT', TT.BOOLEAN: 'BOOLEAN',
            TT.TEXT: 'TEXT', TT.VARCHAR: 'TEXT', TT.TIMESTAMP: 'TIMESTAMP',
        }[tt]

    # ── DROP ─────────────────────────────────────────────────────────────────
    def _drop(self):
        self._eat(TT.DROP); self._eat(TT.TABLE)
        if_exists = False
        if self._match(TT.IDENT) and self._cur().val.upper() == 'IF':
            self._eat(); self._eat()  # IF EXISTS
            if_exists = True
        name = self._eat(TT.IDENT).val
        return DropTableStmt(name, if_exists)

    # ── INSERT ────────────────────────────────────────────────────────────────
    def _insert(self):
        self._eat(TT.INSERT); self._eat(TT.INTO)
        table = self._eat(TT.IDENT).val
        cols  = None
        if self._match(TT.LPAREN) and self._peek(1).tt == TT.IDENT:
            self._eat(TT.LPAREN)
            cols = [self._eat(TT.IDENT).val]
            while self._try_eat(TT.COMMA):
                cols.append(self._eat(TT.IDENT).val)
            self._eat(TT.RPAREN)
        self._eat(TT.VALUES)
        rows = []
        while True:
            self._eat(TT.LPAREN)
            row = [self._expr()]
            while self._try_eat(TT.COMMA):
                row.append(self._expr())
            self._eat(TT.RPAREN)
            rows.append(row)
            if not self._try_eat(TT.COMMA):
                break
        return InsertStmt(table, cols, rows)

    # ── UPDATE ────────────────────────────────────────────────────────────────
    def _update(self):
        self._eat(TT.UPDATE)
        table = self._eat(TT.IDENT).val
        self._eat(TT.SET)
        assigns = []
        col = self._eat(TT.IDENT).val
        self._eat(TT.EQ)
        assigns.append((col, self._expr()))
        while self._try_eat(TT.COMMA):
            col = self._eat(TT.IDENT).val
            self._eat(TT.EQ)
            assigns.append((col, self._expr()))
        where = None
        if self._try_eat(TT.WHERE):
            where = self._expr()
        return UpdateStmt(table, assigns, where)

    # ── DELETE ────────────────────────────────────────────────────────────────
    def _delete(self):
        self._eat(TT.DELETE); self._eat(TT.FROM)
        table = self._eat(TT.IDENT).val
        where = None
        if self._try_eat(TT.WHERE):
            where = self._expr()
        return DeleteStmt(table, where)

    # ── SELECT ────────────────────────────────────────────────────────────────
    def _select(self):
        self._eat(TT.SELECT)
        distinct = bool(self._try_eat(TT.DISTINCT))

        # column list
        cols = self._select_cols()

        # FROM
        from_table = None
        joins: List[JoinClause] = []
        if self._try_eat(TT.FROM):
            from_table = self._table_ref()
            while self._match(TT.JOIN, TT.INNER, TT.LEFT, TT.RIGHT, TT.CROSS, TT.FULL):
                joins.append(self._join())

        where = None
        if self._try_eat(TT.WHERE):
            where = self._expr()

        group_by = []
        if self._try_eat(TT.GROUP):
            self._eat(TT.BY)
            group_by.append(self._expr())
            while self._try_eat(TT.COMMA):
                group_by.append(self._expr())

        having = None
        if self._try_eat(TT.HAVING):
            having = self._expr()

        order_by = []
        if self._try_eat(TT.ORDER):
            self._eat(TT.BY)
            order_by.append(self._order_by_item())
            while self._try_eat(TT.COMMA):
                order_by.append(self._order_by_item())

        limit = None
        if self._try_eat(TT.LIMIT):
            limit = int(self._eat(TT.NUMBER).val)

        return SelectStmt(cols, from_table, joins, where,
                          group_by, having, order_by, limit, distinct)

    def _select_cols(self):
        cols = [self._select_col()]
        while self._try_eat(TT.COMMA):
            cols.append(self._select_col())
        return cols

    def _select_col(self):
        expr = self._expr()
        alias = None
        if self._try_eat(TT.AS):
            alias = self._eat(TT.IDENT).val
        elif self._match(TT.IDENT):
            # implicit alias: SELECT col name
            alias = self._eat(TT.IDENT).val
        if alias:
            if isinstance(expr, ColumnRef):
                expr.alias = alias
            elif isinstance(expr, AggFunc):
                expr.alias = alias
            elif isinstance(expr, FuncCall):
                expr.alias = alias
        return expr

    def _table_ref(self) -> TableRef:
        name  = self._eat(TT.IDENT).val
        alias = None
        if self._try_eat(TT.AS):
            alias = self._eat(TT.IDENT).val
        elif self._match(TT.IDENT) and self._cur().val.upper() not in {
                'WHERE', 'JOIN', 'ON', 'GROUP', 'ORDER', 'HAVING', 'LIMIT',
                'INNER', 'LEFT', 'RIGHT', 'CROSS', 'FULL', 'OUTER'}:
            alias = self._eat(TT.IDENT).val
        return TableRef(name, alias)

    def _join(self) -> JoinClause:
        jtype = 'INNER'
        if self._try_eat(TT.LEFT):
            jtype = 'LEFT'; self._try_eat(TT.OUTER)
        elif self._try_eat(TT.RIGHT):
            jtype = 'RIGHT'; self._try_eat(TT.OUTER)
        elif self._try_eat(TT.CROSS):
            jtype = 'CROSS'
        elif self._try_eat(TT.FULL):
            self._try_eat(TT.OUTER); jtype = 'FULL'
        elif self._try_eat(TT.INNER):
            jtype = 'INNER'
        self._eat(TT.JOIN)
        tref = self._table_ref()
        on   = None
        if self._try_eat(TT.ON):
            on = self._expr()
        return JoinClause(jtype, tref, on)

    def _order_by_item(self) -> OrderByItem:
        expr = self._expr()
        dir_ = 'ASC'
        if self._try_eat(TT.DESC):
            dir_ = 'DESC'
        elif self._try_eat(TT.ASC):
            dir_ = 'ASC'
        return OrderByItem(expr, dir_)

    # ── EXPRESSIONS (recursive descent, precedence climbing) ─────────────────

    def _expr(self) -> Any:
        return self._or_expr()

    def _or_expr(self) -> Any:
        left = self._and_expr()
        while self._try_eat(TT.OR):
            left = BinOp('OR', left, self._and_expr())
        return left

    def _and_expr(self) -> Any:
        left = self._not_expr()
        while self._try_eat(TT.AND):
            left = BinOp('AND', left, self._not_expr())
        return left

    def _not_expr(self) -> Any:
        if self._try_eat(TT.NOT):
            return UnaryOp('NOT', self._not_expr())
        return self._cmp_expr()

    def _cmp_expr(self) -> Any:
        left = self._add_expr()
        t = self._cur()
        if t.tt == TT.IS:
            self._eat()
            negate = bool(self._try_eat(TT.NOT))
            self._eat(TT.NULL)
            return IsNullExpr(left, not negate)   # is_null=True means IS NULL
        if t.tt == TT.NOT and self._peek(1).tt == TT.IN:
            self._eat(); self._eat()
            return InExpr(left, self._in_list(), negate=True)
        if t.tt == TT.IN:
            self._eat()
            return InExpr(left, self._in_list())
        if t.tt == TT.BETWEEN:
            self._eat()
            lo = self._add_expr(); self._eat(TT.AND); hi = self._add_expr()
            return BetweenExpr(left, lo, hi)
        if t.tt == TT.NOT and self._peek(1).tt == TT.BETWEEN:
            self._eat(); self._eat()
            lo = self._add_expr(); self._eat(TT.AND); hi = self._add_expr()
            return BetweenExpr(left, lo, hi, negate=True)
        if t.tt == TT.LIKE:
            self._eat()
            return LikeExpr(left, self._add_expr())
        if t.tt == TT.NOT and self._peek(1).tt == TT.LIKE:
            self._eat(); self._eat()
            return LikeExpr(left, self._add_expr(), negate=True)
        op_map = {TT.EQ: '=', TT.NEQ: '!=', TT.LT: '<',
                  TT.GT: '>', TT.LTE: '<=', TT.GTE: '>='}
        if t.tt in op_map:
            self._eat()
            return BinOp(op_map[t.tt], left, self._add_expr())
        return left

    def _in_list(self):
        self._eat(TT.LPAREN)
        vals = [self._expr()]
        while self._try_eat(TT.COMMA):
            vals.append(self._expr())
        self._eat(TT.RPAREN)
        return vals

    def _add_expr(self) -> Any:
        left = self._mul_expr()
        while self._match(TT.PLUS, TT.MINUS):
            op   = self._eat().val
            left = BinOp(op, left, self._mul_expr())
        return left

    def _mul_expr(self) -> Any:
        left = self._unary_expr()
        while self._match(TT.STAR, TT.SLASH):
            op   = self._eat().val
            left = BinOp(op, left, self._unary_expr())
        return left

    def _unary_expr(self) -> Any:
        if self._try_eat(TT.MINUS):
            return UnaryOp('-', self._primary())
        return self._primary()

    def _primary(self) -> Any:
        t = self._cur()

        # aggregate
        if t.tt in _AGG_KW:
            self._eat()
            self._eat(TT.LPAREN)
            distinct = bool(self._try_eat(TT.DISTINCT))
            if self._match(TT.STAR):
                arg = Literal('*'); self._eat()
            else:
                arg = self._expr()
            self._eat(TT.RPAREN)
            return AggFunc(t.val.upper(), arg, distinct)

        # scalar functions
        if t.tt in {TT.COALESCE, TT.UPPER, TT.LOWER, TT.LENGTH, TT.NOW}:
            name = self._eat().val.upper()
            self._eat(TT.LPAREN)
            args = []
            if not self._match(TT.RPAREN):
                args.append(self._expr())
                while self._try_eat(TT.COMMA):
                    args.append(self._expr())
            self._eat(TT.RPAREN)
            return FuncCall(name, args)

        # literals
        if t.tt == TT.NUMBER:
            self._eat()
            v = t.val
            return Literal(float(v) if '.' in v or 'e' in v.lower() else int(v))
        if t.tt == TT.STRING:
            self._eat(); return Literal(t.val)
        if t.tt == TT.NULL:
            self._eat(); return Literal(None)
        if t.tt == TT.TRUE:
            self._eat(); return Literal(True)
        if t.tt == TT.FALSE:
            self._eat(); return Literal(False)

        # wildcard
        if t.tt == TT.STAR:
            self._eat(); return ColumnRef(None, '*')

        # qualified column: table.col or plain col (or keyword used as col)
        if t.tt in (TT.IDENT, ) or t.tt in _KW.values():
            name = self._eat().val
            if self._match(TT.DOT):
                self._eat()
                col = self._eat().val
                return ColumnRef(name, col)
            return ColumnRef(None, name)

        # parenthesised expression
        if t.tt == TT.LPAREN:
            self._eat()
            inner = self._expr()
            self._eat(TT.RPAREN)
            return inner

        raise ParseError(
            f"Unexpected token {t.tt.name}({t.val!r}) at line {t.line}")


# ═══════════════════════════════════════════════════════════════════════════
# Public API
# ═══════════════════════════════════════════════════════════════════════════

def parse(sql: str):
    """Parse a SQL string and return an AST node."""
    tokens = Lexer(sql).tokens
    return Parser(tokens).parse()
