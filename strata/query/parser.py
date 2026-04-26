"""
CQL-like parser for Strata.

Supported statements
--------------------
CREATE KEYSPACE ks WITH replication = {'class': 'X', 'replication_factor': N}
DROP KEYSPACE ks
USE ks
CREATE TABLE [ks.]t (col type, ..., PRIMARY KEY ((pk [,pk2]), [ck [,ck2]]))
DROP TABLE [ks.]t
INSERT INTO [ks.]t (col,...) VALUES (val,...) [USING TTL n]
SELECT col,... FROM [ks.]t WHERE pk = v [AND ck op v] [ORDER BY ck [ASC|DESC]] [LIMIT n]
UPDATE [ks.]t SET col=val [,col=val] WHERE pk=v [AND ck=v]
DELETE [col,...] FROM [ks.]t WHERE pk=v [AND ck=v]
TRUNCATE [ks.]t
DESCRIBE KEYSPACES | KEYSPACE ks | TABLES | TABLE [ks.]t
"""
from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum, auto
from typing import Any, Dict, List, Optional, Tuple


# ═══════════════════════════════════════════════════════════════════════════
# TOKENS
# ═══════════════════════════════════════════════════════════════════════════

class TT(Enum):
    # Literals
    NUMBER = auto(); STRING = auto(); IDENT = auto(); BOOL = auto()
    # Punctuation
    LPAREN = auto(); RPAREN = auto(); LBRACE = auto(); RBRACE = auto()
    COMMA = auto(); SEMICOLON = auto(); DOT = auto(); COLON = auto()
    # Operators
    EQ = auto(); NEQ = auto(); LT = auto(); GT = auto(); LTE = auto(); GTE = auto()
    PLUS = auto(); MINUS = auto(); STAR = auto()
    # Keywords
    CREATE = auto(); DROP = auto(); ALTER = auto(); TRUNCATE = auto()
    KEYSPACE = auto(); KEYSPACES = auto(); TABLE = auto(); TABLES = auto()
    WITH = auto(); REPLICATION = auto(); USE = auto()
    INSERT = auto(); INTO = auto(); VALUES = auto(); USING = auto(); TTL = auto()
    SELECT = auto(); FROM = auto(); WHERE = auto()
    AND = auto(); OR = auto(); IN = auto(); NOT = auto()
    UPDATE = auto(); SET = auto()
    DELETE = auto()
    PRIMARY = auto(); KEY = auto(); CLUSTERING = auto(); ORDER = auto()
    BY = auto(); ASC = auto(); DESC = auto(); LIMIT = auto()
    ALLOW = auto(); FILTERING = auto()
    IF = auto(); EXISTS = auto()
    NULL = auto(); TRUE = auto(); FALSE = auto()
    DESCRIBE = auto(); DESCRIBE_SHORT = auto()
    COUNT = auto()
    # Types
    TEXT = auto(); INT = auto(); BIGINT = auto(); FLOAT = auto()
    BOOLEAN = auto(); TIMESTAMP = auto(); UUID_ = auto()
    EOF = auto()


_KW = {
    'create': TT.CREATE, 'drop': TT.DROP, 'alter': TT.ALTER, 'truncate': TT.TRUNCATE,
    'keyspace': TT.KEYSPACE, 'keyspaces': TT.KEYSPACES,
    'table': TT.TABLE, 'tables': TT.TABLES,
    'with': TT.WITH, 'replication': TT.REPLICATION, 'use': TT.USE,
    'insert': TT.INSERT, 'into': TT.INTO, 'values': TT.VALUES,
    'using': TT.USING, 'ttl': TT.TTL,
    'select': TT.SELECT, 'from': TT.FROM, 'where': TT.WHERE,
    'and': TT.AND, 'or': TT.OR, 'in': TT.IN, 'not': TT.NOT,
    'update': TT.UPDATE, 'set': TT.SET, 'delete': TT.DELETE,
    'primary': TT.PRIMARY, 'key': TT.KEY, 'clustering': TT.CLUSTERING,
    'order': TT.ORDER, 'by': TT.BY, 'asc': TT.ASC, 'desc': TT.DESC,
    'limit': TT.LIMIT, 'allow': TT.ALLOW, 'filtering': TT.FILTERING,
    'if': TT.IF, 'exists': TT.EXISTS,
    'null': TT.NULL, 'true': TT.TRUE, 'false': TT.FALSE,
    'describe': TT.DESCRIBE, 'desc': TT.DESC,
    'count': TT.COUNT,
    'text': TT.TEXT, 'varchar': TT.TEXT, 'ascii': TT.TEXT,
    'int': TT.INT, 'integer': TT.INT,
    'bigint': TT.BIGINT,
    'float': TT.FLOAT, 'double': TT.FLOAT, 'decimal': TT.FLOAT,
    'boolean': TT.BOOLEAN, 'bool': TT.BOOLEAN,
    'timestamp': TT.TIMESTAMP, 'datetime': TT.TIMESTAMP,
    'uuid': TT.UUID_,
}

_TYPE_KW = {TT.TEXT, TT.INT, TT.BIGINT, TT.FLOAT, TT.BOOLEAN, TT.TIMESTAMP, TT.UUID_}


@dataclass
class Token:
    tt:   TT
    val:  str
    line: int = 0


# ═══════════════════════════════════════════════════════════════════════════
# AST NODES
# ═══════════════════════════════════════════════════════════════════════════

@dataclass
class ColDef:
    name:     str
    col_type: str
    static:   bool = False


@dataclass
class WhereClause:
    pk_conditions:  Dict[str, Any]                       # pk_col → value
    ck_conditions:  List[Tuple[str, str, Any]]           # (col, op, value)


@dataclass
class CreateKeyspaceStmt:
    name:               str
    replication_class:  str
    replication_factor: int


@dataclass
class DropKeyspaceStmt:
    name:      str
    if_exists: bool = False


@dataclass
class UseStmt:
    keyspace: str


@dataclass
class CreateTableStmt:
    keyspace:        Optional[str]
    name:            str
    columns:         List[ColDef]
    partition_keys:  List[str]
    clustering_keys: List[str]


@dataclass
class DropTableStmt:
    keyspace:  Optional[str]
    name:      str
    if_exists: bool = False


@dataclass
class TruncateStmt:
    keyspace: Optional[str]
    name:     str


@dataclass
class InsertStmt:
    keyspace:  Optional[str]
    table:     str
    columns:   List[str]
    values:    List[Any]
    ttl:       Optional[int] = None


@dataclass
class SelectStmt:
    keyspace:   Optional[str]
    table:      str
    columns:    List[str]         # ['*'] or column names; 'COUNT(*)' → special
    where:      Optional[WhereClause] = None
    order_by:   Optional[Tuple[str, str]] = None    # (column, ASC|DESC)
    limit:      Optional[int] = None
    allow_filtering: bool = False
    count_only: bool = False


@dataclass
class UpdateStmt:
    keyspace:    Optional[str]
    table:       str
    assignments: List[Tuple[str, Any]]              # [(col, val), ...]
    where:       WhereClause


@dataclass
class DeleteStmt:
    keyspace:      Optional[str]
    table:         str
    delete_cols:   List[str]                        # [] = delete whole row
    where:         WhereClause


@dataclass
class DescribeStmt:
    target: str    # 'KEYSPACES' | 'KEYSPACE ks' | 'TABLES' | 'TABLE ks.t'
    name:   Optional[str] = None
    keyspace: Optional[str] = None


# ═══════════════════════════════════════════════════════════════════════════
# LEXER
# ═══════════════════════════════════════════════════════════════════════════

class Lexer:
    def __init__(self, src: str):
        self.tokens: List[Token] = []
        self._scan(src)

    def _scan(self, src: str):
        pos = 0; line = 1
        add = self.tokens.append

        while pos < len(src):
            c = src[pos]

            # whitespace
            if c in ' \t\r\n':
                if c == '\n': line += 1
                pos += 1; continue

            # single-line comment
            if c == '-' and pos + 1 < len(src) and src[pos+1] == '-':
                while pos < len(src) and src[pos] != '\n': pos += 1
                continue

            # string literal
            if c == "'":
                pos += 1; buf = []
                while pos < len(src):
                    ch = src[pos]; pos += 1
                    if ch == "'" :
                        if pos < len(src) and src[pos] == "'":
                            buf.append("'"); pos += 1
                        else:
                            break
                    else:
                        buf.append(ch)
                add(Token(TT.STRING, ''.join(buf), line)); continue

            # number
            if c.isdigit() or (c == '-' and pos+1 < len(src) and src[pos+1].isdigit()
                               and (not self.tokens or
                                    self.tokens[-1].tt in {TT.EQ, TT.NEQ, TT.LT, TT.GT,
                                                           TT.LTE, TT.GTE, TT.COLON,
                                                           TT.LPAREN, TT.COMMA})):
                start = pos
                if src[pos] == '-': pos += 1
                while pos < len(src) and (src[pos].isdigit() or src[pos] == '.'):
                    pos += 1
                add(Token(TT.NUMBER, src[start:pos], line)); continue

            # identifier / keyword
            if c.isalpha() or c == '_':
                start = pos
                while pos < len(src) and (src[pos].isalnum() or src[pos] == '_'):
                    pos += 1
                word = src[start:pos]
                tt   = _KW.get(word.lower(), TT.IDENT)
                # 'true'/'false' → BOOL literal
                if word.lower() in ('true', 'false'):
                    add(Token(TT.BOOL, word.lower(), line))
                else:
                    add(Token(tt, word, line))
                continue

            two = src[pos:pos+2]
            if two == '!=': add(Token(TT.NEQ,  '!=', line)); pos += 2; continue
            if two == '<>': add(Token(TT.NEQ,  '<>', line)); pos += 2; continue
            if two == '<=': add(Token(TT.LTE,  '<=', line)); pos += 2; continue
            if two == '>=': add(Token(TT.GTE,  '>=', line)); pos += 2; continue

            single = {
                '=': TT.EQ, '<': TT.LT, '>': TT.GT,
                '+': TT.PLUS, '-': TT.MINUS, '*': TT.STAR,
                '(': TT.LPAREN, ')': TT.RPAREN,
                '{': TT.LBRACE, '}': TT.RBRACE,
                ',': TT.COMMA, ';': TT.SEMICOLON,
                '.': TT.DOT, ':': TT.COLON,
            }
            if c in single:
                add(Token(single[c], c, line)); pos += 1; continue

            raise SyntaxError(f"Unexpected character {c!r} at line {line}")

        add(Token(TT.EOF, '', line))


# ═══════════════════════════════════════════════════════════════════════════
# PARSER
# ═══════════════════════════════════════════════════════════════════════════

class ParseError(Exception):
    pass


class Parser:
    def __init__(self, tokens: List[Token]):
        self._t   = tokens
        self._pos = 0

    def _cur(self)  -> Token: return self._t[self._pos]
    def _peek(self) -> Token: return self._t[min(self._pos+1, len(self._t)-1)]

    def _match(self, *types) -> bool:
        return self._cur().tt in types

    def _eat(self, *types) -> Token:
        t = self._cur()
        if types and t.tt not in types:
            exp = ', '.join(x.name for x in types)
            raise ParseError(
                f"Expected {exp} but got {t.tt.name}({t.val!r}) at line {t.line}")
        self._pos += 1
        return t

    def _try(self, *types) -> Optional[Token]:
        if self._cur().tt in types:
            return self._eat()
        return None

    def parse(self):
        stmt = self._stmt()
        self._try(TT.SEMICOLON)
        return stmt

    # ── dispatch ──────────────────────────────────────────────────────────────
    def _stmt(self):
        t = self._cur().tt
        if t == TT.CREATE:   return self._create()
        if t == TT.DROP:     return self._drop()
        if t == TT.USE:      return self._use()
        if t == TT.TRUNCATE: return self._truncate()
        if t == TT.INSERT:   return self._insert()
        if t == TT.SELECT:   return self._select()
        if t == TT.UPDATE:   return self._update()
        if t == TT.DELETE:   return self._delete()
        if t == TT.DESCRIBE: return self._describe()
        if t == TT.IDENT and self._cur().val.upper() in ('DESC', 'DESCRIBE'):
            return self._describe()
        raise ParseError(f"Unknown statement starting with {self._cur().val!r}")

    # ── DDL ───────────────────────────────────────────────────────────────────
    def _create(self):
        self._eat(TT.CREATE)
        if self._match(TT.KEYSPACE):
            self._eat()
            name = self._eat(TT.IDENT).val
            self._eat(TT.WITH)
            # replication = { ... }
            self._eat(TT.REPLICATION)
            self._eat(TT.EQ)
            opts = self._map_literal()
            rc = opts.get('class', 'SimpleStrategy')
            rf = int(opts.get('replication_factor', 1))
            return CreateKeyspaceStmt(name, rc, rf)

        if self._match(TT.TABLE):
            self._eat()
            ks, name = self._table_name()
            self._eat(TT.LPAREN)
            cols, pks, cks = self._column_defs()
            self._eat(TT.RPAREN)
            return CreateTableStmt(ks, name, cols, pks, cks)

        raise ParseError("Expected KEYSPACE or TABLE after CREATE")

    def _column_defs(self) -> Tuple[List[ColDef], List[str], List[str]]:
        cols: List[ColDef] = []
        pks:  List[str]    = []
        cks:  List[str]    = []

        while True:
            if self._match(TT.PRIMARY):
                self._eat(); self._eat(TT.KEY)
                self._eat(TT.LPAREN)
                # partition key: either (pk1, pk2) or just pk
                if self._match(TT.LPAREN):
                    self._eat()
                    pks.append(self._eat(TT.IDENT).val)
                    while self._try(TT.COMMA):
                        pks.append(self._eat(TT.IDENT).val)
                    self._eat(TT.RPAREN)
                else:
                    pks.append(self._eat(TT.IDENT).val)
                # clustering keys
                while self._try(TT.COMMA):
                    cks.append(self._eat(TT.IDENT).val)
                self._eat(TT.RPAREN)
            else:
                cname = self._eat(TT.IDENT).val
                ctype_tok = self._eat(*_TYPE_KW)
                ctype = _TYPE_KW_TO_STR[ctype_tok.tt]
                static = bool(self._try(TT.IDENT))   # 'STATIC' keyword
                cols.append(ColDef(cname, ctype, static))

            if not self._try(TT.COMMA):
                break

        return cols, pks, cks

    def _drop(self):
        self._eat(TT.DROP)
        if_exists = False
        if self._match(TT.KEYSPACE):
            self._eat()
            name = self._eat(TT.IDENT).val
            return DropKeyspaceStmt(name, if_exists)
        if self._match(TT.TABLE):
            self._eat()
            ks, name = self._table_name()
            return DropTableStmt(ks, name, if_exists)
        raise ParseError("Expected KEYSPACE or TABLE after DROP")

    def _use(self):
        self._eat(TT.USE)
        name = self._eat(TT.IDENT).val
        return UseStmt(name)

    def _truncate(self):
        self._eat(TT.TRUNCATE)
        self._try(TT.TABLE)
        ks, name = self._table_name()
        return TruncateStmt(ks, name)

    # ── DML ───────────────────────────────────────────────────────────────────
    def _insert(self):
        self._eat(TT.INSERT); self._eat(TT.INTO)
        ks, name = self._table_name()
        self._eat(TT.LPAREN)
        cols = [self._eat(TT.IDENT).val]
        while self._try(TT.COMMA):
            cols.append(self._eat(TT.IDENT).val)
        self._eat(TT.RPAREN)
        self._eat(TT.VALUES)
        self._eat(TT.LPAREN)
        vals = [self._literal()]
        while self._try(TT.COMMA):
            vals.append(self._literal())
        self._eat(TT.RPAREN)
        ttl = None
        if self._try(TT.USING):
            self._eat(TT.TTL)
            ttl = int(self._eat(TT.NUMBER).val)
        return InsertStmt(ks, name, cols, vals, ttl)

    def _select(self):
        self._eat(TT.SELECT)
        count_only = False

        # column list
        if self._match(TT.STAR):
            self._eat(); cols = ['*']
        elif self._match(TT.COUNT):
            self._eat()
            self._eat(TT.LPAREN); self._eat(TT.STAR); self._eat(TT.RPAREN)
            cols = ['COUNT(*)']
            count_only = True
        else:
            cols = [self._eat(TT.IDENT).val]
            while self._try(TT.COMMA):
                cols.append(self._eat(TT.IDENT).val)

        self._eat(TT.FROM)
        ks, name = self._table_name()

        where = None
        if self._try(TT.WHERE):
            where = self._where_clause()

        order_by = None
        if self._try(TT.ORDER):
            self._eat(TT.BY)
            col = self._eat(TT.IDENT).val
            dir_ = 'DESC' if self._try(TT.DESC) else 'ASC'
            self._try(TT.ASC)
            order_by = (col, dir_)

        limit = None
        if self._try(TT.LIMIT):
            limit = int(self._eat(TT.NUMBER).val)

        af = bool(self._try(TT.ALLOW))
        if af: self._try(TT.FILTERING)

        return SelectStmt(ks, name, cols, where, order_by, limit, af, count_only)

    def _update(self):
        self._eat(TT.UPDATE)
        ks, name = self._table_name()
        self._eat(TT.SET)
        assigns = []
        col = self._eat(TT.IDENT).val
        self._eat(TT.EQ)
        assigns.append((col, self._literal()))
        while self._try(TT.COMMA):
            col = self._eat(TT.IDENT).val
            self._eat(TT.EQ)
            assigns.append((col, self._literal()))
        self._eat(TT.WHERE)
        where = self._where_clause()
        return UpdateStmt(ks, name, assigns, where)

    def _delete(self):
        self._eat(TT.DELETE)
        delete_cols = []
        while self._match(TT.IDENT):
            delete_cols.append(self._eat().val)
            if not self._try(TT.COMMA):
                break
        self._eat(TT.FROM)
        ks, name = self._table_name()
        self._eat(TT.WHERE)
        where = self._where_clause()
        return DeleteStmt(ks, name, delete_cols, where)

    def _describe(self):
        self._eat()  # consume DESCRIBE / DESC token
        if self._match(TT.KEYSPACES):
            self._eat(); return DescribeStmt('KEYSPACES')
        if self._match(TT.KEYSPACE):
            self._eat()
            name = self._eat(TT.IDENT).val
            return DescribeStmt('KEYSPACE', name)
        if self._match(TT.TABLES):
            self._eat(); return DescribeStmt('TABLES')
        if self._match(TT.TABLE):
            self._eat()
            ks, name = self._table_name()
            return DescribeStmt('TABLE', name, ks)
        # bare DESCRIBE → list tables
        return DescribeStmt('TABLES')

    # ── WHERE ─────────────────────────────────────────────────────────────────
    def _where_clause(self) -> WhereClause:
        pk_conds: Dict[str, Any]             = {}
        ck_conds: List[Tuple[str, str, Any]] = []
        self._condition(pk_conds, ck_conds)
        while self._try(TT.AND):
            self._condition(pk_conds, ck_conds)
        return WhereClause(pk_conds, ck_conds)

    def _condition(self, pk_conds, ck_conds):
        col = self._eat(TT.IDENT).val
        op_tok = self._eat(TT.EQ, TT.NEQ, TT.LT, TT.GT, TT.LTE, TT.GTE)
        op  = op_tok.val
        val = self._literal()
        # Partition key conditions always use '='
        if op == '=':
            pk_conds[col] = val
        else:
            ck_conds.append((col, op, val))

    # ── helpers ───────────────────────────────────────────────────────────────
    def _table_name(self) -> Tuple[Optional[str], str]:
        name = self._eat(TT.IDENT).val
        if self._try(TT.DOT):
            return name, self._eat(TT.IDENT).val
        return None, name

    def _literal(self) -> Any:
        t = self._cur()
        if t.tt == TT.NUMBER:
            self._eat()
            v = t.val
            return float(v) if '.' in v else int(v)
        if t.tt == TT.STRING:
            self._eat(); return t.val
        if t.tt == TT.BOOL:
            self._eat(); return t.val == 'true'
        if t.tt == TT.NULL:
            self._eat(); return None
        if t.tt == TT.IDENT:
            self._eat(); return t.val
        raise ParseError(f"Expected literal, got {t.tt.name}({t.val!r})")

    def _map_literal(self) -> Dict[str, str]:
        """Parse {'key': 'val', ...}"""
        self._eat(TT.LBRACE)
        d: Dict[str, str] = {}
        while not self._match(TT.RBRACE):
            k = self._eat(TT.STRING).val
            self._eat(TT.COLON)
            v = self._eat(TT.STRING).val
            d[k] = v
            self._try(TT.COMMA)
        self._eat(TT.RBRACE)
        return d


_TYPE_KW_TO_STR = {
    TT.TEXT: 'TEXT', TT.INT: 'INT', TT.BIGINT: 'BIGINT',
    TT.FLOAT: 'FLOAT', TT.BOOLEAN: 'BOOLEAN',
    TT.TIMESTAMP: 'TIMESTAMP', TT.UUID_: 'UUID',
}


# ── Public API ────────────────────────────────────────────────────────────────

def parse(cql: str):
    tokens = Lexer(cql).tokens
    return Parser(tokens).parse()
