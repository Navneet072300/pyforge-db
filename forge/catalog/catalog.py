"""
Catalog: stores table schemas, column definitions, and index metadata.
Persisted as a single JSON file (catalog.json) in the data directory.
"""
import json
import os
import struct
from dataclasses import dataclass, field, asdict
from datetime import datetime
from typing import Any, Dict, List, Optional

# ── type system ──────────────────────────────────────────────────────────────

VALID_TYPES = {'INTEGER', 'FLOAT', 'BOOLEAN', 'TEXT', 'TIMESTAMP'}

# Struct format codes for fixed-size types
_TYPE_FMT: Dict[str, str] = {
    'INTEGER':   'q',   # int64
    'FLOAT':     'd',   # float64
    'BOOLEAN':   'B',   # uint8
    'TIMESTAMP': 'q',   # microseconds since epoch
}


@dataclass
class ColumnDef:
    name:        str
    col_type:    str           # one of VALID_TYPES
    nullable:    bool  = True
    primary_key: bool  = False

    def fixed_size(self) -> Optional[int]:
        fmt = _TYPE_FMT.get(self.col_type)
        return struct.calcsize('<' + fmt) if fmt else None


@dataclass
class IndexDef:
    index_name: str
    table_name: str
    columns:    List[str]
    unique:     bool = False


@dataclass
class TableSchema:
    name:    str
    columns: List[ColumnDef] = field(default_factory=list)
    indices: List[IndexDef]  = field(default_factory=list)

    # ── column helpers ────────────────────────────────────────────────────────
    def col_names(self) -> List[str]:
        return [c.name for c in self.columns]

    def col_index(self, name: str) -> int:
        for i, c in enumerate(self.columns):
            if c.name == name:
                return i
        raise KeyError(f"Column '{name}' not in table '{self.name}'")

    def get_col(self, name: str) -> ColumnDef:
        return self.columns[self.col_index(name)]

    def primary_key_cols(self) -> List[str]:
        return [c.name for c in self.columns if c.primary_key]

    def index_for_col(self, col: str) -> Optional[IndexDef]:
        for idx in self.indices:
            if col in idx.columns:
                return idx
        return None


# ── row codec ────────────────────────────────────────────────────────────────

ROW_MVCC_FMT = '<II'
ROW_MVCC_SZ  = struct.calcsize(ROW_MVCC_FMT)


def encode_row(row: Dict[str, Any], schema: TableSchema,
               xmin: int = 0, xmax: int = 0) -> bytes:
    """Serialize a row dict → bytes (with MVCC header)."""
    cols = schema.columns
    n    = len(cols)

    # null bitmap
    null_bm = 0
    for i, col in enumerate(cols):
        if row.get(col.name) is None:
            null_bm |= (1 << i)
    bm_bytes = null_bm.to_bytes((n + 7) // 8, 'little')

    parts = [struct.pack(ROW_MVCC_FMT, xmin, xmax), bm_bytes]
    for i, col in enumerate(cols):
        val = row.get(col.name)
        if val is None:
            continue
        parts.append(_encode_val(val, col.col_type))

    return b''.join(parts)


def decode_row(raw: bytes, schema: TableSchema) -> Dict[str, Any]:
    """Deserialize bytes → row dict including __xmin__ and __xmax__."""
    xmin, xmax = struct.unpack_from(ROW_MVCC_FMT, raw, 0)
    offset     = ROW_MVCC_SZ

    cols = schema.columns
    n    = len(cols)
    bm_sz = (n + 7) // 8
    null_bm = int.from_bytes(raw[offset:offset + bm_sz], 'little')
    offset += bm_sz

    row: Dict[str, Any] = {'__xmin__': xmin, '__xmax__': xmax}
    for i, col in enumerate(cols):
        if null_bm & (1 << i):
            row[col.name] = None
        else:
            val, size  = _decode_val(raw, offset, col.col_type)
            row[col.name] = val
            offset += size
    return row


def set_xmax(raw: bytes, xmax: int) -> bytes:
    """Return a copy of the row bytes with xmax overwritten."""
    buf  = bytearray(raw)
    xmin = struct.unpack_from('<I', buf, 0)[0]
    struct.pack_into(ROW_MVCC_FMT, buf, 0, xmin, xmax)
    return bytes(buf)


def _encode_val(val: Any, col_type: str) -> bytes:
    if col_type == 'INTEGER':
        return struct.pack('<q', int(val))
    if col_type == 'FLOAT':
        return struct.pack('<d', float(val))
    if col_type == 'BOOLEAN':
        return struct.pack('<B', 1 if val else 0)
    if col_type == 'TIMESTAMP':
        if isinstance(val, datetime):
            ts = int(val.timestamp() * 1_000_000)
        else:
            ts = int(val)
        return struct.pack('<q', ts)
    if col_type == 'TEXT':
        data = str(val).encode('utf-8')
        return struct.pack('<I', len(data)) + data
    raise ValueError(f"Unknown type: {col_type}")


def _decode_val(raw: bytes, offset: int, col_type: str):
    if col_type == 'INTEGER':
        return struct.unpack_from('<q', raw, offset)[0], 8
    if col_type == 'FLOAT':
        return struct.unpack_from('<d', raw, offset)[0], 8
    if col_type == 'BOOLEAN':
        return bool(struct.unpack_from('<B', raw, offset)[0]), 1
    if col_type == 'TIMESTAMP':
        ts = struct.unpack_from('<q', raw, offset)[0]
        return datetime.fromtimestamp(ts / 1_000_000), 8
    if col_type == 'TEXT':
        length = struct.unpack_from('<I', raw, offset)[0]
        text   = raw[offset + 4 : offset + 4 + length].decode('utf-8')
        return text, 4 + length
    raise ValueError(f"Unknown type: {col_type}")


# ── catalog ──────────────────────────────────────────────────────────────────

class Catalog:
    def __init__(self, data_dir: str):
        self.data_dir = data_dir
        os.makedirs(data_dir, exist_ok=True)
        self._path   = os.path.join(data_dir, 'catalog.json')
        self._tables: Dict[str, TableSchema] = {}
        self._load()

    # ── persistence ───────────────────────────────────────────────────────────
    def _load(self):
        if not os.path.exists(self._path):
            return
        with open(self._path) as f:
            raw = json.load(f)
        for tname, tdata in raw.items():
            cols = [ColumnDef(**c) for c in tdata['columns']]
            idxs = [IndexDef(**ix)  for ix in tdata.get('indices', [])]
            self._tables[tname] = TableSchema(tname, cols, idxs)

    def _save(self):
        data = {}
        for tname, schema in self._tables.items():
            data[tname] = {
                'columns': [asdict(c) for c in schema.columns],
                'indices': [asdict(ix) for ix in schema.indices],
            }
        with open(self._path, 'w') as f:
            json.dump(data, f, indent=2)

    # ── table management ──────────────────────────────────────────────────────
    def create_table(self, schema: TableSchema):
        if schema.name in self._tables:
            raise RuntimeError(f"Table '{schema.name}' already exists")
        self._tables[schema.name] = schema
        self._save()

    def drop_table(self, name: str):
        if name not in self._tables:
            raise RuntimeError(f"Table '{name}' does not exist")
        del self._tables[name]
        self._save()

    def get_table(self, name: str) -> TableSchema:
        if name not in self._tables:
            raise RuntimeError(f"Table '{name}' does not exist")
        return self._tables[name]

    def table_exists(self, name: str) -> bool:
        return name in self._tables

    def all_tables(self) -> List[str]:
        return list(self._tables.keys())

    # ── index management ──────────────────────────────────────────────────────
    def add_index(self, idx: IndexDef):
        schema = self.get_table(idx.table_name)
        schema.indices.append(idx)
        self._save()

    def drop_index(self, table_name: str, index_name: str):
        schema = self.get_table(table_name)
        schema.indices = [ix for ix in schema.indices if ix.index_name != index_name]
        self._save()

    # ── path helpers ──────────────────────────────────────────────────────────
    def heap_path(self, table_name: str) -> str:
        return os.path.join(self.data_dir, f'{table_name}.db')

    def index_path(self, table_name: str, index_name: str) -> str:
        return os.path.join(self.data_dir, f'{table_name}_{index_name}.idx')
