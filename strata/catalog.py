"""
Catalog: keyspaces, table schemas, column definitions.
Persisted as catalog.json in the engine's root data directory.
"""
import json
import os
from dataclasses import dataclass, field, asdict
from typing import Dict, List, Optional

VALID_TYPES = {'TEXT', 'INT', 'BIGINT', 'FLOAT', 'BOOLEAN', 'TIMESTAMP', 'UUID'}


@dataclass
class ColumnDef:
    name:     str
    col_type: str   # one of VALID_TYPES
    static:   bool  = False   # Cassandra static column


@dataclass
class TableSchema:
    keyspace:        str
    name:            str
    columns:         List[ColumnDef] = field(default_factory=list)
    partition_keys:  List[str]       = field(default_factory=list)
    clustering_keys: List[str]       = field(default_factory=list)

    # ── helpers ────────────────────────────────────────────────────────────
    def col_names(self) -> List[str]:
        return [c.name for c in self.columns]

    def get_col(self, name: str) -> Optional[ColumnDef]:
        for c in self.columns:
            if c.name == name:
                return c
        return None

    def is_partition_key(self, name: str) -> bool:
        return name in self.partition_keys

    def is_clustering_key(self, name: str) -> bool:
        return name in self.clustering_keys

    def is_primary_key(self, name: str) -> bool:
        return self.is_partition_key(name) or self.is_clustering_key(name)

    def value_columns(self) -> List[ColumnDef]:
        pk = set(self.partition_keys) | set(self.clustering_keys)
        return [c for c in self.columns if c.name not in pk]

    def full_name(self) -> str:
        return f"{self.keyspace}.{self.name}"


@dataclass
class KeyspaceDef:
    name:               str
    replication_class:  str  = 'SimpleStrategy'
    replication_factor: int  = 1
    tables:             Dict[str, TableSchema] = field(default_factory=dict)


class Catalog:
    def __init__(self, path: str):
        self.path = path
        self._keyspaces: Dict[str, KeyspaceDef] = {}
        self._load()

    # ── keyspace ──────────────────────────────────────────────────────────────
    def create_keyspace(self, name: str, replication_class: str = 'SimpleStrategy',
                        replication_factor: int = 1):
        if name in self._keyspaces:
            raise RuntimeError(f"Keyspace '{name}' already exists")
        self._keyspaces[name] = KeyspaceDef(name, replication_class, replication_factor)
        self._save()

    def drop_keyspace(self, name: str):
        if name not in self._keyspaces:
            raise RuntimeError(f"Keyspace '{name}' does not exist")
        del self._keyspaces[name]
        self._save()

    def get_keyspace(self, name: str) -> KeyspaceDef:
        if name not in self._keyspaces:
            raise RuntimeError(f"Keyspace '{name}' does not exist")
        return self._keyspaces[name]

    def keyspace_exists(self, name: str) -> bool:
        return name in self._keyspaces

    def all_keyspaces(self) -> List[str]:
        return list(self._keyspaces.keys())

    # ── table ─────────────────────────────────────────────────────────────────
    def create_table(self, schema: TableSchema):
        ks = self.get_keyspace(schema.keyspace)
        if schema.name in ks.tables:
            raise RuntimeError(f"Table '{schema.full_name()}' already exists")
        ks.tables[schema.name] = schema
        self._save()

    def drop_table(self, keyspace: str, table: str):
        ks = self.get_keyspace(keyspace)
        if table not in ks.tables:
            raise RuntimeError(f"Table '{keyspace}.{table}' does not exist")
        del ks.tables[table]
        self._save()

    def get_table(self, keyspace: str, table: str) -> TableSchema:
        ks = self.get_keyspace(keyspace)
        if table not in ks.tables:
            raise RuntimeError(f"Table '{keyspace}.{table}' does not exist")
        return ks.tables[table]

    def table_exists(self, keyspace: str, table: str) -> bool:
        return (keyspace in self._keyspaces and
                table in self._keyspaces[keyspace].tables)

    def all_tables(self, keyspace: str) -> List[str]:
        return list(self.get_keyspace(keyspace).tables.keys())

    # ── coerce value to column type ───────────────────────────────────────────
    @staticmethod
    def coerce(value, col_type: str):
        if value is None:
            return None
        if col_type in ('INT', 'BIGINT'):
            return int(value)
        if col_type == 'FLOAT':
            return float(value)
        if col_type == 'BOOLEAN':
            if isinstance(value, bool):
                return value
            return str(value).lower() in ('true', '1', 'yes')
        return str(value)

    # ── persistence ───────────────────────────────────────────────────────────
    def _load(self):
        if not os.path.exists(self.path):
            return
        with open(self.path) as f:
            raw = json.load(f)
        for ks_name, ks_data in raw.items():
            tables = {}
            for tbl_name, tbl_data in ks_data.get('tables', {}).items():
                cols = [ColumnDef(**c) for c in tbl_data['columns']]
                tables[tbl_name] = TableSchema(
                    ks_name, tbl_name, cols,
                    tbl_data['partition_keys'],
                    tbl_data['clustering_keys'],
                )
            self._keyspaces[ks_name] = KeyspaceDef(
                ks_name,
                ks_data.get('replication_class', 'SimpleStrategy'),
                ks_data.get('replication_factor', 1),
                tables,
            )

    def _save(self):
        data = {}
        for ks_name, ks in self._keyspaces.items():
            data[ks_name] = {
                'replication_class':  ks.replication_class,
                'replication_factor': ks.replication_factor,
                'tables': {
                    tbl_name: {
                        'columns':         [asdict(c) for c in tbl.columns],
                        'partition_keys':  tbl.partition_keys,
                        'clustering_keys': tbl.clustering_keys,
                    }
                    for tbl_name, tbl in ks.tables.items()
                }
            }
        with open(self.path, 'w') as f:
            json.dump(data, f, indent=2)
