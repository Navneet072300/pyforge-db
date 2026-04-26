"""
MemTable — the in-memory, mutable tier of the LSM tree.

Keys are (partition_key, clustering_key) tuples sorted for efficient
range scans within a partition.  When the entry count exceeds the limit
the MemTable is sealed (made read-only) and flushed to an SSTable.
"""
import time
from bisect import bisect_left, insort_left
from dataclasses import dataclass, field
from typing import Any, Dict, Iterator, List, Optional, Tuple


@dataclass
class Cell:
    columns:   Dict[str, Any]
    timestamp: int            # microseconds since epoch
    tombstone: bool = False


# Internal key: (partition_key_str, clustering_key_str)
_Key = Tuple[str, str]


class MemTable:
    def __init__(self, size_limit: int = 2000):
        """size_limit: max number of unique cells before the table is full."""
        self.size_limit = size_limit
        self._data: Dict[_Key, Cell] = {}
        self._keys: List[_Key]       = []   # sorted
        self._sealed = False

    # ── writes ────────────────────────────────────────────────────────────────
    def put(self, pk: str, ck: str, columns: dict, timestamp: int):
        self._check_writable()
        key = (pk, ck)
        if key not in self._data:
            insort_left(self._keys, key)
        self._data[key] = Cell(dict(columns), timestamp, False)

    def delete(self, pk: str, ck: str, timestamp: int):
        self._check_writable()
        key = (pk, ck)
        if key not in self._data:
            insort_left(self._keys, key)
        self._data[key] = Cell({}, timestamp, True)

    def _check_writable(self):
        if self._sealed:
            raise RuntimeError("MemTable is sealed")

    # ── reads ─────────────────────────────────────────────────────────────────
    def get(self, pk: str, ck: str) -> Optional[Cell]:
        return self._data.get((pk, ck))

    def scan(self, pk: str,
             ck_lo: Optional[str] = None, ck_hi: Optional[str] = None,
             ck_lo_exclusive: bool = False,
             ck_hi_exclusive: bool = False,
             reversed_: bool = False) -> Iterator[Tuple[_Key, Cell]]:
        """Yield (key, cell) pairs for the given partition, optionally range-limited."""
        lo = (pk, ck_lo or '')
        hi = (pk, ck_hi) if ck_hi is not None else (pk + '￿', '')

        start = bisect_left(self._keys, lo)
        keys_in_range = []
        for k in self._keys[start:]:
            if k[0] != pk:
                break
            if ck_lo is not None:
                if ck_lo_exclusive and k[1] <= ck_lo:
                    continue
                if not ck_lo_exclusive and k[1] < ck_lo:
                    continue
            if ck_hi is not None:
                if ck_hi_exclusive and k[1] >= ck_hi:
                    break
                if not ck_hi_exclusive and k[1] > ck_hi:
                    break
            keys_in_range.append(k)

        it = reversed(keys_in_range) if reversed_ else iter(keys_in_range)
        for k in it:
            yield k, self._data[k]

    def iter_all(self) -> Iterator[Tuple[_Key, Cell]]:
        for k in self._keys:
            yield k, self._data[k]

    # ── lifecycle ─────────────────────────────────────────────────────────────
    def is_full(self) -> bool:
        return len(self._data) >= self.size_limit

    def seal(self):
        self._sealed = True

    @property
    def sealed(self) -> bool:
        return self._sealed

    def __len__(self) -> int:
        return len(self._data)
