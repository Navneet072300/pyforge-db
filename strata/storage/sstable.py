"""
SSTable (Sorted String Table) — immutable, on-disk sorted file.

File layout
-----------
[Data section]
  For each cell (sorted by (pk, ck)):
    pk_len(2) pk(pk_len) ck_len(2) ck(ck_len) ts(8) tombstone(1)
    col_len(4) col_data(col_len)   ← JSON-encoded column dict

[Index section]  — sparse, one entry per INDEX_STRIDE cells
  num_entries(4)
  For each entry:
    pk_len(2) pk(pk_len) ck_len(2) ck(ck_len) offset(8)

[Bloom section]
  bloom_len(4) bloom_data(bloom_len)

[Footer — always last 24 bytes]
  index_offset(8) bloom_offset(8) num_cells(8)
"""
import json
import os
import struct
from bisect import bisect_left
from typing import Any, Dict, Iterator, List, Optional, Tuple

from .bloom import BloomFilter
from .memtable import Cell

INDEX_STRIDE = 128

_CELL_HDR = '>HH'          # pk_len, ck_len
_TS_TOMB  = '>qB'          # timestamp (int64), tombstone (uint8)
_FOOTER   = '>QQQ'         # index_offset, bloom_offset, num_cells

_CELL_HDR_SZ = struct.calcsize(_CELL_HDR)
_TS_TOMB_SZ  = struct.calcsize(_TS_TOMB)
_FOOTER_SZ   = struct.calcsize(_FOOTER)

_Key = Tuple[str, str]


def _encode_str(s: str) -> bytes:
    return s.encode('utf-8')


def _decode_str(b: bytes) -> str:
    return b.decode('utf-8')


# ── Writer ───────────────────────────────────────────────────────────────────

class SSTableWriter:
    def __init__(self, path: str):
        self.path   = path
        self._f     = open(path, 'wb')
        self._bloom = BloomFilter()
        self._index: List[Tuple[str, str, int]] = []  # (pk, ck, offset)
        self._count = 0

    def write_all(self, items: Iterator[Tuple[_Key, Cell]]):
        for key, cell in items:
            self._write_cell(key, cell)
        self._finalise()

    def _write_cell(self, key: _Key, cell: Cell):
        pk_b = _encode_str(key[0])
        ck_b = _encode_str(key[1])
        bloom_key = pk_b + b'\x00' + ck_b
        self._bloom.add(bloom_key)

        if self._count % INDEX_STRIDE == 0:
            self._index.append((key[0], key[1], self._f.tell()))

        pk_len = len(pk_b); ck_len = len(ck_b)
        cols_b = json.dumps(cell.columns, default=str).encode()
        self._f.write(struct.pack(_CELL_HDR, pk_len, ck_len))
        self._f.write(pk_b + ck_b)
        self._f.write(struct.pack(_TS_TOMB, cell.timestamp, 1 if cell.tombstone else 0))
        self._f.write(struct.pack('>I', len(cols_b)) + cols_b)
        self._count += 1

    def _finalise(self):
        # Write index
        index_offset = self._f.tell()
        self._f.write(struct.pack('>I', len(self._index)))
        for pk, ck, off in self._index:
            pk_b = _encode_str(pk); ck_b = _encode_str(ck)
            self._f.write(struct.pack(_CELL_HDR, len(pk_b), len(ck_b)))
            self._f.write(pk_b + ck_b)
            self._f.write(struct.pack('>Q', off))

        # Write bloom
        bloom_offset = self._f.tell()
        bloom_bytes  = self._bloom.to_bytes()
        self._f.write(struct.pack('>I', len(bloom_bytes)) + bloom_bytes)

        # Write footer
        self._f.write(struct.pack(_FOOTER, index_offset, bloom_offset, self._count))
        self._f.close()


# ── Reader ───────────────────────────────────────────────────────────────────

class SSTableReader:
    def __init__(self, path: str):
        self.path   = path
        self._size  = os.path.getsize(path)
        self._f     = open(path, 'rb')
        self._load_footer()
        self._load_index()
        self._load_bloom()

    def _load_footer(self):
        self._f.seek(self._size - _FOOTER_SZ)
        raw = self._f.read(_FOOTER_SZ)
        self.index_offset, self.bloom_offset, self.num_cells = struct.unpack(_FOOTER, raw)

    def _load_index(self):
        self._f.seek(self.index_offset)
        n = struct.unpack('>I', self._f.read(4))[0]
        self._index: List[Tuple[str, str, int]] = []
        for _ in range(n):
            pk_len, ck_len = struct.unpack(_CELL_HDR, self._f.read(_CELL_HDR_SZ))
            pk = _decode_str(self._f.read(pk_len))
            ck = _decode_str(self._f.read(ck_len))
            off = struct.unpack('>Q', self._f.read(8))[0]
            self._index.append((pk, ck, off))

    def _load_bloom(self):
        self._f.seek(self.bloom_offset)
        blen  = struct.unpack('>I', self._f.read(4))[0]
        bdata = self._f.read(blen)
        self._bloom = BloomFilter.from_bytes(bdata)

    # ── public ────────────────────────────────────────────────────────────────
    def might_contain(self, pk: str, ck: str) -> bool:
        return self._bloom.might_contain(_encode_str(pk) + b'\x00' + _encode_str(ck))

    def get(self, pk: str, ck: str) -> Optional[Cell]:
        if not self.might_contain(pk, ck):
            return None
        for k, cell in self._scan_from(pk, ck):
            if k == (pk, ck):
                return cell
            if k > (pk, ck):
                break
        return None

    def scan_partition(self, pk: str,
                       ck_lo: Optional[str] = None,
                       ck_hi: Optional[str] = None,
                       ck_lo_exclusive: bool = False,
                       ck_hi_exclusive: bool = False,
                       reversed_: bool = False) -> List[Tuple[_Key, Cell]]:
        results = []
        for k, cell in self._scan_from(pk, ck_lo or ''):
            if k[0] < pk:
                continue   # sparse index may start before target partition
            if k[0] != pk:
                break
            if ck_lo is not None:
                if ck_lo_exclusive and k[1] <= ck_lo: continue
                if not ck_lo_exclusive and k[1] < ck_lo: continue
            if ck_hi is not None:
                if ck_hi_exclusive and k[1] >= ck_hi: break
                if not ck_hi_exclusive and k[1] > ck_hi: break
            results.append((k, cell))
        if reversed_:
            results.reverse()
        return results

    def iter_all(self) -> Iterator[Tuple[_Key, Cell]]:
        yield from self._scan_from_offset(0)

    def close(self):
        self._f.close()

    # ── internals ─────────────────────────────────────────────────────────────
    def _scan_from(self, pk: str, ck: str) -> Iterator[Tuple[_Key, Cell]]:
        target = (pk, ck)
        # Binary search the sparse index for the largest entry ≤ target
        lo, hi = 0, len(self._index) - 1
        offset = 0
        while lo <= hi:
            mid = (lo + hi) // 2
            idx_key = (self._index[mid][0], self._index[mid][1])
            if idx_key <= target:
                offset = self._index[mid][2]
                lo = mid + 1
            else:
                hi = mid - 1
        yield from self._scan_from_offset(offset)

    def _scan_from_offset(self, offset: int) -> Iterator[Tuple[_Key, Cell]]:
        self._f.seek(offset)
        end = self.index_offset
        while self._f.tell() < end:
            pos = self._f.tell()
            hdr = self._f.read(_CELL_HDR_SZ)
            if len(hdr) < _CELL_HDR_SZ:
                break
            pk_len, ck_len = struct.unpack(_CELL_HDR, hdr)
            pk   = _decode_str(self._f.read(pk_len))
            ck   = _decode_str(self._f.read(ck_len))
            ts, tomb = struct.unpack(_TS_TOMB, self._f.read(_TS_TOMB_SZ))
            clen = struct.unpack('>I', self._f.read(4))[0]
            cols = json.loads(self._f.read(clen))
            yield (pk, ck), Cell(cols, ts, bool(tomb))
