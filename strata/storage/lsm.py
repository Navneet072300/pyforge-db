"""
LSM Tree — the core storage engine for one logical table on one node.

Layers (checked newest→oldest on every read):
  1. Active MemTable  (mutable)
  2. Sealed MemTables (awaiting flush)
  3. L0 SSTables      (uncompacted, may overlap)
  4. L1 SSTables      (size-tiered compact)
  5. L2 SSTables      (leveled compact)

Compaction triggers:
  • L0 → L1: when ≥ L0_THRESHOLD SSTables exist in L0
  • L1 → L2: when L1 total cells > L1_CELL_LIMIT
"""
import os
import time
from typing import Any, Dict, Iterator, List, Optional, Tuple

from .memtable import Cell, MemTable
from .sstable  import SSTableReader, SSTableWriter

_Key = Tuple[str, str]

L0_THRESHOLD  = 4
L1_CELL_LIMIT = 50_000
MEMTABLE_LIMIT = 2_000


class LSMTree:
    def __init__(self, data_dir: str):
        self.data_dir = data_dir
        os.makedirs(data_dir, exist_ok=True)

        self._memtable: MemTable                 = MemTable(MEMTABLE_LIMIT)
        self._sealed:   List[MemTable]            = []
        self._levels:   List[List[SSTableReader]] = [[], [], []]   # L0, L1, L2
        self._sst_counter = 0

        self._load_existing_sstables()

    # ═══════════════════════════════════════════════════════════════════════
    # Writes
    # ═══════════════════════════════════════════════════════════════════════

    def put(self, pk: str, ck: str, columns: dict, timestamp: int):
        self._memtable.put(pk, ck, columns, timestamp)
        self._maybe_flush()

    def delete(self, pk: str, ck: str, timestamp: int):
        self._memtable.delete(pk, ck, timestamp)
        self._maybe_flush()

    # ═══════════════════════════════════════════════════════════════════════
    # Reads
    # ═══════════════════════════════════════════════════════════════════════

    def get(self, pk: str, ck: str) -> Optional[Cell]:
        """Return the most recent Cell for (pk, ck) or None if not found."""
        # MemTable first
        cell = self._memtable.get(pk, ck)
        if cell is not None:
            return None if cell.tombstone else cell

        # Sealed MemTables (newest first)
        for mt in reversed(self._sealed):
            cell = mt.get(pk, ck)
            if cell is not None:
                return None if cell.tombstone else cell

        # SSTables newest → oldest across all levels
        for level in self._levels:
            for sst in reversed(level):
                cell = sst.get(pk, ck)
                if cell is not None:
                    return None if cell.tombstone else cell

        return None

    def scan(self, pk: str,
             ck_lo: Optional[str] = None, ck_hi: Optional[str] = None,
             ck_lo_exclusive: bool = False,
             ck_hi_exclusive: bool = False,
             reversed_: bool = False,
             limit: Optional[int] = None) -> List[Tuple[_Key, Cell]]:
        """Return visible cells for pk in ck range, newest version wins."""
        latest: Dict[_Key, Cell] = {}

        # MemTable
        for k, c in self._memtable.scan(pk, ck_lo, ck_hi,
                                         ck_lo_exclusive, ck_hi_exclusive):
            self._merge_cell(latest, k, c)

        # Sealed MemTables
        for mt in self._sealed:
            for k, c in mt.scan(pk, ck_lo, ck_hi, ck_lo_exclusive, ck_hi_exclusive):
                self._merge_cell(latest, k, c)

        # SSTables
        for level in self._levels:
            for sst in level:
                for k, c in sst.scan_partition(pk, ck_lo, ck_hi,
                                               ck_lo_exclusive, ck_hi_exclusive):
                    self._merge_cell(latest, k, c)

        # Filter tombstones & sort
        alive = [(k, c) for k, c in latest.items() if not c.tombstone]
        alive.sort(key=lambda x: x[0][1], reverse=reversed_)

        if limit is not None:
            alive = alive[:limit]
        return alive

    # ═══════════════════════════════════════════════════════════════════════
    # Flush & Compaction
    # ═══════════════════════════════════════════════════════════════════════

    def _maybe_flush(self):
        if self._memtable.is_full():
            self._flush_memtable()
            if len(self._levels[0]) >= L0_THRESHOLD:
                self._compact_l0_to_l1()
            self._check_l1_l2()

    def _flush_memtable(self):
        mt = self._memtable
        mt.seal()
        self._sealed.append(mt)
        self._memtable = MemTable(MEMTABLE_LIMIT)

        path  = self._sst_path(0, self._sst_counter)
        self._sst_counter += 1
        writer = SSTableWriter(path)
        writer.write_all(mt.iter_all())

        reader = SSTableReader(path)
        self._levels[0].append(reader)
        self._sealed.remove(mt)

    def flush_all(self):
        """Explicitly flush the active MemTable regardless of size."""
        if len(self._memtable) > 0:
            self._flush_memtable()

    def _compact_l0_to_l1(self):
        """Merge all L0 SSTables into a single sorted L1 SSTable."""
        merged = self._merge_sst_readers(self._levels[0])
        path   = self._sst_path(1, self._sst_counter)
        self._sst_counter += 1

        writer = SSTableWriter(path)
        writer.write_all(iter(merged.items()))

        for sst in self._levels[0]:
            sst.close()
            try: os.remove(sst.path)
            except OSError: pass
        self._levels[0] = []
        self._levels[1].append(SSTableReader(path))

    def _check_l1_l2(self):
        total = sum(r.num_cells for r in self._levels[1])
        if total > L1_CELL_LIMIT:
            merged = self._merge_sst_readers(self._levels[1])
            path   = self._sst_path(2, self._sst_counter)
            self._sst_counter += 1
            SSTableWriter(path).write_all(iter(merged.items()))
            for sst in self._levels[1]:
                sst.close()
                try: os.remove(sst.path)
                except OSError: pass
            self._levels[1] = []
            self._levels[2].append(SSTableReader(path))

    def _merge_sst_readers(self, readers: List[SSTableReader]) -> Dict[_Key, Cell]:
        """Merge multiple SSTables, newest timestamp wins per key."""
        merged: Dict[_Key, Cell] = {}
        for sst in readers:
            for k, c in sst.iter_all():
                self._merge_cell(merged, k, c)
        return dict(sorted(merged.items()))

    # ═══════════════════════════════════════════════════════════════════════
    # Helpers
    # ═══════════════════════════════════════════════════════════════════════

    @staticmethod
    def _merge_cell(store: Dict[_Key, Cell], key: _Key, cell: Cell):
        existing = store.get(key)
        if existing is None or cell.timestamp >= existing.timestamp:
            store[key] = cell

    def _sst_path(self, level: int, seq: int) -> str:
        return os.path.join(self.data_dir, f'L{level}_{seq:06d}.sst')

    def _load_existing_sstables(self):
        files = sorted(f for f in os.listdir(self.data_dir) if f.endswith('.sst'))
        for fname in files:
            level = int(fname[1])   # L0_, L1_, L2_
            path  = os.path.join(self.data_dir, fname)
            seq   = int(fname[3:9])
            self._sst_counter = max(self._sst_counter, seq + 1)
            try:
                self._levels[min(level, 2)].append(SSTableReader(path))
            except Exception:
                pass

    def cell_count(self) -> int:
        n = len(self._memtable)
        for mt in self._sealed:
            n += len(mt)
        for level in self._levels:
            for sst in level:
                n += sst.num_cells
        return n

    def close(self):
        for level in self._levels:
            for sst in level:
                sst.close()
