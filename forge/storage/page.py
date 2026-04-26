"""
Page layout (8 KB):
  [Header 20 bytes][Slot Array →][Free Space][← Tuple Data]

Header: page_id(4) lsn(8) num_slots(2) free_start(2) free_end(2) flags(2)
Slot:   offset(2) length(2)   — offset is from page start; 0,0 = deleted slot
Tuple:  xmin(4) xmax(4) null_bitmap(variable) column_data...
"""
import struct
from typing import List, Optional, Tuple

PAGE_SIZE       = 8192
PAGE_HEADER_FMT = '<IQHHHH'   # page_id, lsn, num_slots, free_start, free_end, flags
PAGE_HEADER_SZ  = struct.calcsize(PAGE_HEADER_FMT)   # 20 bytes
SLOT_FMT        = '<HH'
SLOT_SZ         = struct.calcsize(SLOT_FMT)           # 4 bytes

ROW_MVCC_FMT = '<II'
ROW_MVCC_SZ  = struct.calcsize(ROW_MVCC_FMT)         # 8 bytes


class Page:
    __slots__ = ('page_id', 'data', 'dirty', 'lsn',
                 'num_slots', 'free_start', 'free_end', 'flags')

    def __init__(self, page_id: int, data: Optional[bytes] = None):
        self.page_id = page_id
        self.dirty   = False
        if data:
            self.data = bytearray(data)
            self._load_header()
        else:
            self.data       = bytearray(PAGE_SIZE)
            self.lsn        = 0
            self.num_slots  = 0
            self.free_start = PAGE_HEADER_SZ
            self.free_end   = PAGE_SIZE
            self.flags      = 0
            self._flush_header()

    # ── header ──────────────────────────────────────────────────────────────
    def _load_header(self):
        (pid, self.lsn, self.num_slots,
         self.free_start, self.free_end, self.flags) = struct.unpack_from(
            PAGE_HEADER_FMT, self.data, 0)

    def _flush_header(self):
        struct.pack_into(PAGE_HEADER_FMT, self.data, 0,
                         self.page_id, self.lsn, self.num_slots,
                         self.free_start, self.free_end, self.flags)

    # ── capacity ────────────────────────────────────────────────────────────
    def free_space(self) -> int:
        """Bytes available for a new (slot + tuple)."""
        return self.free_end - self.free_start - SLOT_SZ

    # ── slot helpers ─────────────────────────────────────────────────────────
    def _slot_pos(self, slot_id: int) -> int:
        return PAGE_HEADER_SZ + slot_id * SLOT_SZ

    def _read_slot(self, slot_id: int) -> Tuple[int, int]:
        return struct.unpack_from(SLOT_FMT, self.data, self._slot_pos(slot_id))

    def _write_slot(self, slot_id: int, offset: int, length: int):
        struct.pack_into(SLOT_FMT, self.data, self._slot_pos(slot_id), offset, length)

    # ── tuple operations ─────────────────────────────────────────────────────
    def insert_tuple(self, payload: bytes) -> Optional[int]:
        """Insert payload; return slot_id or None if page is full."""
        if self.free_space() < len(payload):
            return None
        self.free_end -= len(payload)
        self.data[self.free_end : self.free_end + len(payload)] = payload
        slot_id = self.num_slots
        self._write_slot(slot_id, self.free_end, len(payload))
        self.num_slots  += 1
        self.free_start += SLOT_SZ
        self._flush_header()
        self.dirty = True
        return slot_id

    def get_tuple(self, slot_id: int) -> Optional[bytes]:
        if slot_id >= self.num_slots:
            return None
        offset, length = self._read_slot(slot_id)
        if length == 0:
            return None                          # deleted
        return bytes(self.data[offset : offset + length])

    def update_tuple_inplace(self, slot_id: int, payload: bytes) -> bool:
        """Replace tuple if new payload fits in the existing slot space."""
        if slot_id >= self.num_slots:
            return False
        offset, length = self._read_slot(slot_id)
        if length == 0 or len(payload) > length:
            return False
        self.data[offset : offset + len(payload)] = payload
        self._write_slot(slot_id, offset, len(payload))
        self.dirty = True
        return True

    def delete_tuple(self, slot_id: int) -> bool:
        if slot_id >= self.num_slots:
            return False
        _, length = self._read_slot(slot_id)
        if length == 0:
            return False
        self._write_slot(slot_id, 0, 0)
        self._flush_header()
        self.dirty = True
        return True

    def iter_tuples(self):
        """Yield (slot_id, raw_bytes) for every live slot."""
        for sid in range(self.num_slots):
            data = self.get_tuple(sid)
            if data is not None:
                yield sid, data

    def to_bytes(self) -> bytes:
        self._flush_header()
        return bytes(self.data)
