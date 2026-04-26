"""
Write-Ahead Log (WAL) for durability and crash recovery.

Record wire format (binary):
  lsn(8) xid(4) rtype(1) datalen(4) data(datalen bytes, JSON-encoded)

Record types mirror the set of operations that mutate heap files so the
recovery routine can redo committed changes and skip aborted ones.
"""
import json
import os
import struct
from enum import IntEnum
from typing import Dict, Iterator, List, Optional

_HDR_FMT  = '<QIbI'                    # lsn, xid, rtype, datalen
_HDR_SIZE = struct.calcsize(_HDR_FMT)   # 17 bytes


class WALType(IntEnum):
    BEGIN      = 1
    INSERT     = 2
    UPDATE     = 3
    DELETE     = 4
    COMMIT     = 5
    ABORT      = 6
    CHECKPOINT = 7


class WALRecord:
    __slots__ = ('lsn', 'xid', 'rtype', 'data')

    def __init__(self, lsn: int, xid: int, rtype: WALType, data: dict):
        self.lsn   = lsn
        self.xid   = xid
        self.rtype = rtype
        self.data  = data

    def __repr__(self):
        return f"WALRecord(lsn={self.lsn}, xid={self.xid}, type={self.rtype.name})"


class WAL:
    def __init__(self, filepath: str):
        self.filepath    = filepath
        self._lsn        = 0
        if os.path.exists(filepath):
            # Fast-scan to find the highest LSN already written
            for rec in self._iter_file():
                if rec.lsn > self._lsn:
                    self._lsn = rec.lsn

    # ── writing ──────────────────────────────────────────────────────────────
    def append(self, xid: int, rtype: WALType, data: dict) -> int:
        self._lsn += 1
        raw = json.dumps(data, default=str).encode()
        hdr = struct.pack(_HDR_FMT, self._lsn, xid, int(rtype), len(raw))
        with open(self.filepath, 'ab') as f:
            f.write(hdr)
            f.write(raw)
            f.flush()
            os.fsync(f.fileno())
        return self._lsn

    @property
    def current_lsn(self) -> int:
        return self._lsn

    # ── reading ──────────────────────────────────────────────────────────────
    def read_all(self) -> List[WALRecord]:
        return list(self._iter_file())

    def _iter_file(self) -> Iterator[WALRecord]:
        if not os.path.exists(self.filepath):
            return
        with open(self.filepath, 'rb') as f:
            while True:
                hdr = f.read(_HDR_SIZE)
                if len(hdr) < _HDR_SIZE:
                    break
                lsn, xid, rtype, datalen = struct.unpack(_HDR_FMT, hdr)
                raw = f.read(datalen)
                if len(raw) < datalen:
                    break          # truncated record — stop here
                data = json.loads(raw.decode())
                yield WALRecord(lsn, xid, WALType(rtype), data)

    # ── recovery ─────────────────────────────────────────────────────────────
    def committed_xids(self) -> Dict[int, List[WALRecord]]:
        """Return {xid: [records]} for every committed transaction."""
        all_recs: Dict[int, List[WALRecord]] = {}
        committed: set = set()
        for rec in self._iter_file():
            all_recs.setdefault(rec.xid, []).append(rec)
            if rec.rtype == WALType.COMMIT:
                committed.add(rec.xid)
        return {xid: recs for xid, recs in all_recs.items() if xid in committed}

    def truncate(self):
        with open(self.filepath, 'wb'):
            pass
        self._lsn = 0
