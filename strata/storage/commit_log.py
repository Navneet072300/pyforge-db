"""
Append-only commit log — every write is persisted here before the MemTable
is updated, providing crash recovery.  On startup the engine replays any
records that weren't yet flushed to an SSTable.

Wire format per record:
  crc32(4) | data_len(4) | data(JSON-UTF8)
"""
import json
import os
import struct
import zlib
from typing import Iterator, List

_HDR = '>II'
_HDR_SZ = struct.calcsize(_HDR)


class CommitLogRecord:
    __slots__ = ('op', 'keyspace', 'table', 'pk', 'ck', 'columns', 'timestamp', 'tombstone')

    def __init__(self, op, keyspace, table, pk, ck, columns, timestamp, tombstone=False):
        self.op        = op         # 'PUT' | 'DELETE'
        self.keyspace  = keyspace
        self.table     = table
        self.pk        = pk
        self.ck        = ck
        self.columns   = columns
        self.timestamp = timestamp
        self.tombstone = tombstone


class CommitLog:
    def __init__(self, path: str):
        self.path = path
        os.makedirs(os.path.dirname(path) if os.path.dirname(path) else '.', exist_ok=True)
        self._f = open(path, 'ab')

    # ── write ─────────────────────────────────────────────────────────────────
    def append(self, rec: CommitLogRecord):
        payload = json.dumps({
            'op': rec.op, 'ks': rec.keyspace, 'tbl': rec.table,
            'pk':  rec.pk, 'ck': rec.ck,
            'cols': rec.columns, 'ts': rec.timestamp, 'tomb': rec.tombstone,
        }, default=str).encode()
        crc  = zlib.crc32(payload) & 0xFFFFFFFF
        self._f.write(struct.pack(_HDR, crc, len(payload)) + payload)
        self._f.flush()
        os.fsync(self._f.fileno())

    # ── read ──────────────────────────────────────────────────────────────────
    def replay(self) -> List[CommitLogRecord]:
        recs = []
        if not os.path.exists(self.path):
            return recs
        with open(self.path, 'rb') as f:
            while True:
                hdr = f.read(_HDR_SZ)
                if len(hdr) < _HDR_SZ:
                    break
                crc, dlen = struct.unpack(_HDR, hdr)
                data = f.read(dlen)
                if len(data) < dlen:
                    break
                if zlib.crc32(data) & 0xFFFFFFFF != crc:
                    break   # corrupted — stop here
                d = json.loads(data)
                recs.append(CommitLogRecord(
                    d['op'], d['ks'], d['tbl'],
                    d['pk'], d['ck'], d['cols'], d['ts'], d['tomb'],
                ))
        return recs

    # ── lifecycle ─────────────────────────────────────────────────────────────
    def truncate(self):
        self._f.close()
        with open(self.path, 'wb'):
            pass
        self._f = open(self.path, 'ab')

    def close(self):
        self._f.close()
