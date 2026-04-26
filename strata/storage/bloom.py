"""
Bloom filter for fast SSTable membership tests.

Uses k independent hash functions derived from SHA-256 double-hashing.
Supports serialisation to/from bytes for embedding in SSTable files.
"""
import hashlib
import math
import struct
from typing import Union


class BloomFilter:
    def __init__(self, capacity: int = 10_000, error_rate: float = 0.01):
        m = max(1, int(-capacity * math.log(error_rate) / (math.log(2) ** 2)))
        k = max(1, int(m / capacity * math.log(2)))
        self.m     = m
        self.k     = k
        self.bits  = bytearray((m + 7) // 8)
        self._count = 0

    # ── mutation ─────────────────────────────────────────────────────────────
    def add(self, key):
        if isinstance(key, str):
            key = key.encode('utf-8')
        for pos in self._positions(key):
            self.bits[pos >> 3] |= 1 << (pos & 7)
        self._count += 1

    # ── query ────────────────────────────────────────────────────────────────
    def might_contain(self, key) -> bool:
        if isinstance(key, str):
            key = key.encode('utf-8')
        return all(self.bits[pos >> 3] & (1 << (pos & 7))
                   for pos in self._positions(key))

    # ── serialisation ─────────────────────────────────────────────────────────
    def to_bytes(self) -> bytes:
        bits_bytes = bytes(self.bits)
        return struct.pack('>III', self.m, self.k, len(bits_bytes)) + bits_bytes

    @classmethod
    def from_bytes(cls, data: bytes) -> 'BloomFilter':
        m, k, blen = struct.unpack_from('>III', data, 0)
        bf       = cls.__new__(cls)
        bf.m     = m
        bf.k     = k
        bf.bits  = bytearray(data[12 : 12 + blen])
        bf._count = 0
        return bf

    # ── internals ────────────────────────────────────────────────────────────
    def _positions(self, key: bytes):
        h1 = int.from_bytes(hashlib.sha256(key).digest()[:8],  'big')
        h2 = int.from_bytes(hashlib.sha256(key + b'\xff').digest()[:8], 'big')
        for i in range(self.k):
            yield (h1 + i * h2) % self.m
