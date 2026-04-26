"""
LRU buffer pool – keeps pages in memory to avoid repeated disk I/O.
Key: (filepath, page_id) → Page object.
"""
from collections import OrderedDict
from typing import Dict, Optional, Tuple

from .heap import HeapFile
from .page import Page

_Key = Tuple[str, int]


class BufferPool:
    def __init__(self, capacity: int = 512):
        self.capacity   = capacity
        self._cache: OrderedDict[_Key, Page] = OrderedDict()
        self._heaps: Dict[str, HeapFile]      = {}

    # ── heap file registry ───────────────────────────────────────────────────
    def register(self, filepath: str) -> HeapFile:
        if filepath not in self._heaps:
            self._heaps[filepath] = HeapFile(filepath)
        return self._heaps[filepath]

    def _heap(self, filepath: str) -> HeapFile:
        return self.register(filepath)

    # ── page access ──────────────────────────────────────────────────────────
    def fetch(self, filepath: str, page_id: int) -> Optional[Page]:
        key = (filepath, page_id)
        if key in self._cache:
            self._cache.move_to_end(key)
            return self._cache[key]
        page = self._heap(filepath).read_page(page_id)
        if page is None:
            return None
        self._admit(key, page)
        return page

    def new_page(self, filepath: str) -> Page:
        page = self._heap(filepath).allocate_page()
        key  = (filepath, page.page_id)
        self._admit(key, page)
        return page

    def mark_dirty(self, filepath: str, page_id: int):
        key = (filepath, page_id)
        if key in self._cache:
            self._cache[key].dirty = True

    # ── flushing ────────────────────────────────────────────────────────────
    def flush(self, filepath: str, page_id: int):
        key = (filepath, page_id)
        if key in self._cache and self._cache[key].dirty:
            self._heap(filepath).write_page(self._cache[key])

    def flush_all(self):
        for (fp, _), page in list(self._cache.items()):
            if page.dirty:
                self._heap(fp).write_page(page)

    def flush_table(self, filepath: str):
        for (fp, _), page in list(self._cache.items()):
            if fp == filepath and page.dirty:
                self._heap(fp).write_page(page)

    def evict_table(self, filepath: str):
        """Remove all cached pages for a table (after drop)."""
        for key in [k for k in self._cache if k[0] == filepath]:
            del self._cache[key]

    # ── internals ────────────────────────────────────────────────────────────
    def _admit(self, key: _Key, page: Page):
        if len(self._cache) >= self.capacity:
            self._evict_one()
        self._cache[key] = page

    def _evict_one(self):
        # LRU = front of OrderedDict
        lru_key, lru_page = next(iter(self._cache.items()))
        if lru_page.dirty:
            fp = lru_key[0]
            self._heap(fp).write_page(lru_page)
        del self._cache[lru_key]

    def num_pages(self, filepath: str) -> int:
        return self._heap(filepath).num_pages()
