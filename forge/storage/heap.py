"""Heap file: a sequence of fixed-size pages stored on disk."""
import os
from typing import Iterator, Optional, Tuple

from .page import Page, PAGE_SIZE


class HeapFile:
    def __init__(self, filepath: str):
        self.filepath = filepath
        if not os.path.exists(filepath):
            open(filepath, 'wb').close()

    def num_pages(self) -> int:
        return os.path.getsize(self.filepath) // PAGE_SIZE

    # ── I/O ────────────────────────────────────────────────────────────────
    def read_page(self, page_id: int) -> Optional[Page]:
        if page_id >= self.num_pages():
            return None
        with open(self.filepath, 'rb') as f:
            f.seek(page_id * PAGE_SIZE)
            raw = f.read(PAGE_SIZE)
        if len(raw) < PAGE_SIZE:
            return None
        return Page(page_id, raw)

    def write_page(self, page: Page):
        with open(self.filepath, 'r+b') as f:
            f.seek(page.page_id * PAGE_SIZE)
            f.write(page.to_bytes())
            f.flush()
            os.fsync(f.fileno())
        page.dirty = False

    def allocate_page(self) -> Page:
        page_id = self.num_pages()
        page    = Page(page_id)
        with open(self.filepath, 'ab') as f:
            f.write(page.to_bytes())
            f.flush()
            os.fsync(f.fileno())
        return page

    # ── scan ────────────────────────────────────────────────────────────────
    def iter_pages(self) -> Iterator[Page]:
        for pid in range(self.num_pages()):
            p = self.read_page(pid)
            if p:
                yield p

    def truncate(self):
        with open(self.filepath, 'wb'):
            pass
