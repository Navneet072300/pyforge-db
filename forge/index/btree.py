"""
B+ tree index for fast key lookups and range scans.

Leaves store  keys → [list of (page_id, slot_id) RIDs]  and are linked
for sequential range scans.  Internal nodes store separator keys + child
pointers.  Duplicate keys are supported (multiple RIDs per key).

The tree is kept entirely in memory and serialised to disk with pickle.
"""
import bisect
import os
import pickle
from typing import Any, Dict, Iterator, List, Optional, Tuple

RID = Tuple[int, int]   # (page_id, slot_id)


class _Node:
    __slots__ = ('leaf', 'keys', 'vals', 'next')

    def __init__(self, leaf: bool = True):
        self.leaf: bool         = leaf
        self.keys: List[Any]    = []
        # leaf  → vals[i] = list[RID] for keys[i]
        # internal → vals[i] = child _Node  (len = len(keys)+1, vals[0] is leftmost)
        self.vals: list         = []
        self.next: Optional['_Node'] = None   # leaf-chain linkage


class BPlusTree:
    """Persistent B+ tree.  order controls max fan-out (2*order keys/node)."""

    def __init__(self, order: int = 64):
        self.order = order
        self.root  = _Node(leaf=True)
        self._size = 0

    # ── public interface ─────────────────────────────────────────────────────

    def insert(self, key: Any, rid: RID):
        result = self._insert_node(self.root, key, rid)
        if result:
            sep, right = result
            new_root      = _Node(leaf=False)
            new_root.keys = [sep]
            new_root.vals = [self.root, right]
            self.root = new_root
        self._size += 1

    def search(self, key: Any) -> List[RID]:
        leaf = self._find_leaf(key)
        i    = bisect.bisect_left(leaf.keys, key)
        if i < len(leaf.keys) and leaf.keys[i] == key:
            return list(leaf.vals[i])
        return []

    def range_search(self, lo=None, hi=None) -> List[RID]:
        """Return all RIDs with lo <= key <= hi (None = unbounded)."""
        results: List[RID] = []
        node = self._leftmost_leaf() if lo is None else self._find_leaf(lo)
        while node:
            for i, k in enumerate(node.keys):
                if lo is not None and k < lo:
                    continue
                if hi is not None and k > hi:
                    return results
                results.extend(node.vals[i])
            node = node.next
        return results

    def delete(self, key: Any, rid: RID):
        """Remove one (key, rid) entry.  No rebalancing — correct but lazy."""
        leaf = self._find_leaf(key)
        i    = bisect.bisect_left(leaf.keys, key)
        while i < len(leaf.keys) and leaf.keys[i] == key:
            try:
                leaf.vals[i].remove(rid)
                self._size -= 1
                if not leaf.vals[i]:
                    leaf.keys.pop(i)
                    leaf.vals.pop(i)
            except ValueError:
                pass
            break

    def __len__(self) -> int:
        return self._size

    def iter_all(self) -> Iterator[Tuple[Any, RID]]:
        node = self._leftmost_leaf()
        while node:
            for k, rids in zip(node.keys, node.vals):
                for rid in rids:
                    yield k, rid
            node = node.next

    # ── serialisation ────────────────────────────────────────────────────────

    def save(self, path: str):
        with open(path, 'wb') as f:
            pickle.dump(self, f, protocol=pickle.HIGHEST_PROTOCOL)

    @classmethod
    def load(cls, path: str) -> 'BPlusTree':
        with open(path, 'rb') as f:
            return pickle.load(f)

    @classmethod
    def load_or_new(cls, path: str, order: int = 64) -> 'BPlusTree':
        if os.path.exists(path):
            return cls.load(path)
        return cls(order)

    # ── internals ────────────────────────────────────────────────────────────

    def _find_leaf(self, key: Any) -> _Node:
        node = self.root
        while not node.leaf:
            i = bisect.bisect_right(node.keys, key)
            node = node.vals[i]
        return node

    def _leftmost_leaf(self) -> _Node:
        node = self.root
        while not node.leaf:
            node = node.vals[0]
        return node

    def _insert_node(self, node: _Node, key: Any, rid: RID
                     ) -> Optional[Tuple[Any, _Node]]:
        if node.leaf:
            self._leaf_insert(node, key, rid)
            if len(node.keys) >= 2 * self.order:
                return self._split_leaf(node)
            return None
        # internal node
        i     = bisect.bisect_right(node.keys, key)
        child = node.vals[i]
        res   = self._insert_node(child, key, rid)
        if res:
            sep, right = res
            j = bisect.bisect_right(node.keys, sep)
            node.keys.insert(j, sep)
            node.vals.insert(j + 1, right)
            if len(node.keys) >= 2 * self.order:
                return self._split_internal(node)
        return None

    @staticmethod
    def _leaf_insert(leaf: _Node, key: Any, rid: RID):
        i = bisect.bisect_left(leaf.keys, key)
        if i < len(leaf.keys) and leaf.keys[i] == key:
            leaf.vals[i].append(rid)
        else:
            leaf.keys.insert(i, key)
            leaf.vals.insert(i, [rid])

    @staticmethod
    def _split_leaf(leaf: _Node) -> Tuple[Any, _Node]:
        mid  = len(leaf.keys) // 2
        right           = _Node(leaf=True)
        right.keys      = leaf.keys[mid:]
        right.vals      = leaf.vals[mid:]
        right.next      = leaf.next
        leaf.keys       = leaf.keys[:mid]
        leaf.vals       = leaf.vals[:mid]
        leaf.next       = right
        return right.keys[0], right

    @staticmethod
    def _split_internal(node: _Node) -> Tuple[Any, _Node]:
        mid     = len(node.keys) // 2
        sep     = node.keys[mid]
        right           = _Node(leaf=False)
        right.keys      = node.keys[mid + 1:]
        right.vals      = node.vals[mid + 1:]
        node.keys       = node.keys[:mid]
        node.vals       = node.vals[:mid + 1]
        return sep, right
