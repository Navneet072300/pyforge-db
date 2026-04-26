"""
Consistent hashing ring with virtual nodes (vnodes).

Each physical node is mapped to `num_vnodes` positions on a 128-bit hash
ring.  Writes and reads are routed to the first N distinct physical nodes
clockwise from the key's hash position.
"""
import hashlib
from bisect import bisect_right, insort
from typing import Any, Dict, List, Optional, Tuple


class RingNode:
    """Lightweight descriptor for a node on the ring."""
    __slots__ = ('node_id', 'address')

    def __init__(self, node_id: str, address: str = ''):
        self.node_id = node_id
        self.address = address

    def __repr__(self):
        return f"RingNode({self.node_id!r})"


class ConsistentHashRing:
    def __init__(self, num_vnodes: int = 150):
        self.num_vnodes = num_vnodes
        self._ring:  List[int]          = []   # sorted hash positions
        self._vmap:  Dict[int, str]     = {}   # hash → node_id
        self._nodes: Dict[str, RingNode] = {}

    # ── node management ───────────────────────────────────────────────────────
    def add_node(self, node: RingNode):
        self._nodes[node.node_id] = node
        for i in range(self.num_vnodes):
            h = self._hash(f"{node.node_id}:vn{i}")
            insort(self._ring, h)
            self._vmap[h] = node.node_id

    def remove_node(self, node_id: str):
        to_remove = [h for h, nid in self._vmap.items() if nid == node_id]
        for h in to_remove:
            idx = bisect_right(self._ring, h) - 1
            if 0 <= idx < len(self._ring) and self._ring[idx] == h:
                self._ring.pop(idx)
            del self._vmap[h]
        self._nodes.pop(node_id, None)

    def all_nodes(self) -> List[RingNode]:
        return list(self._nodes.values())

    def node_count(self) -> int:
        return len(self._nodes)

    # ── routing ───────────────────────────────────────────────────────────────
    def get_replicas(self, key: str, n: int) -> List[RingNode]:
        """Return up to n distinct physical nodes responsible for key."""
        if not self._ring:
            return []
        h   = self._hash(key)
        pos = bisect_right(self._ring, h) % len(self._ring)

        seen:     set             = set()
        replicas: List[RingNode] = []
        for i in range(len(self._ring)):
            idx    = (pos + i) % len(self._ring)
            nid    = self._vmap[self._ring[idx]]
            if nid not in seen and nid in self._nodes:
                seen.add(nid)
                replicas.append(self._nodes[nid])
                if len(replicas) == n:
                    break
        return replicas

    def primary(self, key: str) -> Optional[RingNode]:
        reps = self.get_replicas(key, 1)
        return reps[0] if reps else None

    # ── diagnostics ───────────────────────────────────────────────────────────
    def token_distribution(self) -> Dict[str, int]:
        """Count vnodes owned per physical node."""
        dist: Dict[str, int] = {nid: 0 for nid in self._nodes}
        for nid in self._vmap.values():
            dist[nid] = dist.get(nid, 0) + 1
        return dist

    # ── internals ────────────────────────────────────────────────────────────
    @staticmethod
    def _hash(s: str) -> int:
        return int(hashlib.md5(s.encode()).hexdigest(), 16)
