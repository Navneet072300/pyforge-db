"""
Consistency levels and the coordinator logic that drives replication.

ONE    — succeed when 1 replica acknowledges
QUORUM — succeed when majority (RF/2 + 1) acknowledges
ALL    — succeed when all RF replicas acknowledge

The Coordinator routes writes/reads to the correct replica set and
applies the read-repair / latest-wins merge for reads.
"""
from __future__ import annotations

import time
from enum import Enum
from typing import Any, Dict, List, Optional, Tuple, TYPE_CHECKING

if TYPE_CHECKING:
    from .engine import StrataNode


class ConsistencyLevel(Enum):
    ONE    = 'ONE'
    QUORUM = 'QUORUM'
    ALL    = 'ALL'


def quorum(n: int) -> int:
    return n // 2 + 1


def required(level: ConsistencyLevel, rf: int) -> int:
    if level == ConsistencyLevel.ONE:
        return 1
    if level == ConsistencyLevel.QUORUM:
        return quorum(rf)
    return rf   # ALL


class WriteError(Exception):
    pass


class ReadError(Exception):
    pass


class Coordinator:
    def __init__(self, ring, nodes: Dict[str, 'StrataNode'],
                 replication_factor: int, gossip: Optional[Any] = None):
        self.ring  = ring
        self.nodes = nodes
        self.rf    = replication_factor
        self._gossip = gossip

    # ── write ─────────────────────────────────────────────────────────────────
    def write(self, keyspace: str, table: str,
              pk: str, ck: str, columns: dict, timestamp: int,
              tombstone: bool = False,
              consistency: ConsistencyLevel = ConsistencyLevel.QUORUM):
        replicas = self._live_replicas(pk)
        need     = required(consistency, self.rf)
        if len(replicas) < need:
            raise WriteError(
                f"Not enough live replicas: have {len(replicas)}, need {need}")

        successes = 0
        for node in replicas:
            try:
                if tombstone:
                    node.local_delete(keyspace, table, pk, ck, timestamp)
                else:
                    node.local_put(keyspace, table, pk, ck, columns, timestamp)
                successes += 1
                if successes >= need:
                    break
            except Exception as e:
                pass   # replica unavailable

        if successes < need:
            raise WriteError(
                f"Write failed: achieved {successes}/{need} acknowledgements")

    # ── read ──────────────────────────────────────────────────────────────────
    def read(self, keyspace: str, table: str, pk: str, ck: Optional[str],
             ck_lo=None, ck_hi=None,
             ck_lo_exclusive=False, ck_hi_exclusive=False,
             reversed_: bool = False, limit: Optional[int] = None,
             consistency: ConsistencyLevel = ConsistencyLevel.QUORUM
             ) -> List[dict]:
        replicas = self._live_replicas(pk)
        need     = required(consistency, self.rf)
        if len(replicas) < need:
            raise ReadError(
                f"Not enough live replicas: have {len(replicas)}, need {need}")

        # Gather responses from `need` replicas
        responses: List[List[Tuple]] = []
        for node in replicas[:need]:
            try:
                resp = node.local_scan(
                    keyspace, table, pk,
                    ck_lo=ck if ck is not None else ck_lo,
                    ck_hi=ck if ck is not None else ck_hi,
                    ck_lo_exclusive=ck_lo_exclusive,
                    ck_hi_exclusive=ck_hi_exclusive,
                    reversed_=reversed_, limit=limit)
                responses.append(resp)
            except Exception:
                pass

        if not responses:
            raise ReadError("No replica responded")

        merged = self._merge_responses(responses)

        # Apply final sort direction and limit at coordinator level
        if reversed_:
            merged.sort(key=lambda r: r.get('__ck__', ''), reverse=True)
        if limit is not None:
            merged = merged[:limit]

        return merged

    # ── internals ─────────────────────────────────────────────────────────────
    def _live_replicas(self, pk: str) -> List['StrataNode']:
        ring_nodes = self.ring.get_replicas(pk, self.rf)
        live = []
        for rn in ring_nodes:
            node = self.nodes.get(rn.node_id)
            if node and node.is_alive():
                if self._gossip is None or self._gossip.is_alive(rn.node_id):
                    live.append(node)
        return live

    @staticmethod
    def _merge_responses(responses: List[List[Tuple]]) -> List[dict]:
        """Latest timestamp wins per (pk, ck) key across all replica responses."""
        merged: Dict[Tuple, Tuple] = {}
        for resp in responses:
            for key, cell in resp:
                existing = merged.get(key)
                if existing is None or cell.timestamp > existing[1].timestamp:
                    merged[key] = (key, cell)

        return [
            {'__pk__': k[0], '__ck__': k[1],
             '__ts__': c.timestamp, **c.columns}
            for k, (_, c) in sorted(merged.items())
            if not c.tombstone
        ]
