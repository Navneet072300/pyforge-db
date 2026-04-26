"""
Gossip protocol — simulated in-process for the multi-node cluster.

Each node maintains a gossip table: {node_id → NodeState}.  On every
gossip tick a node picks a random live peer, exchanges its full table,
and merges any newer heartbeat/status information received.  Nodes that
miss FAILURE_THRESHOLD consecutive ticks are marked SUSPECT then DOWN.
"""
import random
import time
from dataclasses import dataclass, field
from enum import Enum
from typing import Dict, List, Optional


class NodeStatus(Enum):
    UP      = 'UP'
    SUSPECT = 'SUSPECT'
    DOWN    = 'DOWN'


@dataclass
class NodeState:
    node_id:    str
    heartbeat:  int         = 0
    status:     NodeStatus  = NodeStatus.UP
    updated_at: float       = field(default_factory=time.time)

    def as_dict(self):
        return {
            'hb':  self.heartbeat,
            'st':  self.status.value,
            'ts':  self.updated_at,
        }

    @classmethod
    def from_dict(cls, node_id: str, d: dict) -> 'NodeState':
        s = cls(node_id)
        s.heartbeat  = d['hb']
        s.status     = NodeStatus(d['st'])
        s.updated_at = d['ts']
        return s


SUSPECT_THRESHOLD = 3.0   # seconds without heartbeat → SUSPECT
DOWN_THRESHOLD    = 8.0   # seconds without heartbeat → DOWN


class GossipAgent:
    """Per-node gossip state and merge logic."""

    def __init__(self, node_id: str):
        self.node_id   = node_id
        self.table: Dict[str, NodeState] = {
            node_id: NodeState(node_id, heartbeat=0, status=NodeStatus.UP)
        }
        self._all_agents: Dict[str, 'GossipAgent'] = {}

    def register_peers(self, agents: Dict[str, 'GossipAgent']):
        self._all_agents = agents
        for nid in agents:
            if nid not in self.table:
                self.table[nid] = NodeState(nid)

    # ── one gossip tick ───────────────────────────────────────────────────────
    def tick(self):
        self._increment_heartbeat()
        self._detect_failures()
        self._gossip_with_random_peer()

    def _increment_heartbeat(self):
        s = self.table[self.node_id]
        s.heartbeat  += 1
        s.updated_at  = time.time()
        s.status      = NodeStatus.UP

    def _detect_failures(self):
        now = time.time()
        for nid, state in self.table.items():
            if nid == self.node_id:
                continue
            age = now - state.updated_at
            if age > DOWN_THRESHOLD:
                state.status = NodeStatus.DOWN
            elif age > SUSPECT_THRESHOLD:
                state.status = NodeStatus.SUSPECT

    def _gossip_with_random_peer(self):
        live_peers = [
            nid for nid, st in self.table.items()
            if nid != self.node_id and st.status != NodeStatus.DOWN
            and nid in self._all_agents
        ]
        if not live_peers:
            return
        peer_id = random.choice(live_peers)
        peer    = self._all_agents[peer_id]
        peer.receive(self._export())

    def receive(self, remote: Dict[str, dict]):
        for nid, d in remote.items():
            if nid not in self.table:
                self.table[nid] = NodeState.from_dict(nid, d)
            else:
                local = self.table[nid]
                if d['hb'] > local.heartbeat:
                    local.heartbeat  = d['hb']
                    local.status     = NodeStatus(d['st'])
                    local.updated_at = d['ts']

    def _export(self) -> Dict[str, dict]:
        return {nid: s.as_dict() for nid, s in self.table.items()}

    # ── queries ───────────────────────────────────────────────────────────────
    def live_nodes(self) -> List[str]:
        return [nid for nid, s in self.table.items()
                if s.status == NodeStatus.UP]

    def is_alive(self, node_id: str) -> bool:
        return self.table.get(node_id, NodeState(node_id)).status == NodeStatus.UP

    def cluster_view(self) -> Dict[str, str]:
        return {nid: s.status.value for nid, s in self.table.items()}
