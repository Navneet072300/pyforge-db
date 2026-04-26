"""
Strata Engine — single-node and multi-node cluster entry points.

StrataNode   — one Cassandra-like node: per-table LSM trees + commit log
StrataCluster — a set of StrataNodes wired together via a ring + gossip
StrataEngine  — top-level API (USE keyspace, execute CQL, cluster management)
"""
from __future__ import annotations

import os
import time
import threading
from typing import Any, Dict, List, Optional

from .catalog      import Catalog
from .cluster      import ConsistentHashRing, RingNode, GossipAgent, NodeStatus
from .consistency  import Coordinator, ConsistencyLevel
from .storage.lsm  import LSMTree
from .storage.commit_log import CommitLog, CommitLogRecord
from .query.executor import Executor, QueryResult, ExecutionError


# ═══════════════════════════════════════════════════════════════════════════════
# StrataNode
# ═══════════════════════════════════════════════════════════════════════════════

class StrataNode:
    """
    A single Strata node.

    Owns:
    - One LSMTree per (keyspace, table) pair
    - A commit log for write durability
    - A GossipAgent for cluster health
    """

    def __init__(self, node_id: str, data_dir: str):
        self.node_id  = node_id
        self.data_dir = data_dir
        os.makedirs(data_dir, exist_ok=True)

        self._lsm:    Dict[str, LSMTree]  = {}   # "ks.table" → LSMTree
        self._alive   = True
        self.gossip   = GossipAgent(node_id)

        log_path = os.path.join(data_dir, 'commit.log')
        self._commit_log = CommitLog(log_path)
        self._recover()

    # ── lifecycle ─────────────────────────────────────────────────────────────

    def is_alive(self) -> bool:
        return self._alive

    def stop(self):
        self._alive = False
        self.flush_all()
        for lsm in self._lsm.values():
            lsm.close()

    def flush_all(self):
        for lsm in self._lsm.values():
            lsm.flush_all()

    # ── DDL callbacks (called by Executor after catalog update) ───────────────

    def on_keyspace_created(self, keyspace: str):
        ks_dir = os.path.join(self.data_dir, keyspace)
        os.makedirs(ks_dir, exist_ok=True)

    def on_keyspace_dropped(self, keyspace: str):
        prefix = keyspace + '.'
        stale  = [k for k in list(self._lsm) if k.startswith(prefix)]
        for key in stale:
            self._lsm[key].close()
            del self._lsm[key]

    def on_table_created(self, keyspace: str, table: str):
        self._get_lsm(keyspace, table)   # creates the LSMTree directory

    def on_table_dropped(self, keyspace: str, table: str):
        key = f"{keyspace}.{table}"
        if key in self._lsm:
            self._lsm[key].close()
            del self._lsm[key]

    # ── local write/read (called by Coordinator) ──────────────────────────────

    def local_put(self, keyspace: str, table: str,
                  pk: str, ck: str, columns: dict, timestamp: int):
        self._commit_log.append(CommitLogRecord(
            'PUT', keyspace, table, pk, ck, columns, timestamp))
        self._get_lsm(keyspace, table).put(pk, ck, columns, timestamp)

    def local_delete(self, keyspace: str, table: str,
                     pk: str, ck: str, timestamp: int):
        self._commit_log.append(CommitLogRecord(
            'DELETE', keyspace, table, pk, ck, {}, timestamp, tombstone=True))
        self._get_lsm(keyspace, table).delete(pk, ck, timestamp)

    def local_scan(self, keyspace: str, table: str, pk: str,
                   ck_lo=None, ck_hi=None,
                   ck_lo_exclusive=False, ck_hi_exclusive=False,
                   reversed_=False, limit=None):
        lsm = self._get_lsm(keyspace, table)
        return lsm.scan(pk, ck_lo, ck_hi,
                        ck_lo_exclusive, ck_hi_exclusive,
                        reversed_, limit)

    def local_truncate(self, keyspace: str, table: str):
        key = f"{keyspace}.{table}"
        if key in self._lsm:
            self._lsm[key].close()
        tbl_dir = os.path.join(self.data_dir, keyspace, table)
        if os.path.isdir(tbl_dir):
            for fname in os.listdir(tbl_dir):
                fpath = os.path.join(tbl_dir, fname)
                try:
                    os.remove(fpath)
                except OSError:
                    pass
        self._lsm[key] = LSMTree(tbl_dir)

    # ── gossip tick ───────────────────────────────────────────────────────────

    def gossip_tick(self):
        self.gossip.tick()

    # ── internals ─────────────────────────────────────────────────────────────

    def _get_lsm(self, keyspace: str, table: str) -> LSMTree:
        key = f"{keyspace}.{table}"
        if key not in self._lsm:
            tbl_dir = os.path.join(self.data_dir, keyspace, table)
            os.makedirs(tbl_dir, exist_ok=True)
            self._lsm[key] = LSMTree(tbl_dir)
        return self._lsm[key]

    def _recover(self):
        """Replay commit log to rebuild MemTable state after crash."""
        for rec in self._commit_log.replay():
            try:
                lsm = self._get_lsm(rec.keyspace, rec.table)
                if rec.op == 'PUT':
                    lsm.put(rec.pk, rec.ck, rec.columns, rec.timestamp)
                elif rec.op == 'DELETE':
                    lsm.delete(rec.pk, rec.ck, rec.timestamp)
            except Exception:
                pass


# ═══════════════════════════════════════════════════════════════════════════════
# StrataCluster
# ═══════════════════════════════════════════════════════════════════════════════

class StrataCluster:
    """
    Manages a set of StrataNodes with a consistent hash ring and gossip.

    Parameters
    ----------
    data_dir           : base directory; each node gets data_dir/<node_id>/
    replication_factor : default RF; overridden per keyspace if specified
    num_vnodes         : virtual nodes per physical node on the ring
    """

    def __init__(self, data_dir: str,
                 replication_factor: int = 3,
                 num_vnodes: int = 150):
        self._data_dir = data_dir
        self._rf       = replication_factor
        os.makedirs(data_dir, exist_ok=True)

        catalog_path = os.path.join(data_dir, 'catalog.json')
        self.catalog  = Catalog(catalog_path)

        self._ring:  ConsistentHashRing      = ConsistentHashRing(num_vnodes)
        self._nodes: Dict[str, StrataNode]   = {}
        self._gossip_agents: Dict[str, GossipAgent] = {}

        self._gossip_thread: Optional[threading.Thread] = None
        self._stop_gossip = threading.Event()

    # ── node management ───────────────────────────────────────────────────────

    def add_node(self, node_id: str) -> StrataNode:
        node_dir = os.path.join(self._data_dir, node_id)
        node     = StrataNode(node_id, node_dir)
        self._nodes[node_id]         = node
        self._gossip_agents[node_id] = node.gossip

        ring_node = RingNode(node_id, node_id)
        self._ring.add_node(ring_node)

        # Re-register all gossip agents with each other
        for agent in self._gossip_agents.values():
            agent.register_peers(self._gossip_agents)

        return node

    def remove_node(self, node_id: str):
        if node_id in self._nodes:
            self._nodes[node_id].stop()
            del self._nodes[node_id]
        if node_id in self._gossip_agents:
            del self._gossip_agents[node_id]
        self._ring.remove_node(node_id)

    def get_node(self, node_id: str) -> Optional[StrataNode]:
        return self._nodes.get(node_id)

    def all_node_ids(self) -> List[str]:
        return list(self._nodes.keys())

    # ── coordinator ───────────────────────────────────────────────────────────

    def coordinator(self, node_id: Optional[str] = None,
                    replication_factor: Optional[int] = None) -> Coordinator:
        """
        Return a Coordinator using the gossip view from `node_id`.
        If node_id is None, uses the first live node.
        """
        rf = replication_factor or self._rf
        if node_id:
            gossip = self._gossip_agents.get(node_id)
        else:
            gossip = next(iter(self._gossip_agents.values()), None)

        return Coordinator(self._ring, self._nodes, rf, gossip)

    # ── gossip simulation ─────────────────────────────────────────────────────

    def gossip_tick_all(self):
        """Advance one gossip round for every node simultaneously."""
        for node in self._nodes.values():
            if node.is_alive():
                node.gossip_tick()

    def start_gossip_background(self, interval_s: float = 1.0):
        """Spawn a background thread that calls gossip_tick_all periodically."""
        self._stop_gossip.clear()

        def _loop():
            while not self._stop_gossip.wait(interval_s):
                self.gossip_tick_all()

        self._gossip_thread = threading.Thread(
            target=_loop, daemon=True, name='gossip')
        self._gossip_thread.start()

    def stop_gossip_background(self):
        self._stop_gossip.set()
        if self._gossip_thread:
            self._gossip_thread.join(timeout=3)

    # ── cluster status ────────────────────────────────────────────────────────

    def cluster_status(self) -> Dict[str, str]:
        """Return {node_id: status_str} from the first live node's gossip view."""
        if not self._gossip_agents:
            return {}
        agent = next(iter(self._gossip_agents.values()))
        return agent.cluster_view()

    def ring_distribution(self) -> Dict[str, int]:
        return self._ring.token_distribution()

    def shutdown(self):
        self.stop_gossip_background()
        for node in self._nodes.values():
            node.stop()


# ═══════════════════════════════════════════════════════════════════════════════
# StrataEngine  — top-level user-facing API
# ═══════════════════════════════════════════════════════════════════════════════

class StrataEngine:
    """
    Top-level API for the Strata engine.

    Usage (single node)::

        engine = StrataEngine.single_node('/tmp/strata')
        engine.execute("CREATE KEYSPACE ks WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'}")
        engine.execute("USE ks")
        engine.execute("CREATE TABLE users (id TEXT, age INT, PRIMARY KEY (id))")
        engine.execute("INSERT INTO users (id, age) VALUES ('alice', 30)")
        result = engine.execute("SELECT * FROM users WHERE id = 'alice'")

    Usage (multi-node cluster)::

        engine = StrataEngine.cluster('/tmp/strata_cluster',
                                      node_ids=['n1','n2','n3'],
                                      replication_factor=3)
    """

    def __init__(self, cluster: StrataCluster,
                 default_consistency: str = 'QUORUM'):
        self._cluster  = cluster
        self._keyspace: Optional[str] = None
        self._consistency = default_consistency

        # Pick the first node as the local coordinator node
        nids = cluster.all_node_ids()
        if not nids:
            raise RuntimeError("Cluster has no nodes")
        self._local_node_id = nids[0]

        self._executor = self._make_executor()

    # ── factory helpers ───────────────────────────────────────────────────────

    @classmethod
    def single_node(cls, data_dir: str,
                    consistency: str = 'ONE') -> 'StrataEngine':
        cluster = StrataCluster(data_dir, replication_factor=1)
        cluster.add_node('node1')
        return cls(cluster, default_consistency=consistency)

    @classmethod
    def cluster(cls, data_dir: str,
                node_ids: Optional[List[str]] = None,
                replication_factor: int = 3,
                consistency: str = 'QUORUM',
                start_gossip: bool = True) -> 'StrataEngine':
        if node_ids is None:
            node_ids = ['n1', 'n2', 'n3']
        c = StrataCluster(data_dir, replication_factor=replication_factor)
        for nid in node_ids:
            c.add_node(nid)
        if start_gossip:
            c.start_gossip_background(interval_s=1.0)
        return cls(c, default_consistency=consistency)

    # ── execution ─────────────────────────────────────────────────────────────

    def execute(self, cql: str,
                consistency: Optional[str] = None) -> QueryResult:
        cl = consistency or self._consistency
        result = self._executor.execute(cql, self._keyspace, cl)

        # Mirror USE keyspace state into the engine
        if cql.strip().upper().startswith('USE '):
            self._keyspace = self._executor._keyspace

        return result

    def use(self, keyspace: str):
        self._keyspace = keyspace
        self._executor._keyspace = keyspace

    # ── cluster management pass-throughs ──────────────────────────────────────

    def add_node(self, node_id: str) -> StrataNode:
        node = self._cluster.add_node(node_id)
        self._executor = self._make_executor()   # refresh coordinator
        return node

    def remove_node(self, node_id: str):
        self._cluster.remove_node(node_id)
        if node_id == self._local_node_id:
            nids = self._cluster.all_node_ids()
            self._local_node_id = nids[0] if nids else None
        self._executor = self._make_executor()

    def cluster_status(self) -> Dict[str, str]:
        return self._cluster.cluster_status()

    def ring_distribution(self) -> Dict[str, int]:
        return self._cluster.ring_distribution()

    def shutdown(self):
        self._cluster.shutdown()

    # ── internals ─────────────────────────────────────────────────────────────

    def _make_executor(self) -> Executor:
        local_node = self._cluster.get_node(self._local_node_id)
        coord      = self._cluster.coordinator(self._local_node_id)
        return Executor(self._cluster.catalog, coord, local_node)
