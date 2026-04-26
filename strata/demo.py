"""
Strata demo — exercises every major feature:

  1. Single-node basic CRUD
  2. Clustering keys, range scans, ORDER BY
  3. Multi-node cluster with RF=3, QUORUM consistency
  4. Gossip failure detection simulation
  5. Compaction (fill past L0 flush threshold)
  6. Consistency level escalation / degradation
  7. TTL (commit-log level acknowledgement)
  8. DESCRIBE / schema introspection
"""
from __future__ import annotations

import time
import os
import shutil
import tempfile

from strata.engine import StrataEngine, StrataCluster
from strata.cluster.gossip import NodeStatus

DIVIDER = '─' * 64


def section(title: str):
    print(f"\n{DIVIDER}")
    print(f"  {title}")
    print(DIVIDER)


def show(result, label: str = ''):
    tag = f"[{label}] " if label else ''
    if result.rows:
        cols = list(result.rows[0].keys())
        widths = {c: max(len(c), max(len(str(r.get(c, ''))) for r in result.rows))
                  for c in cols}
        sep = '+' + '+'.join('-' * (widths[c] + 2) for c in cols) + '+'
        hdr = '|' + '|'.join(f" {c:{widths[c]}} " for c in cols) + '|'
        print(f"{tag}{sep}")
        print(f"{tag}{hdr}")
        print(f"{tag}{sep}")
        for row in result.rows:
            line = '|' + '|'.join(
                f" {str(row.get(c, '')):{widths[c]}} " for c in cols) + '|'
            print(f"{tag}{line}")
        print(f"{tag}{sep}")
        print(f"{tag}({len(result.rows)} row(s))")
    else:
        print(f"{tag}{result.message or 'OK'}")


def main():
    tmpdir = tempfile.mkdtemp(prefix='strata_demo_')
    try:
        _run_demo(tmpdir)
    finally:
        shutil.rmtree(tmpdir, ignore_errors=True)


def _run_demo(base: str):

    # ─────────────────────────────────────────────────────────────────────────
    # 1. Single-node basic CRUD
    # ─────────────────────────────────────────────────────────────────────────
    section("1. Single-node — basic CRUD")
    eng = StrataEngine.single_node(os.path.join(base, 'single'), consistency='ONE')

    eng.execute("""
        CREATE KEYSPACE shop
        WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'}
    """)
    eng.execute("USE shop")
    eng.execute("""
        CREATE TABLE products (
            id      TEXT,
            name    TEXT,
            price   FLOAT,
            PRIMARY KEY (id)
        )
    """)

    for pid, name, price in [
        ('p1', 'Widget', 9.99),
        ('p2', 'Gadget', 24.99),
        ('p3', 'Doohickey', 4.49),
    ]:
        eng.execute(f"INSERT INTO products (id, name, price) VALUES ('{pid}', '{name}', {price})")

    show(eng.execute("SELECT * FROM products WHERE id = 'p1'"), "SELECT p1")
    show(eng.execute("SELECT * FROM products WHERE id = 'p2'"), "SELECT p2")

    # Update
    eng.execute("UPDATE products SET price = 19.99 WHERE id = 'p2'")
    show(eng.execute("SELECT * FROM products WHERE id = 'p2'"), "after UPDATE")

    # Delete
    eng.execute("DELETE FROM products WHERE id = 'p3'")
    result = eng.execute("SELECT * FROM products WHERE id = 'p3'")
    print(f"[after DELETE p3] rows: {len(result.rows)}  (expected 0)")

    eng.shutdown()

    # ─────────────────────────────────────────────────────────────────────────
    # 2. Clustering keys + range scans + ORDER BY
    # ─────────────────────────────────────────────────────────────────────────
    section("2. Clustering keys — range scans and ORDER BY")
    eng = StrataEngine.single_node(os.path.join(base, 'ck'), consistency='ONE')
    eng.execute("""
        CREATE KEYSPACE logs
        WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'}
    """)
    eng.execute("USE logs")
    eng.execute("""
        CREATE TABLE events (
            user_id   TEXT,
            event_ts  TEXT,
            action    TEXT,
            PRIMARY KEY (user_id, event_ts)
        )
    """)

    events = [
        ('alice', '2024-01-01T10:00', 'login'),
        ('alice', '2024-01-01T10:05', 'view'),
        ('alice', '2024-01-01T10:10', 'purchase'),
        ('alice', '2024-01-01T10:20', 'logout'),
        ('bob',   '2024-01-01T09:00', 'login'),
        ('bob',   '2024-01-01T09:30', 'logout'),
    ]
    for uid, ts, act in events:
        eng.execute(f"INSERT INTO events (user_id, event_ts, action) "
                    f"VALUES ('{uid}', '{ts}', '{act}')")

    print("\nAll alice events:")
    show(eng.execute("SELECT * FROM events WHERE user_id = 'alice'"), "alice")

    print("\nalice events between 10:05 and 10:15:")
    show(eng.execute(
        "SELECT * FROM events WHERE user_id = 'alice' "
        "AND event_ts >= '2024-01-01T10:05' AND event_ts <= '2024-01-01T10:15' "
        "ALLOW FILTERING"
    ), "range")

    print("\nalice events DESC LIMIT 2:")
    show(eng.execute(
        "SELECT * FROM events WHERE user_id = 'alice' "
        "ORDER BY event_ts DESC LIMIT 2"
    ), "desc limit")

    eng.shutdown()

    # ─────────────────────────────────────────────────────────────────────────
    # 3. Multi-node cluster, RF=3, QUORUM
    # ─────────────────────────────────────────────────────────────────────────
    section("3. Multi-node cluster — RF=3, QUORUM consistency")
    eng = StrataEngine.cluster(
        os.path.join(base, 'cluster'),
        node_ids=['n1', 'n2', 'n3'],
        replication_factor=3,
        consistency='QUORUM',
        start_gossip=False,
    )

    eng.execute("""
        CREATE KEYSPACE social
        WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '3'}
    """)
    eng.execute("USE social")
    eng.execute("""
        CREATE TABLE users (
            id     TEXT,
            name   TEXT,
            age    INT,
            PRIMARY KEY (id)
        )
    """)

    for uid, name, age in [('u1', 'Alice', 30), ('u2', 'Bob', 25), ('u3', 'Carol', 35)]:
        eng.execute(f"INSERT INTO users (id, name, age) VALUES ('{uid}', '{name}', {age})")

    show(eng.execute("SELECT * FROM users WHERE id = 'u1'"), "QUORUM read u1")
    show(eng.execute("SELECT * FROM users WHERE id = 'u2'"), "QUORUM read u2")

    print("\nRing distribution:")
    for nid, cnt in sorted(eng.ring_distribution().items()):
        print(f"  {nid}: {cnt} vnodes")

    # ─────────────────────────────────────────────────────────────────────────
    # 4. Gossip failure simulation
    # ─────────────────────────────────────────────────────────────────────────
    section("4. Gossip — failure detection simulation")

    # Run several gossip ticks while all nodes are up
    cluster = eng._cluster
    for _ in range(5):
        cluster.gossip_tick_all()

    print("Cluster view (all UP):")
    for nid, st in sorted(cluster.cluster_status().items()):
        print(f"  {nid}: {st}")

    # Simulate n3 going down: mark it stopped and age its gossip heartbeat
    n3 = cluster.get_node('n3')
    n3._alive = False
    # Fast-forward n3's last-seen timestamp past the DOWN_THRESHOLD
    from strata.cluster.gossip import DOWN_THRESHOLD
    for agent in cluster._gossip_agents.values():
        if 'n3' in agent.table:
            agent.table['n3'].updated_at = time.time() - DOWN_THRESHOLD - 1.0
    # Run detection on n1 and n2
    for nid in ('n1', 'n2'):
        cluster.get_node(nid).gossip._detect_failures()

    print("\nCluster view after n3 goes quiet (seen from n1):")
    n1_view = cluster.get_node('n1').gossip.cluster_view()
    for nid, st in sorted(n1_view.items()):
        print(f"  {nid}: {st}")

    # QUORUM (2/3) should still work with n3 DOWN
    show(eng.execute("SELECT * FROM users WHERE id = 'u3'",
                     consistency='QUORUM'), "QUORUM read with n3 down")

    eng.shutdown()

    # ─────────────────────────────────────────────────────────────────────────
    # 5. Compaction — fill past L0 flush threshold
    # ─────────────────────────────────────────────────────────────────────────
    section("5. Compaction — fill past L0 flush threshold (2000 cells/file × 4)")
    eng = StrataEngine.single_node(os.path.join(base, 'compact'), consistency='ONE')
    eng.execute("""
        CREATE KEYSPACE metrics
        WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'}
    """)
    eng.execute("USE metrics")
    eng.execute("""
        CREATE TABLE readings (
            sensor_id TEXT,
            ts        TEXT,
            value     FLOAT,
            PRIMARY KEY (sensor_id, ts)
        )
    """)

    # Write 9000 rows — triggers 4 flushes and one L0→L1 compaction
    print("Writing 9000 rows …", end=' ', flush=True)
    for i in range(9000):
        sid = f"sensor_{i % 10}"
        ts  = f"{i:08d}"
        eng.execute(f"INSERT INTO readings (sensor_id, ts, value) "
                    f"VALUES ('{sid}', '{ts}', {i * 0.1:.1f})")
    print("done.")

    # Inspect L-levels by peeking at the LSM for sensor_0
    node = eng._cluster.get_node('node1')
    lsm  = node._get_lsm('metrics', 'readings')
    print(f"  L0 files: {len(lsm._levels[0])}")
    print(f"  L1 files: {len(lsm._levels[1])}")
    print(f"  L2 files: {len(lsm._levels[2])}")
    print(f"  Total cells in LSM: {lsm.cell_count()}")

    # Spot-check a value
    show(eng.execute(
        "SELECT * FROM readings WHERE sensor_id = 'sensor_0' AND ts = '00000000'"
    ), "spot check")

    eng.shutdown()

    # ─────────────────────────────────────────────────────────────────────────
    # 6. Consistency escalation — ALL requires all replicas live
    # ─────────────────────────────────────────────────────────────────────────
    section("6. Consistency levels — ONE / QUORUM / ALL")
    eng = StrataEngine.cluster(
        os.path.join(base, 'cl'),
        node_ids=['a', 'b', 'c'],
        replication_factor=3,
        consistency='QUORUM',
        start_gossip=False,
    )
    eng.execute("""
        CREATE KEYSPACE demo
        WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '3'}
    """)
    eng.execute("USE demo")
    eng.execute("CREATE TABLE kv (k TEXT, v TEXT, PRIMARY KEY (k))")
    eng.execute("INSERT INTO kv (k, v) VALUES ('x', 'hello')")

    for cl in ('ONE', 'QUORUM', 'ALL'):
        r = eng.execute("SELECT * FROM kv WHERE k = 'x'", consistency=cl)
        print(f"  {cl:6s} → {r.rows}")

    # Take node 'c' down; ALL should fail, QUORUM should succeed
    eng._cluster.get_node('c')._alive = False
    from strata.consistency import WriteError, ReadError
    print("\n  node 'c' is now DOWN:")
    for cl in ('ONE', 'QUORUM', 'ALL'):
        try:
            r = eng.execute("SELECT * FROM kv WHERE k = 'x'", consistency=cl)
            print(f"  {cl:6s} → ok ({len(r.rows)} rows)")
        except (ReadError, Exception) as e:
            print(f"  {cl:6s} → FAILED: {e}")

    eng.shutdown()

    # ─────────────────────────────────────────────────────────────────────────
    # 7. Schema introspection
    # ─────────────────────────────────────────────────────────────────────────
    section("7. Schema introspection — DESCRIBE")
    eng = StrataEngine.single_node(os.path.join(base, 'desc'), consistency='ONE')
    eng.execute("""
        CREATE KEYSPACE catalog_demo
        WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'}
    """)
    eng.execute("USE catalog_demo")
    eng.execute("""
        CREATE TABLE orders (
            customer_id TEXT,
            order_id    TEXT,
            total       FLOAT,
            status      TEXT,
            PRIMARY KEY (customer_id, order_id)
        )
    """)

    print("\nDESCRIBE KEYSPACES:")
    show(eng.execute("DESCRIBE KEYSPACES"), "ks")

    print("\nDESCRIBE TABLES:")
    show(eng.execute("DESCRIBE TABLES"), "tables")

    print("\nDESCRIBE TABLE orders:")
    show(eng.execute("DESCRIBE TABLE orders"), "schema")

    eng.shutdown()

    # ─────────────────────────────────────────────────────────────────────────
    # 8. TRUNCATE
    # ─────────────────────────────────────────────────────────────────────────
    section("8. TRUNCATE")
    eng = StrataEngine.single_node(os.path.join(base, 'trunc'), consistency='ONE')
    eng.execute("""
        CREATE KEYSPACE trunc_ks
        WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'}
    """)
    eng.execute("USE trunc_ks")
    eng.execute("CREATE TABLE tmp (id TEXT, val TEXT, PRIMARY KEY (id))")
    for i in range(5):
        eng.execute(f"INSERT INTO tmp (id, val) VALUES ('k{i}', 'v{i}')")
    show(eng.execute("SELECT * FROM tmp WHERE id = 'k0'"), "before TRUNCATE")
    eng.execute("TRUNCATE tmp")
    r = eng.execute("SELECT * FROM tmp WHERE id = 'k0'")
    print(f"[after TRUNCATE] rows: {len(r.rows)}  (expected 0)")
    eng.shutdown()

    print(f"\n{DIVIDER}")
    print("  All Strata demo sections completed successfully.")
    print(DIVIDER)


if __name__ == '__main__':
    main()
