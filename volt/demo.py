"""
Volt demo — exercises every major feature:

  1.  Strings  : SET / GET / INCR / APPEND / GETRANGE
  2.  Expiry   : EXPIRE / TTL / PTTL / PERSIST / active eviction
  3.  Lists    : LPUSH / RPUSH / LRANGE / LPOP / RPOP / LREM
  4.  Hashes   : HSET / HGETALL / HINCRBY / HDEL
  5.  Sets     : SADD / SMEMBERS / SINTER / SUNION / SDIFF
  6.  Sorted sets (ZSet / skip list) : ZADD / ZRANGE / ZRANGEBYSCORE /
                  ZREVRANGE / ZRANK / ZINCRBY / ZPOPMIN
  7.  Pub/Sub  : PUBLISH / SUBSCRIBE (async delivery)
  8.  AOF      : write → restart → verify state survived
  9.  Multi-DB : SELECT 0 / SELECT 1 isolation
  10. Keys API : KEYS / SCAN / RENAME / TYPE / DEL
"""
from __future__ import annotations

import os
import queue
import shutil
import tempfile
import threading
import time

from volt.engine import VoltDB

DIVIDER = '─' * 60


def section(title: str):
    print(f"\n{DIVIDER}\n  {title}\n{DIVIDER}")


def show(label: str, raw: bytes):
    from volt.repl import _decode_resp_for_print
    print(f"  {label:<30s} → {_decode_resp_for_print(raw)}")


def run(db: VoltDB, *cmd: str) -> bytes:
    return db.execute(list(cmd))


def main():
    tmpdir = tempfile.mkdtemp(prefix='volt_demo_')
    try:
        _run_demo(tmpdir)
    finally:
        shutil.rmtree(tmpdir, ignore_errors=True)


def _run_demo(base: str):

    # ── 1. Strings ────────────────────────────────────────────────────────────
    section("1. Strings")
    db = VoltDB()

    run(db, 'SET', 'name', 'Volt')
    show("GET name",          run(db, 'GET', 'name'))
    run(db, 'SET', 'count', '10')
    show("INCR count",        run(db, 'INCR', 'count'))
    show("INCRBY count 5",    run(db, 'INCRBY', 'count', '5'))
    show("INCRBYFLOAT pi 3.14", run(db, 'SET', 'pi', '0') or run(db, 'INCRBYFLOAT', 'pi', '3.14'))
    run(db, 'SET', 'greeting', 'Hello')
    show("APPEND greeting",   run(db, 'APPEND', 'greeting', ', World!'))
    show("GETRANGE 0..4",     run(db, 'GETRANGE', 'greeting', '0', '4'))
    show("STRLEN greeting",   run(db, 'STRLEN', 'greeting'))
    show("MGET name count",   run(db, 'MGET', 'name', 'count'))
    run(db, 'MSET', 'a', '1', 'b', '2', 'c', '3')
    show("MGET a b c",        run(db, 'MGET', 'a', 'b', 'c'))
    db.shutdown()

    # ── 2. Expiry ─────────────────────────────────────────────────────────────
    section("2. Expiry")
    db = VoltDB()

    run(db, 'SET', 'temp', 'expires soon')
    run(db, 'EXPIRE', 'temp', '2')
    show("TTL temp (2s)",     run(db, 'TTL', 'temp'))
    show("PTTL temp (ms)",    run(db, 'PTTL', 'temp'))
    show("GET temp (exists)", run(db, 'GET', 'temp'))
    time.sleep(2.1)
    show("GET temp (expired)",run(db, 'GET', 'temp'))
    show("TTL temp (gone)",   run(db, 'TTL', 'temp'))

    run(db, 'SET', 'sticky', 'will persist')
    run(db, 'EXPIRE', 'sticky', '100')
    run(db, 'PERSIST', 'sticky')
    show("TTL sticky (persisted)", run(db, 'TTL', 'sticky'))
    db.shutdown()

    # ── 3. Lists ──────────────────────────────────────────────────────────────
    section("3. Lists")
    db = VoltDB()

    run(db, 'RPUSH', 'queue', 'job1', 'job2', 'job3')
    run(db, 'LPUSH', 'queue', 'priority')
    show("LRANGE queue 0 -1", run(db, 'LRANGE', 'queue', '0', '-1'))
    show("LLEN queue",        run(db, 'LLEN', 'queue'))
    show("LPOP queue",        run(db, 'LPOP', 'queue'))
    show("RPOP queue",        run(db, 'RPOP', 'queue'))
    show("LINDEX queue 0",    run(db, 'LINDEX', 'queue', '0'))
    run(db, 'RPUSH', 'dups', 'a', 'b', 'a', 'c', 'a')
    show("LREM dups 2 a",     run(db, 'LREM', 'dups', '2', 'a'))
    show("LRANGE dups 0 -1",  run(db, 'LRANGE', 'dups', '0', '-1'))
    db.shutdown()

    # ── 4. Hashes ─────────────────────────────────────────────────────────────
    section("4. Hashes")
    db = VoltDB()

    run(db, 'HSET', 'user:1', 'name', 'Alice', 'age', '30', 'city', 'NYC')
    show("HGET user:1 name",  run(db, 'HGET', 'user:1', 'name'))
    show("HGETALL user:1",    run(db, 'HGETALL', 'user:1'))
    show("HINCRBY age 1",     run(db, 'HINCRBY', 'user:1', 'age', '1'))
    show("HLEN user:1",       run(db, 'HLEN', 'user:1'))
    show("HKEYS user:1",      run(db, 'HKEYS', 'user:1'))
    show("HVALS user:1",      run(db, 'HVALS', 'user:1'))
    show("HEXISTS user:1 age",run(db, 'HEXISTS', 'user:1', 'age'))
    run(db, 'HDEL', 'user:1', 'city')
    show("HGETALL (after del)",run(db, 'HGETALL', 'user:1'))
    db.shutdown()

    # ── 5. Sets ───────────────────────────────────────────────────────────────
    section("5. Sets")
    db = VoltDB()

    run(db, 'SADD', 'python_devs', 'alice', 'bob', 'carol')
    run(db, 'SADD', 'js_devs',    'bob', 'dave', 'eve')
    show("SMEMBERS python_devs",  run(db, 'SMEMBERS', 'python_devs'))
    show("SCARD python_devs",     run(db, 'SCARD', 'python_devs'))
    show("SISMEMBER alice",       run(db, 'SISMEMBER', 'python_devs', 'alice'))
    show("SISMEMBER dave",        run(db, 'SISMEMBER', 'python_devs', 'dave'))
    show("SINTER (both langs)",   run(db, 'SINTER', 'python_devs', 'js_devs'))
    show("SUNION (any lang)",     run(db, 'SUNION', 'python_devs', 'js_devs'))
    show("SDIFF py-js",           run(db, 'SDIFF', 'python_devs', 'js_devs'))
    db.shutdown()

    # ── 6. Sorted Sets ────────────────────────────────────────────────────────
    section("6. Sorted Sets (ZSet / skip list)")
    db = VoltDB()

    scores = [('alice', 95.5), ('bob', 87.0), ('carol', 91.0),
              ('dave', 78.5), ('eve', 99.0)]
    for member, score in scores:
        run(db, 'ZADD', 'leaderboard', str(score), member)

    show("ZCARD",             run(db, 'ZCARD', 'leaderboard'))
    show("ZRANGE 0 -1 WS",   run(db, 'ZRANGE', 'leaderboard', '0', '-1', 'WITHSCORES'))
    show("ZREVRANGE 0 2 WS", run(db, 'ZREVRANGE', 'leaderboard', '0', '2', 'WITHSCORES'))
    show("ZRANK alice",       run(db, 'ZRANK', 'leaderboard', 'alice'))
    show("ZREVRANK alice",    run(db, 'ZREVRANK', 'leaderboard', 'alice'))
    show("ZSCORE eve",        run(db, 'ZSCORE', 'leaderboard', 'eve'))
    show("ZRANGEBYSCORE 90 100 WS",
         run(db, 'ZRANGEBYSCORE', 'leaderboard', '90', '100', 'WITHSCORES'))
    show("ZINCRBY alice +5", run(db, 'ZINCRBY', 'leaderboard', '5', 'alice'))
    show("ZPOPMIN (last)",   run(db, 'ZPOPMIN', 'leaderboard'))
    show("ZCOUNT 85 100",    run(db, 'ZCOUNT', 'leaderboard', '85', '100'))

    # NX / XX flags
    run(db, 'ZADD', 'flags', '10', 'x')
    run(db, 'ZADD', 'flags', 'NX', '20', 'x')    # NX — should not update
    show("NX skips existing", run(db, 'ZSCORE', 'flags', 'x'))
    run(db, 'ZADD', 'flags', 'XX', '30', 'x')    # XX — updates existing
    show("XX updates",        run(db, 'ZSCORE', 'flags', 'x'))
    db.shutdown()

    # ── 7. Pub/Sub ────────────────────────────────────────────────────────────
    section("7. Pub/Sub")
    db = VoltDB()

    received: list = []
    ev = threading.Event()

    def _on_msg(ch, msg):
        received.append((ch, msg))
        ev.set()

    db.pubsub.subscribe('news', _on_msg)
    db.pubsub.subscribe('alerts', _on_msg)

    # PUBLISH from engine
    r1 = run(db, 'PUBLISH', 'news', 'Breaking: Volt ships!')
    ev.wait(timeout=0.5); ev.clear()
    r2 = run(db, 'PUBLISH', 'alerts', 'High memory usage')
    ev.wait(timeout=0.5); ev.clear()
    r3 = run(db, 'PUBLISH', 'unknown', 'nobody listens')
    ev.wait(timeout=0.1)

    show("PUBLISH news (1 rcvr)",    r1)
    show("PUBLISH alerts (1 rcvr)",  r2)
    show("PUBLISH unknown (0 rcvr)", r3)
    print(f"  Messages received: {received}")

    show("PUBSUB CHANNELS",          run(db, 'PUBSUB', 'CHANNELS'))
    show("PUBSUB NUMSUB news alerts",run(db, 'PUBSUB', 'NUMSUB', 'news', 'alerts'))
    db.pubsub.unsubscribe('news', _on_msg)
    db.pubsub.unsubscribe('alerts', _on_msg)
    db.shutdown()

    # ── 8. AOF persistence ────────────────────────────────────────────────────
    section("8. AOF — crash recovery")
    aof_path = os.path.join(base, 'volt.aof')

    db = VoltDB(aof_path=aof_path, fsync='always')
    run(db, 'SET', 'persisted_key', 'survives restart')
    run(db, 'RPUSH', 'persisted_list', 'a', 'b', 'c')
    run(db, 'HSET', 'persisted_hash', 'field', 'value')
    run(db, 'ZADD', 'persisted_zset', '1', 'alpha', '2', 'beta')
    db.shutdown()

    # Simulate restart: new VoltDB that replays AOF
    db2 = VoltDB(aof_path=aof_path, fsync='always')
    show("GET after restart",        run(db2, 'GET', 'persisted_key'))
    show("LRANGE list after restart",run(db2, 'LRANGE', 'persisted_list', '0', '-1'))
    show("HGET hash after restart",  run(db2, 'HGET', 'persisted_hash', 'field'))
    show("ZRANGE zset after restart",run(db2, 'ZRANGE', 'persisted_zset', '0', '-1', 'WITHSCORES'))
    db2.shutdown()

    # ── 9. Multi-DB ───────────────────────────────────────────────────────────
    section("9. Multi-DB isolation")
    db = VoltDB()

    run(db, 'SELECT', '0')
    run(db, 'SET', 'shared_key', 'db0_value')
    run(db, 'SELECT', '1')
    show("GET in DB1 (empty)",   run(db, 'GET', 'shared_key'))
    run(db, 'SET', 'shared_key', 'db1_value')
    run(db, 'SELECT', '0')
    show("GET in DB0 (db0_value)", run(db, 'GET', 'shared_key'))
    run(db, 'SELECT', '1')
    show("GET in DB1 (db1_value)", run(db, 'GET', 'shared_key'))
    db.shutdown()

    # ── 10. Keys API ──────────────────────────────────────────────────────────
    section("10. Keys API")
    db = VoltDB()

    run(db, 'SET', 'user:1', 'alice')
    run(db, 'SET', 'user:2', 'bob')
    run(db, 'SET', 'session:abc', 'token')
    run(db, 'LPUSH', 'mylist', 'x')
    run(db, 'HSET', 'myhash', 'f', 'v')

    show("KEYS user:*",    run(db, 'KEYS', 'user:*'))
    show("KEYS *",         run(db, 'KEYS', '*'))
    show("EXISTS user:1",  run(db, 'EXISTS', 'user:1'))
    show("TYPE user:1",    run(db, 'TYPE', 'user:1'))
    show("TYPE mylist",    run(db, 'TYPE', 'mylist'))
    show("TYPE myhash",    run(db, 'TYPE', 'myhash'))
    show("RENAME user:1",  run(db, 'RENAME', 'user:1', 'user:one'))
    show("GET user:one",   run(db, 'GET', 'user:one'))
    show("EXISTS user:1",  run(db, 'EXISTS', 'user:1'))
    show("DBSIZE",         run(db, 'DBSIZE'))
    show("SCAN 0",         run(db, 'SCAN', '0'))
    show("DEL multi",      run(db, 'DEL', 'user:one', 'user:2', 'session:abc'))
    show("DBSIZE after DEL", run(db, 'DBSIZE'))
    db.shutdown()

    print(f"\n{DIVIDER}")
    print("  All Volt demo sections completed successfully.")
    print(DIVIDER)


if __name__ == '__main__':
    main()
