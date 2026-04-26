"""
Volt — a Redis-compatible in-memory data store built from scratch.

Features: Strings, Lists, Hashes, Sets, Sorted Sets (skip list),
TTL/expiry, AOF persistence, Pub/Sub, RESP2 server, multi-DB.

    from volt.engine import VoltDB

    db = VoltDB()
    db.execute(['SET', 'key', 'value'])
    db.execute(['GET', 'key'])
"""
from .engine import VoltDB
__all__ = ['VoltDB']
