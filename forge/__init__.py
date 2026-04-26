"""
Forge — a PostgreSQL-inspired relational database engine built from scratch.

Features: page-based heap storage, WAL, B+ tree indexes, MVCC transactions,
buffer pool, SQL parser, query planner, aggregate functions, REPL.
"""
from .engine import DatabaseEngine, Connection, QueryResult

__all__ = ['DatabaseEngine', 'Connection', 'QueryResult']
