"""
Strata — a Cassandra-inspired distributed storage engine built from scratch.

    from strata.engine import StrataEngine

    engine = StrataEngine.single_node('/tmp/strata')
    engine.execute("CREATE KEYSPACE ks WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'}")
    engine.execute("USE ks")
    engine.execute("CREATE TABLE t (id TEXT, name TEXT, PRIMARY KEY (id))")
    engine.execute("INSERT INTO t (id, name) VALUES ('1', 'alice')")
    result = engine.execute("SELECT * FROM t WHERE id = '1'")
"""
from .engine import StrataEngine, StrataCluster, StrataNode

__all__ = ['StrataEngine', 'StrataCluster', 'StrataNode']
