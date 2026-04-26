# database-py

Three database engines built from scratch in Python — no external database libraries.

| Engine | Inspired by | Storage model | Protocol |
|--------|-------------|---------------|----------|
| **Forge** | PostgreSQL | Page-based heap + B+ tree | SQL |
| **Strata** | Apache Cassandra | LSM tree + SSTables | CQL-like |
| **Volt** | Redis | In-memory skip list + AOF | RESP2 |

---

## Forge — PostgreSQL-like Relational Engine

### Architecture

```
SQL string
    └─► Parser (recursive descent)
            └─► Planner (cost-based: SeqScan vs IndexScan, nested-loop join)
                    └─► Executor
                            ├─► Buffer Pool (LRU, 512 pages × 8 KB)
                            ├─► Heap (slotted page, page 0 = header)
                            ├─► B+ Tree Index (order 64, linked leaf nodes)
                            ├─► WAL (fsync on commit)
                            ├─► MVCC (xmin/xmax, Read Committed)
                            └─► Lock Manager (row-level S/X, deadlock via wait-for graph)
```

### Features

- `CREATE TABLE`, `DROP TABLE`, `CREATE INDEX`
- `INSERT`, `UPDATE`, `DELETE`, `SELECT`
- `WHERE` with `AND`/`OR`, `IS NULL`, `LIKE`, `BETWEEN`, `IN`, comparison operators
- `JOIN` (nested-loop), `ORDER BY`, `LIMIT`
- Aggregates: `COUNT`, `SUM`, `AVG`, `MIN`, `MAX`, `GROUP BY`, `HAVING`
- Explicit transactions: `BEGIN`, `COMMIT`, `ROLLBACK`
- WAL-based crash recovery on startup
- REPL meta-commands: `\d <table>`, `\dt`, `\q`

### Run

```bash
python3 -m forge                    # interactive REPL (data in /tmp/forge_data)
python3 -m forge ./my_data_dir      # custom data directory
```

### Screenshots

![Forge REPL — DDL, DML, aggregates](images/Screenshot%202026-04-25%20at%203.30.11%20PM.png)

![Forge REPL — DELETE, transactions, ROLLBACK, meta-commands](images/Screenshot%202026-04-25%20at%203.30.20%20PM.png)

### Example queries

```sql
CREATE TABLE users (id INT, name TEXT, age INT, email TEXT);
CREATE INDEX idx_age ON users (age);

INSERT INTO users VALUES (1, 'Alice', 30, 'alice@example.com');
INSERT INTO users VALUES (2, 'Bob',   25, NULL);

SELECT * FROM users WHERE age > 20 ORDER BY age DESC;
SELECT name, age FROM users WHERE email IS NULL;
SELECT name FROM users WHERE name LIKE 'A%';

SELECT COUNT(*), AVG(age), MAX(age) FROM users;

BEGIN;
INSERT INTO users VALUES (3, 'Charlie', 35, 'c@example.com');
ROLLBACK;

\d users
\dt
```

---

## Strata — Cassandra-like Wide-Column Engine

### Architecture

```
CQL string
    └─► Parser
            └─► Executor
                    └─► StrataEngine / StrataCluster
                            ├─► Coordinator (consistent hashing, quorum logic)
                            ├─► LSM Tree
                            │       ├─► MemTable (sorted dict)
                            │       ├─► SSTable (binary, sparse index, CRC32)
                            │       └─► Compaction (size-tiered L0, leveled L1/L2)
                            ├─► BloomFilter (SHA-256 double hashing)
                            ├─► Commit Log (CRC32, replay on recovery)
                            └─► Gossip (SUSPECT @ 3 s, DOWN @ 8 s, 150 vnodes/node)
```

### Features

- `CREATE KEYSPACE`, `CREATE TABLE` (partition keys + clustering keys)
- `INSERT INTO … VALUES … USING TIMESTAMP`
- `SELECT` with partition-key equality, clustering-key range, `ORDER BY`, `LIMIT`
- `UPDATE`, `DELETE`, `TRUNCATE`, `DROP TABLE`
- `DESCRIBE KEYSPACES`, `DESCRIBE TABLE`
- Consistency levels: `ONE`, `QUORUM`, `ALL`
- Replication factor per keyspace
- Multi-node cluster mode with virtual nodes and gossip-based failure detection
- REPL meta-commands: `\s` (cluster status), `USE <keyspace>`, `\q`

### Run

```bash
# Single node
python3 -m strata

# 3-node cluster, RF=3, QUORUM reads/writes
python3 -m strata --cluster n1,n2,n3 --rf 3 --consistency QUORUM
```

### Screenshots

![Strata single-node — keyspace, table, CRUD, DESCRIBE](images/Screenshot%202026-04-25%20at%209.13.34%20PM.png)

![Strata 3-node cluster — QUORUM consistency, cluster status](images/Screenshot%202026-04-25%20at%209.13.59%20PM.png)

### Example queries

```cql
CREATE KEYSPACE ks WITH replication_factor = 1;
USE ks;

CREATE TABLE events (
    user_id TEXT,
    event_time TEXT,
    action TEXT,
    PRIMARY KEY (user_id, event_time)
);

INSERT INTO events (user_id, event_time, action) VALUES ('u1', '2024-01-01', 'login');
INSERT INTO events (user_id, event_time, action) VALUES ('u1', '2024-01-02', 'purchase');

SELECT * FROM events WHERE user_id = 'u1';
SELECT * FROM events WHERE user_id = 'u1' AND event_time > '2024-01-01';
SELECT * FROM events WHERE user_id = 'u1' ORDER BY event_time DESC LIMIT 1;

UPDATE events SET action = 'logout' WHERE user_id = 'u1' AND event_time = '2024-01-02';
DELETE FROM events WHERE user_id = 'u2' AND event_time = '2024-01-01';

DESCRIBE KEYSPACES;
DESCRIBE TABLE events;
\s
```

---

## Volt — Redis-like In-Memory Store

### Architecture

```
RESP2 command
    └─► Command dispatcher (VoltDB)
            ├─► String  — byte string with TTL
            ├─► List    — Python deque
            ├─► Hash    — dict
            ├─► Set     — Python set
            ├─► ZSet    — Skip list (O(log n) rank/range, max level 32, p=0.25)
            ├─► 16 logical databases (SELECT 0..15)
            ├─► TTL expiry (lazy + active background sweep)
            ├─► AOF persistence (always / everysec / no)
            └─► Pub/Sub (thread-safe callbacks)
```

### Features

**Strings:** `SET`, `GET`, `DEL`, `EXISTS`, `INCR`, `INCRBY`, `DECR`, `DECRBY`, `APPEND`, `STRLEN`, `MSET`, `MGET`, `SETNX`, `GETSET`, `SETEX`, `TTL`, `EXPIRE`, `PERSIST`

**Lists:** `LPUSH`, `RPUSH`, `LPOP`, `RPOP`, `LRANGE`, `LLEN`, `LINDEX`, `LSET`, `LINSERT`, `LREM`

**Hashes:** `HSET`, `HGET`, `HMSET`, `HMGET`, `HGETALL`, `HDEL`, `HEXISTS`, `HKEYS`, `HVALS`, `HLEN`, `HINCRBY`

**Sets:** `SADD`, `SREM`, `SMEMBERS`, `SISMEMBER`, `SCARD`, `SUNION`, `SINTER`, `SDIFF`

**Sorted Sets:** `ZADD`, `ZREM`, `ZSCORE`, `ZRANK`, `ZRANGE`, `ZREVRANGE`, `ZRANGEBYSCORE`, `ZCOUNT`, `ZCARD`, `ZINCRBY`

**Server:** `PING`, `ECHO`, `INFO`, `KEYS`, `TYPE`, `RENAME`, `RANDOMKEY`, `DBSIZE`, `SELECT`, `FLUSHDB`, `FLUSHALL`, `SAVE`, `BGSAVE`

**Pub/Sub:** `SUBSCRIBE`, `UNSUBSCRIBE`, `PUBLISH`

### Run

```bash
# Interactive in-process shell
python3 -m volt

# TCP server (RESP2, port 6399)
python3 -m volt --server
python3 -m volt --server --port 6399 --aof ./volt.aof --aof-fsync everysec

# Connect with redis-cli
redis-cli -p 6399
```

### Screenshots

![Volt shell — strings, INCR, APPEND, PING, INFO, KEYS](images/Screenshot%202026-04-25%20at%209.36.53%20PM.png)

![Volt TCP server — listening on port 6399](images/Screenshot%202026-04-25%20at%209.37.09%20PM.png)

### Example commands

```
SET name "Alice"
GET name
INCR counter
APPEND name " Smith"
SETEX session 3600 "token-abc"
TTL session

LPUSH queue "job1" "job2" "job3"
LRANGE queue 0 -1
RPOP queue

HSET user:1 name "Bob" age "25"
HGETALL user:1
HINCRBY user:1 age 1

SADD tags "python" "db" "redis"
SMEMBERS tags

ZADD leaderboard 100 "alice" 200 "bob" 150 "charlie"
ZREVRANGE leaderboard 0 -1 WITHSCORES
ZRANGEBYSCORE leaderboard 100 200

SUBSCRIBE news
PUBLISH news "breaking story"

SELECT 1
FLUSHDB
```

---

## Tests

```bash
# Run all tests
python3 -m pytest forge/tests/ strata/tests/ volt/tests/ -v

# Per engine
python3 -m pytest forge/tests/   -v   # 102 tests
python3 -m pytest strata/tests/  -v   # 69 tests
python3 -m pytest volt/tests/    -v   # 99 tests
```

---

## Project structure

```
database-py/
├── forge/              # PostgreSQL-like relational engine
│   ├── catalog/        # Schema & metadata
│   ├── query/          # Parser, planner, executor
│   ├── storage/        # Heap pages, buffer pool, B+ tree, WAL
│   ├── transaction/    # MVCC, lock manager, transaction manager
│   └── tests/
├── strata/             # Cassandra-like wide-column engine
│   ├── query/          # CQL parser & executor
│   ├── storage/        # LSM tree, MemTable, SSTable, BloomFilter, commit log
│   ├── consistency.py  # Coordinator, replication, quorum
│   ├── engine.py       # StrataNode, StrataCluster, StrataEngine
│   └── tests/
├── volt/               # Redis-like in-memory store
│   ├── types/          # Skip list
│   ├── protocol/       # RESP2 encoder/decoder
│   ├── persistence/    # AOF
│   ├── commands.py     # ~80 commands
│   ├── engine.py       # VoltDB dispatcher, 16 DBs, TTL expiry
│   ├── server.py       # asyncio TCP server
│   └── tests/
└── images/             # REPL screenshots
```
