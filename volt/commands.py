"""
Volt command implementations.

Each command is a function that receives (db, args) and returns a RESP-encoded
bytes response.  `db` is the VoltDB instance that holds all in-memory state.

Commands are registered in the COMMANDS dict at the bottom of this module
so the dispatcher (engine.py) can look them up by name.
"""
from __future__ import annotations

import fnmatch
import math
import time
from typing import TYPE_CHECKING

from .protocol import resp
from .types import SkipList, VOLT_STRING, VOLT_LIST, VOLT_HASH, VOLT_SET, VOLT_ZSET

if TYPE_CHECKING:
    from .engine import VoltDB


# ── helpers ───────────────────────────────────────────────────────────────────

def _str_val(db: 'VoltDB', key: str):
    """Return string value or None; error-bytes if wrong type."""
    v = db.get(key)
    if v is None:
        return None, None
    if db.type_of(key) != VOLT_STRING:
        return None, resp.wrong_type()
    return v, None


def _check_type(db: 'VoltDB', key: str, expected: str):
    t = db.type_of(key)
    if t is not None and t != expected:
        return resp.wrong_type()
    return None


def _float(s: str):
    try:
        return float(s), None
    except ValueError:
        return None, resp.error("not a float value")


def _int(s: str):
    try:
        return int(s), None
    except ValueError:
        return None, resp.error("value is not an integer or out of range")


# ═══════════════════════════════════════════════════════════════════════════════
# String commands
# ═══════════════════════════════════════════════════════════════════════════════

def cmd_set(db, args):
    # SET key value [EX seconds] [PX ms] [NX] [XX] [GET]
    if len(args) < 2:
        return resp.error("wrong number of arguments")
    key, value = args[0], args[1]
    ex = px = None
    nx = xx = get = False
    i = 2
    while i < len(args):
        opt = args[i].upper()
        if opt == 'EX':
            i += 1
            n, err = _int(args[i])
            if err: return err
            ex = n
        elif opt == 'PX':
            i += 1
            n, err = _int(args[i])
            if err: return err
            px = n
        elif opt == 'NX':
            nx = True
        elif opt == 'XX':
            xx = True
        elif opt == 'GET':
            get = True
        i += 1

    existing = db.get(key)
    get_resp = resp.bulk(str(existing) if existing is not None else None) if get else None

    if nx and existing is not None:
        return get_resp or resp.nil_bulk()
    if xx and existing is None:
        return get_resp or resp.nil_bulk()

    db.put(key, value, VOLT_STRING)
    if ex:
        db.expire(key, ex)
    elif px:
        db.pexpire(key, px)

    return get_resp or resp.ok()


def cmd_get(db, args):
    if len(args) < 1:
        return resp.error("wrong number of arguments")
    v, err = _str_val(db, args[0])
    if err: return err
    return resp.bulk(str(v) if v is not None else None)


def cmd_getdel(db, args):
    if len(args) < 1:
        return resp.error("wrong number of arguments")
    v, err = _str_val(db, args[0])
    if err: return err
    if v is not None:
        db.delete(args[0])
    return resp.bulk(str(v) if v is not None else None)


def cmd_getset(db, args):
    if len(args) < 2:
        return resp.error("wrong number of arguments")
    old, err = _str_val(db, args[0])
    if err: return err
    db.put(args[0], args[1], VOLT_STRING)
    return resp.bulk(str(old) if old is not None else None)


def cmd_mset(db, args):
    if len(args) < 2 or len(args) % 2 != 0:
        return resp.error("wrong number of arguments")
    for i in range(0, len(args), 2):
        db.put(args[i], args[i+1], VOLT_STRING)
    return resp.ok()


def cmd_mget(db, args):
    results = []
    for key in args:
        v, err = _str_val(db, key)
        results.append(str(v) if v is not None and not err else None)
    return resp.array(results)


def cmd_setnx(db, args):
    if len(args) < 2:
        return resp.error("wrong number of arguments")
    if db.get(args[0]) is not None:
        return resp.integer(0)
    db.put(args[0], args[1], VOLT_STRING)
    return resp.integer(1)


def cmd_incr(db, args):
    return _incr_by(db, args[0], 1)


def cmd_decr(db, args):
    return _incr_by(db, args[0], -1)


def cmd_incrby(db, args):
    if len(args) < 2:
        return resp.error("wrong number of arguments")
    n, err = _int(args[1])
    if err: return err
    return _incr_by(db, args[0], n)


def cmd_decrby(db, args):
    if len(args) < 2:
        return resp.error("wrong number of arguments")
    n, err = _int(args[1])
    if err: return err
    return _incr_by(db, args[0], -n)


def _incr_by(db, key, delta):
    v = db.get(key)
    if v is None:
        v = 0
    elif db.type_of(key) != VOLT_STRING:
        return resp.wrong_type()
    n, err = _int(str(v))
    if err:
        return resp.error("value is not an integer or out of range")
    n += delta
    db.put(key, str(n), VOLT_STRING)
    return resp.integer(n)


def cmd_incrbyfloat(db, args):
    if len(args) < 2:
        return resp.error("wrong number of arguments")
    v = db.get(args[0])
    if v is None:
        v = '0'
    elif db.type_of(args[0]) != VOLT_STRING:
        return resp.wrong_type()
    cur, err = _float(str(v))
    if err: return err
    delta, err = _float(args[1])
    if err: return err
    result = cur + delta
    s = f'{result:g}'
    db.put(args[0], s, VOLT_STRING)
    return resp.bulk(s)


def cmd_append(db, args):
    if len(args) < 2:
        return resp.error("wrong number of arguments")
    v, err = _str_val(db, args[0])
    if err: return err
    new_val = (str(v) if v is not None else '') + args[1]
    db.put(args[0], new_val, VOLT_STRING)
    return resp.integer(len(new_val.encode('utf-8')))


def cmd_strlen(db, args):
    if len(args) < 1:
        return resp.error("wrong number of arguments")
    v, err = _str_val(db, args[0])
    if err: return err
    return resp.integer(len(str(v).encode('utf-8')) if v is not None else 0)


def cmd_getrange(db, args):
    if len(args) < 3:
        return resp.error("wrong number of arguments")
    v, err = _str_val(db, args[0])
    if err: return err
    s = str(v) if v is not None else ''
    n = len(s)
    start, _ = _int(args[1]); stop, _ = _int(args[2])
    if start < 0: start = max(0, n + start)
    if stop  < 0: stop  = n + stop
    start = max(0, start); stop = min(stop, n - 1)
    if start > stop:
        return resp.bulk('')
    return resp.bulk(s[start:stop+1])


# ═══════════════════════════════════════════════════════════════════════════════
# Key / expiry commands
# ═══════════════════════════════════════════════════════════════════════════════

def cmd_del(db, args):
    return resp.integer(sum(1 for k in args if db.delete(k)))


def cmd_exists(db, args):
    return resp.integer(sum(1 for k in args if db.get(k) is not None))


def cmd_type(db, args):
    if len(args) < 1:
        return resp.error("wrong number of arguments")
    t = db.type_of(args[0])
    return resp.simple(t if t else 'none')


def cmd_keys(db, args):
    pattern = args[0] if args else '*'
    return resp.array([k for k in db.all_keys()
                       if fnmatch.fnmatch(k, pattern)])


def cmd_scan(db, args):
    cursor = int(args[0]) if args else 0
    pattern = '*'
    count   = 10
    i = 1
    while i < len(args):
        opt = args[i].upper()
        if opt == 'MATCH':
            i += 1; pattern = args[i]
        elif opt == 'COUNT':
            i += 1; count, _ = _int(args[i])
        i += 1
    all_keys = sorted(db.all_keys())
    start = cursor
    end   = min(start + count, len(all_keys))
    chunk = [k for k in all_keys[start:end] if fnmatch.fnmatch(k, pattern)]
    next_cursor = end if end < len(all_keys) else 0
    return resp.array([str(next_cursor), chunk])


def cmd_rename(db, args):
    if len(args) < 2:
        return resp.error("wrong number of arguments")
    src, dst = args[0], args[1]
    v = db.get(src)
    if v is None:
        return resp.error("no such key")
    t = db.type_of(src)
    db.delete(dst)
    db.put(dst, v, t)
    ttl = db.ttl(src)
    if ttl > 0:
        db.expire(dst, ttl)
    db.delete(src)
    return resp.ok()


def cmd_expire(db, args):
    if len(args) < 2:
        return resp.error("wrong number of arguments")
    n, err = _int(args[1])
    if err: return err
    if db.get(args[0]) is None:
        return resp.integer(0)
    db.expire(args[0], n)
    return resp.integer(1)


def cmd_pexpire(db, args):
    if len(args) < 2:
        return resp.error("wrong number of arguments")
    n, err = _int(args[1])
    if err: return err
    if db.get(args[0]) is None:
        return resp.integer(0)
    db.pexpire(args[0], n)
    return resp.integer(1)


def cmd_ttl(db, args):
    if len(args) < 1:
        return resp.error("wrong number of arguments")
    if db.get(args[0]) is None:
        return resp.integer(-2)
    return resp.integer(db.ttl(args[0]))


def cmd_pttl(db, args):
    if len(args) < 1:
        return resp.error("wrong number of arguments")
    if db.get(args[0]) is None:
        return resp.integer(-2)
    return resp.integer(db.pttl(args[0]))


def cmd_persist(db, args):
    if len(args) < 1:
        return resp.error("wrong number of arguments")
    return resp.integer(1 if db.persist(args[0]) else 0)


def cmd_expireat(db, args):
    if len(args) < 2:
        return resp.error("wrong number of arguments")
    n, err = _int(args[1])
    if err: return err
    if db.get(args[0]) is None:
        return resp.integer(0)
    db.expireat(args[0], n)
    return resp.integer(1)


def cmd_dbsize(db, args):
    return resp.integer(len(db.all_keys()))


def cmd_flushdb(db, args):
    db.flushdb()
    return resp.ok()


def cmd_flushall(db, args):
    db.flushall()
    return resp.ok()


# ═══════════════════════════════════════════════════════════════════════════════
# List commands
# ═══════════════════════════════════════════════════════════════════════════════

def cmd_lpush(db, args):
    if len(args) < 2:
        return resp.error("wrong number of arguments")
    err = _check_type(db, args[0], VOLT_LIST)
    if err: return err
    lst = db.get(args[0]) or []
    for v in args[1:]:
        lst.insert(0, v)
    db.put(args[0], lst, VOLT_LIST)
    return resp.integer(len(lst))


def cmd_rpush(db, args):
    if len(args) < 2:
        return resp.error("wrong number of arguments")
    err = _check_type(db, args[0], VOLT_LIST)
    if err: return err
    lst = db.get(args[0]) or []
    lst.extend(args[1:])
    db.put(args[0], lst, VOLT_LIST)
    return resp.integer(len(lst))


def cmd_lpop(db, args):
    err = _check_type(db, args[0], VOLT_LIST)
    if err: return err
    count = int(args[1]) if len(args) > 1 else None
    lst = db.get(args[0])
    if not lst:
        return resp.nil_bulk() if count is None else resp.nil_array()
    if count is None:
        val = lst.pop(0)
        db.put(args[0], lst, VOLT_LIST) if lst else db.delete(args[0])
        return resp.bulk(val)
    result = lst[:count]; del lst[:count]
    db.put(args[0], lst, VOLT_LIST) if lst else db.delete(args[0])
    return resp.array(result)


def cmd_rpop(db, args):
    err = _check_type(db, args[0], VOLT_LIST)
    if err: return err
    count = int(args[1]) if len(args) > 1 else None
    lst = db.get(args[0])
    if not lst:
        return resp.nil_bulk() if count is None else resp.nil_array()
    if count is None:
        val = lst.pop()
        db.put(args[0], lst, VOLT_LIST) if lst else db.delete(args[0])
        return resp.bulk(val)
    result = lst[-count:]; del lst[-count:]
    db.put(args[0], lst, VOLT_LIST) if lst else db.delete(args[0])
    return resp.array(list(reversed(result)))


def cmd_llen(db, args):
    err = _check_type(db, args[0], VOLT_LIST)
    if err: return err
    return resp.integer(len(db.get(args[0]) or []))


def cmd_lrange(db, args):
    if len(args) < 3:
        return resp.error("wrong number of arguments")
    err = _check_type(db, args[0], VOLT_LIST)
    if err: return err
    lst = db.get(args[0]) or []
    n = len(lst)
    start, _ = _int(args[1]); stop, _ = _int(args[2])
    if start < 0: start = max(0, n + start)
    if stop  < 0: stop  = n + stop
    return resp.array(lst[start:stop+1])


def cmd_lindex(db, args):
    if len(args) < 2:
        return resp.error("wrong number of arguments")
    err = _check_type(db, args[0], VOLT_LIST)
    if err: return err
    lst = db.get(args[0]) or []
    idx, _ = _int(args[1])
    try:
        return resp.bulk(lst[idx])
    except IndexError:
        return resp.nil_bulk()


def cmd_lset(db, args):
    if len(args) < 3:
        return resp.error("wrong number of arguments")
    err = _check_type(db, args[0], VOLT_LIST)
    if err: return err
    lst = db.get(args[0])
    if not lst:
        return resp.error("no such key")
    idx, _ = _int(args[1])
    try:
        lst[idx] = args[2]
    except IndexError:
        return resp.error("index out of range")
    db.put(args[0], lst, VOLT_LIST)
    return resp.ok()


def cmd_lrem(db, args):
    if len(args) < 3:
        return resp.error("wrong number of arguments")
    err = _check_type(db, args[0], VOLT_LIST)
    if err: return err
    lst = db.get(args[0]) or []
    count, _ = _int(args[1]); val = args[2]
    removed = 0
    if count == 0:
        new = [x for x in lst if x != val]
        removed = len(lst) - len(new)
    elif count > 0:
        new = []
        for x in lst:
            if x == val and removed < count:
                removed += 1
            else:
                new.append(x)
    else:
        new = []
        for x in reversed(lst):
            if x == val and removed < abs(count):
                removed += 1
            else:
                new.insert(0, x)
    db.put(args[0], new, VOLT_LIST) if new else db.delete(args[0])
    return resp.integer(removed)


# ═══════════════════════════════════════════════════════════════════════════════
# Hash commands
# ═══════════════════════════════════════════════════════════════════════════════

def cmd_hset(db, args):
    if len(args) < 3 or len(args) % 2 == 0:
        return resp.error("wrong number of arguments")
    err = _check_type(db, args[0], VOLT_HASH)
    if err: return err
    h = db.get(args[0]) or {}
    added = 0
    for i in range(1, len(args), 2):
        if args[i] not in h:
            added += 1
        h[args[i]] = args[i+1]
    db.put(args[0], h, VOLT_HASH)
    return resp.integer(added)


def cmd_hget(db, args):
    if len(args) < 2:
        return resp.error("wrong number of arguments")
    err = _check_type(db, args[0], VOLT_HASH)
    if err: return err
    h = db.get(args[0]) or {}
    return resp.bulk(h.get(args[1]))


def cmd_hmset(db, args):
    return cmd_hset(db, args)   # same logic


def cmd_hmget(db, args):
    if len(args) < 2:
        return resp.error("wrong number of arguments")
    err = _check_type(db, args[0], VOLT_HASH)
    if err: return err
    h = db.get(args[0]) or {}
    return resp.array([h.get(f) for f in args[1:]])


def cmd_hgetall(db, args):
    if len(args) < 1:
        return resp.error("wrong number of arguments")
    err = _check_type(db, args[0], VOLT_HASH)
    if err: return err
    h = db.get(args[0]) or {}
    out = []
    for k, v in h.items():
        out.append(k); out.append(v)
    return resp.array(out)


def cmd_hdel(db, args):
    if len(args) < 2:
        return resp.error("wrong number of arguments")
    err = _check_type(db, args[0], VOLT_HASH)
    if err: return err
    h = db.get(args[0]) or {}
    removed = sum(1 for f in args[1:] if h.pop(f, None) is not None)
    db.put(args[0], h, VOLT_HASH) if h else db.delete(args[0])
    return resp.integer(removed)


def cmd_hlen(db, args):
    err = _check_type(db, args[0], VOLT_HASH)
    if err: return err
    return resp.integer(len(db.get(args[0]) or {}))


def cmd_hkeys(db, args):
    err = _check_type(db, args[0], VOLT_HASH)
    if err: return err
    return resp.array(list((db.get(args[0]) or {}).keys()))


def cmd_hvals(db, args):
    err = _check_type(db, args[0], VOLT_HASH)
    if err: return err
    return resp.array(list((db.get(args[0]) or {}).values()))


def cmd_hexists(db, args):
    if len(args) < 2:
        return resp.error("wrong number of arguments")
    err = _check_type(db, args[0], VOLT_HASH)
    if err: return err
    return resp.integer(1 if args[1] in (db.get(args[0]) or {}) else 0)


def cmd_hincrby(db, args):
    if len(args) < 3:
        return resp.error("wrong number of arguments")
    err = _check_type(db, args[0], VOLT_HASH)
    if err: return err
    delta, err = _int(args[2])
    if err: return err
    h = db.get(args[0]) or {}
    cur, ierr = _int(str(h.get(args[1], 0)))
    if ierr: return resp.error("hash value is not an integer")
    h[args[1]] = str(cur + delta)
    db.put(args[0], h, VOLT_HASH)
    return resp.integer(cur + delta)


def cmd_hincrbyfloat(db, args):
    if len(args) < 3:
        return resp.error("wrong number of arguments")
    err = _check_type(db, args[0], VOLT_HASH)
    if err: return err
    delta, err = _float(args[2])
    if err: return err
    h = db.get(args[0]) or {}
    cur, ferr = _float(str(h.get(args[1], 0)))
    if ferr: return resp.error("hash value is not a float")
    result = cur + delta
    h[args[1]] = f'{result:g}'
    db.put(args[0], h, VOLT_HASH)
    return resp.bulk(f'{result:g}')


# ═══════════════════════════════════════════════════════════════════════════════
# Set commands
# ═══════════════════════════════════════════════════════════════════════════════

def cmd_sadd(db, args):
    if len(args) < 2:
        return resp.error("wrong number of arguments")
    err = _check_type(db, args[0], VOLT_SET)
    if err: return err
    s = db.get(args[0]) or set()
    before = len(s)
    s.update(args[1:])
    added = len(s) - before
    db.put(args[0], s, VOLT_SET)
    return resp.integer(added)


def cmd_srem(db, args):
    if len(args) < 2:
        return resp.error("wrong number of arguments")
    err = _check_type(db, args[0], VOLT_SET)
    if err: return err
    s = db.get(args[0]) or set()
    removed = sum(1 for m in args[1:] if m in s)
    s.difference_update(args[1:])
    db.put(args[0], s, VOLT_SET) if s else db.delete(args[0])
    return resp.integer(removed)


def cmd_smembers(db, args):
    err = _check_type(db, args[0], VOLT_SET)
    if err: return err
    return resp.array(sorted(db.get(args[0]) or set()))


def cmd_sismember(db, args):
    if len(args) < 2:
        return resp.error("wrong number of arguments")
    err = _check_type(db, args[0], VOLT_SET)
    if err: return err
    return resp.integer(1 if args[1] in (db.get(args[0]) or set()) else 0)


def cmd_smismember(db, args):
    if len(args) < 2:
        return resp.error("wrong number of arguments")
    err = _check_type(db, args[0], VOLT_SET)
    if err: return err
    s = db.get(args[0]) or set()
    return resp.array([1 if m in s else 0 for m in args[1:]])


def cmd_scard(db, args):
    err = _check_type(db, args[0], VOLT_SET)
    if err: return err
    return resp.integer(len(db.get(args[0]) or set()))


def cmd_sunion(db, args):
    result = set()
    for key in args:
        err = _check_type(db, key, VOLT_SET)
        if err: return err
        result |= (db.get(key) or set())
    return resp.array(sorted(result))


def cmd_sinter(db, args):
    sets = []
    for key in args:
        err = _check_type(db, key, VOLT_SET)
        if err: return err
        sets.append(db.get(key) or set())
    result = sets[0].intersection(*sets[1:]) if sets else set()
    return resp.array(sorted(result))


def cmd_sdiff(db, args):
    sets = []
    for key in args:
        err = _check_type(db, key, VOLT_SET)
        if err: return err
        sets.append(db.get(key) or set())
    result = sets[0].difference(*sets[1:]) if sets else set()
    return resp.array(sorted(result))


def cmd_spop(db, args):
    if len(args) < 1:
        return resp.error("wrong number of arguments")
    import random as _random
    err = _check_type(db, args[0], VOLT_SET)
    if err: return err
    s = db.get(args[0]) or set()
    count = int(args[1]) if len(args) > 1 else None
    if not s:
        return resp.nil_bulk() if count is None else resp.array([])
    if count is None:
        m = _random.choice(list(s))
        s.remove(m)
        db.put(args[0], s, VOLT_SET) if s else db.delete(args[0])
        return resp.bulk(m)
    chosen = _random.sample(list(s), min(count, len(s)))
    s.difference_update(chosen)
    db.put(args[0], s, VOLT_SET) if s else db.delete(args[0])
    return resp.array(chosen)


def cmd_sunionstore(db, args):
    if len(args) < 2:
        return resp.error("wrong number of arguments")
    result = set()
    for key in args[1:]:
        err = _check_type(db, key, VOLT_SET)
        if err: return err
        result |= (db.get(key) or set())
    db.put(args[0], result, VOLT_SET)
    return resp.integer(len(result))


def cmd_sinterstore(db, args):
    if len(args) < 2:
        return resp.error("wrong number of arguments")
    sets = [(db.get(k) or set()) for k in args[1:]]
    result = sets[0].intersection(*sets[1:]) if sets else set()
    db.put(args[0], result, VOLT_SET)
    return resp.integer(len(result))


# ═══════════════════════════════════════════════════════════════════════════════
# Sorted Set (ZSet) commands
# ═══════════════════════════════════════════════════════════════════════════════

def _get_zset(db, key) -> tuple:
    err = _check_type(db, key, VOLT_ZSET)
    if err:
        return None, err
    val = db.get(key)
    if val is None:
        val = SkipList()
        db.put(key, val, VOLT_ZSET)
    return val, None


def cmd_zadd(db, args):
    # ZADD key [NX|XX] [GT|LT] [CH] [INCR] score member [score member ...]
    if len(args) < 3:
        return resp.error("wrong number of arguments")
    key = args[0]
    err = _check_type(db, key, VOLT_ZSET)
    if err: return err

    flags = set()
    i = 1
    while i < len(args) and args[i].upper() in ('NX','XX','GT','LT','CH','INCR'):
        flags.add(args[i].upper())
        i += 1

    pairs = args[i:]
    if len(pairs) < 2 or len(pairs) % 2 != 0:
        return resp.error("syntax error")

    zset, err = _get_zset(db, key)
    if err: return err

    added = changed = 0
    last_score = None
    for j in range(0, len(pairs), 2):
        score_f, serr = _float(pairs[j])
        if serr: return serr
        member = pairs[j+1]
        old_score = zset.score(member)

        if 'NX' in flags and old_score is not None:
            continue
        if 'XX' in flags and old_score is None:
            continue
        if 'GT' in flags and old_score is not None and score_f <= old_score:
            continue
        if 'LT' in flags and old_score is not None and score_f >= old_score:
            continue

        if 'INCR' in flags:
            score_f = (old_score or 0) + score_f

        if old_score is None:
            added += 1
        elif old_score != score_f:
            changed += 1
        zset.insert(score_f, member)
        last_score = score_f

    db.put(key, zset, VOLT_ZSET)
    if 'INCR' in flags:
        return resp.bulk(f'{last_score:g}' if last_score is not None else None)
    return resp.integer(added + (changed if 'CH' in flags else 0))


def cmd_zrange(db, args):
    # ZRANGE key start stop [BYSCORE] [REV] [LIMIT offset count] [WITHSCORES]
    if len(args) < 3:
        return resp.error("wrong number of arguments")
    zset, err = _get_zset(db, args[0])
    if err: return err

    byscore = 'BYSCORE' in [a.upper() for a in args]
    rev     = 'REV'     in [a.upper() for a in args]
    ws      = 'WITHSCORES' in [a.upper() for a in args]
    offset = 0; count = -1
    for i, a in enumerate(args):
        if a.upper() == 'LIMIT' and i+2 < len(args):
            offset = int(args[i+1]); count = int(args[i+2])

    if byscore:
        lo_s, hi_s = args[1], args[2]
        lo_ex = lo_s.startswith('('); hi_ex = hi_s.startswith('(')
        lo = float(lo_s.lstrip('('))
        hi = float(hi_s.lstrip('('))
        if rev:
            results = zset.range_by_score(hi, lo, hi_ex, lo_ex, offset, count)
            results.reverse()
        else:
            results = zset.range_by_score(lo, hi, lo_ex, hi_ex, offset, count)
    else:
        start, _ = _int(args[1]); stop, _ = _int(args[2])
        results = zset.range_by_rank(start, stop)
        if rev:
            results.reverse()

    if ws:
        out = []
        for m, s in results:
            out.append(m); out.append(f'{s:g}')
        return resp.array(out)
    return resp.array([m for m, _ in results])


def cmd_zrevrange(db, args):
    if len(args) < 3:
        return resp.error("wrong number of arguments")
    zset, err = _get_zset(db, args[0])
    if err: return err
    start, _ = _int(args[1]); stop, _ = _int(args[2])
    ws = len(args) > 3 and args[3].upper() == 'WITHSCORES'
    n = len(zset)
    # Map rev-rank [start, stop] to ascending rank [n-1-stop, n-1-start]
    asc_start = n - 1 - (stop  if stop  >= 0 else n + stop)
    asc_stop  = n - 1 - (start if start >= 0 else n + start)
    results = list(reversed(zset.range_by_rank(asc_start, asc_stop)))
    if ws:
        out = []
        for m, s in results:
            out.append(m); out.append(f'{s:g}')
        return resp.array(out)
    return resp.array([m for m, _ in results])


def cmd_zrangebyscore(db, args):
    if len(args) < 3:
        return resp.error("wrong number of arguments")
    zset, err = _get_zset(db, args[0])
    if err: return err
    lo_s, hi_s = args[1], args[2]
    lo_ex = lo_s.startswith('('); hi_ex = hi_s.startswith('(')
    lo_s2 = lo_s.lstrip('(')
    hi_s2 = hi_s.lstrip('(')
    lo = -math.inf if lo_s2 == '-inf' else float(lo_s2)
    hi =  math.inf if hi_s2 == '+inf' else float(hi_s2)
    ws = 'WITHSCORES' in [a.upper() for a in args]
    offset = 0; count = -1
    for i, a in enumerate(args):
        if a.upper() == 'LIMIT' and i+2 < len(args):
            offset = int(args[i+1]); count = int(args[i+2])
    results = zset.range_by_score(lo, hi, lo_ex, hi_ex, offset, count)
    if ws:
        out = []
        for m, s in results:
            out.append(m); out.append(f'{s:g}')
        return resp.array(out)
    return resp.array([m for m, _ in results])


def cmd_zrevrangebyscore(db, args):
    if len(args) < 3:
        return resp.error("wrong number of arguments")
    zset, err = _get_zset(db, args[0])
    if err: return err
    hi_s, lo_s = args[1], args[2]   # reversed order for ZREVRANGEBYSCORE
    lo_ex = lo_s.startswith('('); hi_ex = hi_s.startswith('(')
    lo_s2 = lo_s.lstrip('(')
    hi_s2 = hi_s.lstrip('(')
    lo = -math.inf if lo_s2 == '-inf' else float(lo_s2)
    hi =  math.inf if hi_s2 == '+inf' else float(hi_s2)
    ws = 'WITHSCORES' in [a.upper() for a in args]
    results = list(reversed(zset.range_by_score(lo, hi, lo_ex, hi_ex)))
    if ws:
        out = []
        for m, s in results:
            out.append(m); out.append(f'{s:g}')
        return resp.array(out)
    return resp.array([m for m, _ in results])


def cmd_zrank(db, args):
    if len(args) < 2:
        return resp.error("wrong number of arguments")
    zset, err = _get_zset(db, args[0])
    if err: return err
    r = zset.rank(args[1])
    return resp.integer(r) if r is not None else resp.nil_bulk()


def cmd_zrevrank(db, args):
    if len(args) < 2:
        return resp.error("wrong number of arguments")
    zset, err = _get_zset(db, args[0])
    if err: return err
    r = zset.rank(args[1])
    if r is None:
        return resp.nil_bulk()
    return resp.integer(len(zset) - 1 - r)


def cmd_zscore(db, args):
    if len(args) < 2:
        return resp.error("wrong number of arguments")
    zset, err = _get_zset(db, args[0])
    if err: return err
    s = zset.score(args[1])
    return resp.bulk(f'{s:g}') if s is not None else resp.nil_bulk()


def cmd_zrem(db, args):
    if len(args) < 2:
        return resp.error("wrong number of arguments")
    zset, err = _get_zset(db, args[0])
    if err: return err
    removed = sum(1 for m in args[1:] if zset.remove(m))
    db.put(args[0], zset, VOLT_ZSET)
    return resp.integer(removed)


def cmd_zcard(db, args):
    zset, err = _get_zset(db, args[0])
    if err: return err
    return resp.integer(len(zset))


def cmd_zcount(db, args):
    if len(args) < 3:
        return resp.error("wrong number of arguments")
    zset, err = _get_zset(db, args[0])
    if err: return err
    lo_s, hi_s = args[1], args[2]
    lo_ex = lo_s.startswith('('); hi_ex = hi_s.startswith('(')
    lo_s2 = lo_s.lstrip('(')
    hi_s2 = hi_s.lstrip('(')
    lo = -math.inf if lo_s2 == '-inf' else float(lo_s2)
    hi =  math.inf if hi_s2 == '+inf' else float(hi_s2)
    return resp.integer(len(zset.range_by_score(lo, hi, lo_ex, hi_ex)))


def cmd_zincrby(db, args):
    if len(args) < 3:
        return resp.error("wrong number of arguments")
    zset, err = _get_zset(db, args[0])
    if err: return err
    delta, derr = _float(args[1])
    if derr: return derr
    old = zset.score(args[2]) or 0
    new_score = old + delta
    zset.insert(new_score, args[2])
    db.put(args[0], zset, VOLT_ZSET)
    return resp.bulk(f'{new_score:g}')


def cmd_zpopmin(db, args):
    zset, err = _get_zset(db, args[0])
    if err: return err
    count = int(args[1]) if len(args) > 1 else 1
    results = zset.range_by_rank(0, count - 1)
    for m, _ in results:
        zset.remove(m)
    db.put(args[0], zset, VOLT_ZSET)
    out = []
    for m, s in results:
        out.append(m); out.append(f'{s:g}')
    return resp.array(out)


def cmd_zpopmax(db, args):
    zset, err = _get_zset(db, args[0])
    if err: return err
    count = int(args[1]) if len(args) > 1 else 1
    results = list(reversed(zset.range_by_rank(-count, -1)))
    for m, _ in results:
        zset.remove(m)
    db.put(args[0], zset, VOLT_ZSET)
    out = []
    for m, s in results:
        out.append(m); out.append(f'{s:g}')
    return resp.array(out)


# ═══════════════════════════════════════════════════════════════════════════════
# Pub/Sub commands
# ═══════════════════════════════════════════════════════════════════════════════

def cmd_publish(db, args):
    if len(args) < 2:
        return resp.error("wrong number of arguments")
    n = db.pubsub.publish(args[0], args[1])
    return resp.integer(n)


def cmd_subscribe(db, args):
    # In-process subscribe: prints messages to stdout (real async subscribe
    # is handled by the TCP server layer directly)
    return resp.error("SUBSCRIBE requires a connected client")


def cmd_pubsub(db, args):
    if not args:
        return resp.error("wrong number of arguments")
    sub = args[0].upper()
    if sub == 'CHANNELS':
        pattern = args[1] if len(args) > 1 else None
        return resp.array(db.pubsub.channels(pattern))
    if sub == 'NUMSUB':
        d = db.pubsub.numsub(*args[1:])
        out = []
        for ch, n in d.items():
            out.append(ch); out.append(n)
        return resp.array(out)
    return resp.error(f"Unknown PUBSUB subcommand {args[0]!r}")


# ═══════════════════════════════════════════════════════════════════════════════
# Server commands
# ═══════════════════════════════════════════════════════════════════════════════

def cmd_ping(db, args):
    if args:
        return resp.bulk(args[0])
    return resp.simple('PONG')


def cmd_echo(db, args):
    if not args:
        return resp.error("wrong number of arguments")
    return resp.bulk(args[0])


def cmd_select(db, args):
    if not args:
        return resp.error("wrong number of arguments")
    idx, err = _int(args[0])
    if err: return err
    db.select(idx)
    return resp.ok()


def cmd_info(db, args):
    section = args[0].lower() if args else 'all'
    lines = [
        '# Server',
        'volt_version:1.0.0',
        f'uptime_in_seconds:{int(time.time() - db.start_time)}',
        '# Keyspace',
        f'db0:keys={len(db.all_keys())}',
    ]
    return resp.bulk('\r\n'.join(lines))


def cmd_save(db, args):
    db.save()
    return resp.ok()


def cmd_bgsave(db, args):
    import threading as _t
    _t.Thread(target=db.save, daemon=True).start()
    return resp.simple('Background saving started')


def cmd_config(db, args):
    if not args:
        return resp.error("wrong number of arguments")
    sub = args[0].upper()
    if sub == 'GET':
        param = args[1] if len(args) > 1 else '*'
        settings = {
            'maxmemory': '0',
            'save': '',
            'appendonly': 'yes' if db.aof else 'no',
        }
        out = []
        for k, v in settings.items():
            if fnmatch.fnmatch(k, param):
                out.append(k); out.append(v)
        return resp.array(out)
    if sub == 'SET':
        return resp.ok()   # no-op
    return resp.error(f"Unknown CONFIG subcommand {args[0]!r}")


def cmd_object(db, args):
    # OBJECT ENCODING key
    if not args or args[0].upper() != 'ENCODING':
        return resp.error("syntax error")
    key = args[1] if len(args) > 1 else ''
    t = db.type_of(key)
    enc = {'string': 'embstr', 'list': 'listpack', 'hash': 'ziplist',
           'set': 'listpack', 'zset': 'skiplist'}.get(t or '', 'none')
    return resp.bulk(enc)


def cmd_wait(db, args):
    return resp.integer(0)


def cmd_lolwut(db, args):
    return resp.bulk('Volt v1.0  ¯\\_(ツ)_/¯')


# ═══════════════════════════════════════════════════════════════════════════════
# Command registry
# ═══════════════════════════════════════════════════════════════════════════════

COMMANDS = {
    # String
    'SET': cmd_set,  'GET': cmd_get,  'GETDEL': cmd_getdel,
    'GETSET': cmd_getset, 'MSET': cmd_mset, 'MGET': cmd_mget,
    'SETNX': cmd_setnx, 'INCR': cmd_incr, 'DECR': cmd_decr,
    'INCRBY': cmd_incrby, 'DECRBY': cmd_decrby,
    'INCRBYFLOAT': cmd_incrbyfloat,
    'APPEND': cmd_append, 'STRLEN': cmd_strlen, 'GETRANGE': cmd_getrange,
    # Keys / expiry
    'DEL': cmd_del, 'EXISTS': cmd_exists, 'TYPE': cmd_type,
    'KEYS': cmd_keys, 'SCAN': cmd_scan, 'RENAME': cmd_rename,
    'EXPIRE': cmd_expire, 'PEXPIRE': cmd_pexpire,
    'EXPIREAT': cmd_expireat,
    'TTL': cmd_ttl, 'PTTL': cmd_pttl, 'PERSIST': cmd_persist,
    'DBSIZE': cmd_dbsize, 'FLUSHDB': cmd_flushdb, 'FLUSHALL': cmd_flushall,
    # List
    'LPUSH': cmd_lpush, 'RPUSH': cmd_rpush,
    'LPOP': cmd_lpop, 'RPOP': cmd_rpop,
    'LLEN': cmd_llen, 'LRANGE': cmd_lrange,
    'LINDEX': cmd_lindex, 'LSET': cmd_lset, 'LREM': cmd_lrem,
    # Hash
    'HSET': cmd_hset, 'HGET': cmd_hget,
    'HMSET': cmd_hmset, 'HMGET': cmd_hmget, 'HGETALL': cmd_hgetall,
    'HDEL': cmd_hdel, 'HLEN': cmd_hlen,
    'HKEYS': cmd_hkeys, 'HVALS': cmd_hvals,
    'HEXISTS': cmd_hexists, 'HINCRBY': cmd_hincrby,
    'HINCRBYFLOAT': cmd_hincrbyfloat,
    # Set
    'SADD': cmd_sadd, 'SREM': cmd_srem,
    'SMEMBERS': cmd_smembers, 'SISMEMBER': cmd_sismember,
    'SMISMEMBER': cmd_smismember,
    'SCARD': cmd_scard, 'SUNION': cmd_sunion,
    'SINTER': cmd_sinter, 'SDIFF': cmd_sdiff,
    'SPOP': cmd_spop,
    'SUNIONSTORE': cmd_sunionstore, 'SINTERSTORE': cmd_sinterstore,
    # ZSet
    'ZADD': cmd_zadd, 'ZRANGE': cmd_zrange,
    'ZREVRANGE': cmd_zrevrange,
    'ZRANGEBYSCORE': cmd_zrangebyscore,
    'ZREVRANGEBYSCORE': cmd_zrevrangebyscore,
    'ZRANK': cmd_zrank, 'ZREVRANK': cmd_zrevrank,
    'ZSCORE': cmd_zscore, 'ZREM': cmd_zrem,
    'ZCARD': cmd_zcard, 'ZCOUNT': cmd_zcount,
    'ZINCRBY': cmd_zincrby,
    'ZPOPMIN': cmd_zpopmin, 'ZPOPMAX': cmd_zpopmax,
    # Pub/Sub
    'PUBLISH': cmd_publish, 'SUBSCRIBE': cmd_subscribe,
    'PUBSUB': cmd_pubsub,
    # Server
    'PING': cmd_ping, 'ECHO': cmd_echo, 'SELECT': cmd_select,
    'INFO': cmd_info, 'SAVE': cmd_save, 'BGSAVE': cmd_bgsave,
    'CONFIG': cmd_config, 'OBJECT': cmd_object,
    'WAIT': cmd_wait, 'LOLWUT': cmd_lolwut,
}
