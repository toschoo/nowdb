#!/usr/bin/env python

# to be run after basicsmoke

import now
import db
import datetime
import time
import random

ERR_DUP_KEY = 27

# simple queries
def simplequeries(c):

    print "RUNNING TEST 'simplequeries'"

    idx = random.randint(1,len(es)-1)
    l = len(es)
    n = 0

    # fullscan with constant
    stmt = "select true from sales where edge = 'buys'"
    with c.execute(stmt) as cur:
        if not cur.ok():
            raise db.TestFailed("not ok (%d): %s" % (cur.code(),cur.details()))
        for row in cur:
            n+=1
            if not row.field(0):
               raise db.TestFailed("wrong constant: %s" % (row.field(0)))

    print "counted: %d" % n
    print "length : %d" % l

    if n != l:
       raise db.TestFailed("count is wrong: %d / %d" % (l, n))

    # fullscan with fields
    n = 0
    stmt = "select edge, origin from sales where edge = 'buys'"
    with c.execute(stmt) as cur:
        if not cur.ok():
            raise db.TestFailed("not ok (%d): %s" % (cur.code(),cur.details()))
        for row in cur:
            n+=1
            if row.field(0) != 'buys':
               raise db.TestFailed("wrong edge: %s" % (row.field(0)))

    if n != l:
       raise db.TestFailed("count is wrong: %d / %d" % (l, n))

    # count
    stmt = "select count(*) from sales where edge = 'buys'"
    with c.execute(stmt) as cur:
        if not cur.ok():
          raise db.TestFailed("not ok (%d): %s" % (cur.code(),cur.details()))
        for row in cur:
            if row.field(0) != n:
               raise db.TestFailed("wrong count: %d %% %d" % (n, row.field(0)))

    l = 0
    for e in es:
        if e.weight == es[idx].weight:
           l+=1

    print "expected: %d" % l

    # fullscan with condition
    stmt = "select edge, origin, weight from sales \
             where edge = 'buys' \
             and weight = %d" % es[idx].weight
    n = 0
    with c.execute(stmt) as cur:
        if not cur.ok():
            raise db.TestFailed("not ok (%d): %s" % (cur.code(),cur.details()))
        for row in cur:
            n+=1
            if row.field(0) != 'buys':
               raise db.TestFailed("wrong edge: %s" % (row.field(0)))
            if row.field(2) != es[idx].weight:
               raise db.TestFailed("wrong weight: %d / %d" % (es[idx].weight, row.field(2)))

    if n != l:
       raise db.TestFailed("count is wrong: %d / %d" % (l, n))

    # count with condition
    stmt = "select count(*) from sales \
             where edge = 'buys' \
             and weight = %d" % es[idx].weight
    with c.execute(stmt) as cur:
        if not cur.ok():
          raise db.TestFailed("not ok (%d): %s" % (cur.code(),cur.details()))
        for row in cur:
            if row.field(0) != n:
               raise db.TestFailed("wrong count: %d %% %d" % (n, row.field(0)))

# complex where queries
def nwherequeries(c):

    print "RUNNING TEST 'nwherequeries'"

    idx = random.randint(1,len(es)-1)
    o = es[idx].origin
    d = es[idx].destin

# group queries
def groupqueries(c):

    print "RUNNING TEST 'groupqueries'"

    idx = random.randint(1,len(es)-1)
    o = es[idx].origin
    d = es[idx].destin

    keys = {}
    for e in es:
      keys[e.origin] = False

    l = len(keys)

    # keys only
    stmt = "select edge, origin from sales group by edge, origin"
    with c.execute(stmt) as cur:
        if not cur.ok():
          raise db.TestFailed("not ok (%d): %s" % (cur.code(),cur.details()))
        for row in cur:
            if keys[row.field(1)]:
               raise db.TestFailed("visiting %d twice" % row.field(1))
            keys[row.field(1)] = True

    if l != len(keys):
       raise db.TestFailed("too many keys!")

    for (k,v) in keys.items():
        if not v:
           raise db.TestFailed("key %d not found" % k)

    for e in es:
      keys[e.origin] = 0

    for e in es:
      keys[e.origin] += 1

    q = {}

    # keys with count
    stmt = "select edge, origin, count(*) from sales group by edge, origin"
    with c.execute(stmt) as cur:
        if not cur.ok():
          raise db.TestFailed("not ok (%d): %s" % (cur.code(),cur.details()))
        for row in cur:
            q[row.field(1)] = row.field(2)

    if len(q) != len(keys):
       raise db.TestFailed("number of keys does not match")

    n = 0
    for (k,v) in keys.items():
        if q[k] != v:
           raise db.TestFailed("values for key %d differs: %d %% %d" % (k, v, q[k]))
        n += v

    # crosscheck values
    if n != len(es):
       raise db.TestFailed("counted too many values: %d %% %d" % (n, len(es)))

    q = {}

    # keys with count and where (not in keys)
    stmt = "select edge, origin, count(*) from sales \
             where stamp > '1970-01-01' \
             group by edge, origin"
    with c.execute(stmt) as cur:
        if not cur.ok():
          raise db.TestFailed("not ok (%d): %s" % (cur.code(),cur.details()))
        for row in cur:
            q[row.field(1)] = row.field(2)

    if len(q) != len(keys):
       raise db.TestFailed("number of keys does not match")

    for (k,v) in keys.items():
        if q[k] != v:
           raise db.TestFailed("values for key %d differs: %d %% %d" % (k, v, q[k]))

    for e in es:
      keys[e.origin] = False

    # keys-only group with where (not in keys)
    stmt = "select edge, origin from sales \
             where stamp > '1970-01-01' \
             group by edge, origin"
    with c.execute(stmt) as cur:
        if not cur.ok():
          raise db.TestFailed("not ok (%d): %s" % (cur.code(),cur.details()))
        for row in cur:
            if keys[row.field(1)]:
               raise db.TestFailed("visiting %d twice" % row.field(1))
            keys[row.field(1)] = True

    if l != len(keys):
       raise db.TestFailed("number of keys does not match")

    for (k,v) in keys.items():
        if not keys[k]:
           raise db.TestFailed("value %d not found" % k)

if __name__ == "__main__":
    with now.Connection("localhost", "55505", None, None) as c:
        (ps, cs, es) = db.loadDB(c, "db100")

        simplequeries(c)
        nwherequeries(c)
        groupqueries(c)

        print "PASSED"
