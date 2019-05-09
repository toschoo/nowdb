#!/usr/bin/env python

# to be run after basicsmoke

import now
import db
import datetime
import time
import random

ERR_DUP_KEY = 27

def countedge(es): # there should be a function argument!
    n=0
    for e in es:
        n+=1
    return n

# simple queries
def simplequeries(c):

    print "RUNNING TEST 'simplequeries'"

    idx = random.randint(1,len(es)-1)
    l = countedge(es)
    n = 0

    # fullscan with constant
    stmt = "select true from buys"
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
    stmt = "select origin, destin from buys"
    with c.execute(stmt) as cur:
        if not cur.ok():
            raise db.TestFailed("not ok (%d): %s" % (cur.code(),cur.details()))
        for row in cur:
            n+=1
    if n != l:
       raise db.TestFailed("count is wrong: %d / %d" % (l, n))

    # count
    stmt = "select count(*) from buys"
    with c.execute(stmt) as cur:
        if not cur.ok():
          raise db.TestFailed("not ok (%d): %s" % (cur.code(),cur.details()))
        for row in cur:
            if row.field(0) != n:
               raise db.TestFailed("wrong count: %d %% %d" % (n, row.field(0)))

    l = 0
    for e in es:
        if e.quantity == es[idx].quantity:
           l+=1

    print "expected: %d" % l

    # fullscan with condition
    stmt = "select origin, quantity from buys \
             where quantity = %d" % es[idx].quantity
    n = 0
    with c.execute(stmt) as cur:
        if not cur.ok():
            raise db.TestFailed("not ok (%d): %s" % (cur.code(),cur.details()))
        for row in cur:
            n+=1
            if row.field(1) != es[idx].quantity:
               raise db.TestFailed("wrong quantity: %d / %d" % (es[idx].quantity, row.field(1)))

    if n != l:
       raise db.TestFailed("count is wrong: %d / %d" % (l, n))

    # count with condition
    stmt = "select count(*) from buys \
             where quantity = %d" % es[idx].quantity
    with c.execute(stmt) as cur:
        if not cur.ok():
          raise db.TestFailed("not ok (%d): %s" % (cur.code(),cur.details()))
        for row in cur:
            if row.field(0) != n:
               raise db.TestFailed("wrong count: %d %% %d" % (n, row.field(0)))

def singlerow(c):

    print "RUNNING TEST 'singlerow'"

    stmt = "select count(*), sum(quantity) from buys"
    cnt = 0
    sm  = 0
    for row in c.execute(stmt):
        cnt = row.field(0)
        sm  = row.field(1)

    r = c.oneRow(stmt)
    if r[0] != cnt:
       raise db.TestFailed("count differs: %d | %d" % (r[0], cnt))
    if r[1] != sm:
       raise db.TestFailed("sum differs: %d | %d" % (r[1], sm))

    v = c.oneValue("select count(*) from buys")
    if v != cnt:
       raise db.TestFailed("count differs (2): %d | %d" % (v, cnt))

    v = c.oneValue("select sum(quantity) from buys")
    if v != sm:
       raise db.TestFailed("sum differs (2): %d | %d" % (v, sm))

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
    stmt = "select origin from buys group by origin"
    with c.execute(stmt) as cur:
        if not cur.ok():
          raise db.TestFailed("not ok (%d): %s" % (cur.code(),cur.details()))
        for row in cur:
            if keys[row.field(0)]:
               raise db.TestFailed("visiting %d twice" % row.field(1))
            keys[row.field(0)] = True

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
    stmt = "select origin, count(*) from buys group by origin"
    with c.execute(stmt) as cur:
        if not cur.ok():
          raise db.TestFailed("not ok (%d): %s" % (cur.code(),cur.details()))
        for row in cur:
            q[row.field(0)] = row.field(1)

    if len(q) != len(keys):
       raise db.TestFailed("number of keys does not match")

    n = 0
    for (k,v) in keys.items():
        if q[k] != v:
           raise db.TestFailed("values for key %d differs: %d %% %d" % (k, v, q[k]))
        n += v

    # crosscheck values
    if n != countedge(es):
       raise db.TestFailed("counted too many values: %d %% %d" % (n, len(es)))

    q = {}

    # keys with count and where (not in keys)
    stmt = "select origin, count(*) from buys \
             where stamp > '1970-01-01' \
             group by origin"
    with c.execute(stmt) as cur:
        if not cur.ok():
          raise db.TestFailed("not ok (%d): %s" % (cur.code(),cur.details()))
        for row in cur:
            q[row.field(0)] = row.field(1)

    if len(q) != len(keys):
       raise db.TestFailed("number of keys does not match")

    for (k,v) in keys.items():
        if q[k] != v:
           raise db.TestFailed("values for key %d differs: %d %% %d" % (k, v, q[k]))

    for e in es:
        keys[e.origin] = False

    # keys-only group with where (not in keys)
    stmt = "select origin from buys \
             where stamp > '1970-01-01' \
             group by origin"
    with c.execute(stmt) as cur:
        if not cur.ok():
          raise db.TestFailed("not ok (%d): %s" % (cur.code(),cur.details()))
        for row in cur:
            if keys[row.field(0)]:
               raise db.TestFailed("visiting %d twice" % row.field(1))
            keys[row.field(0)] = True

    if l != len(keys):
       raise db.TestFailed("number of keys does not match")

    for (k,v) in keys.items():
        if not keys[k]:
           raise db.TestFailed("value %d not found" % k)

# order queries
def orderqueries(c):

    print "RUNNING TEST 'orderqueries'"
    
    ses = sorted(es, key=lambda e: (e.destin))

    x=True
    i=0
    stmt = "select destin from buys"
    with c.execute(stmt) as cur:
        if not cur.ok():
          raise db.TestFailed("not ok (%d): %s" % (cur.code(),cur.details()))
        for row in cur:
            if row.field(0) != ses[i].destin:
               x=False
               break
            i+=1
    if x:
       print "CAUTION data already sorted according to destination!"

    x=True
    i=0
    stmt = "select destin from buys order by destin"
    with c.execute(stmt) as cur:
        if not cur.ok():
          raise db.TestFailed("not ok (%d): %s" % (cur.code(),cur.details()))
        for row in cur:
            if row.field(0) != ses[i].destin:
               x=False
               break
            i+=1
    if not x:
       raise db.TestFailed("data not sorted!")

if __name__ == "__main__":

    random.seed()

    with now.Connection("localhost", "55505", None, None) as c:
        (ps, cs, ss, es, vs) = db.loadDB(c, "db100")

        simplequeries(c)
        singlerow(c)
        nwherequeries(c)
        groupqueries(c)
        orderqueries(c)

        print "PASSED"
