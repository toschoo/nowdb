#!/usr/bin/env python

# to be run after basicsmoke

import now
import db
import datetime
import random

MYRANGE = (5000000, 5999999)
MYKEY = MYRANGE[0]
ERR_DUP_KEY = 27

def getNextKey():
    global MYKEY
    MYKEY += 1
    return MYKEY

def getProduct(p):
    i = random.randint(0,len(p)-1)
    return p[i]

def getClient(c):
    i = random.randint(0,len(c)-1)
    return c[i]

def dupkeyvertex(c):

    print "RUNNING TEST 'dupkeyvertex'"

    k = getNextKey()
    stmt = "insert into product (prod_key, prod_desc, prod_price) \
                                (%d, 'product %s', 1.99)" % (k, str(k))
    with c.execute(stmt) as r:
       if not r.ok():
          raise db.TestFailed("cannot insert %d: %s" % (k,r.details()))

    stmt = "select prod_desc from product where prod_key = %d" % k
    with c.execute(stmt) as cur:
        if not cur.ok():
          raise db.TestFailed("cannot find %d: %s" % (k,cur.details()))
        n=0
        for row in cur:
            if n != 0:
               raise db.TestFailed("multiple rows for %d" % k)
            n+=1
            if row.field(0) != ("product %d" % k):
               raise db.TestFailed("wrong product %d: %s" % (k, row.field(0)))

    stmt = "insert into product (prod_key, prod_desc, prod_price) \
                                (%d, 'product %s', 1.99)" % (k, str(k))
    with c.execute(stmt) as r:
       if r.ok():
          raise db.TestFailed("I can insert %d twice!" % k)
       if r.code() != ERR_DUP_KEY:
          raise db.TestFailed("wrong error: %s" % r.details())

    stmt = "select prod_desc from product where prod_key = %d" % k
    with c.execute(stmt) as cur:
        if not cur.ok():
          raise db.TestFailed("cannot find %d: %s" % (k,cur.details()))
        n=0
        for row in cur:
            if n != 0:
               raise db.TestFailed("multiple rows for %d" % k)
            n+=1
            if row.field(0) != ("product %d" % k):
               raise db.TestFailed("wrong product %d: %s" % (k, row.field(0)))

def failedinsert(c):

    print "RUNNING TEST 'failedinsert'"

    k = getNextKey()
    stmt = "insert into product (prod_key, prod_desc, prod_price) \
                                (%d, 'product %s', 'this is not a number')" % (k, str(k))
    with c.execute(stmt) as r:
       if r.ok():
          raise db.TestFailed("I can insert a vertex with wrong type")
       print r.details()

    stmt = "select prod_desc from product where prod_key = %d" % k
    with c.execute(stmt) as cur:
        if cur.ok():
          raise db.TestFailed("%d inserted in spite of error" % k)
        if cur.code() != now.EOF:
          raise db.TestFailed("Expecting EOF, but have %d: %s" % (cur.code(),cur.details()))

def insertallvertex(c):

    print "RUNNING TEST 'insertallvertex'"

    # insert with field list (partial)
    k = getNextKey()
    stmt = "insert into product (prod_key, prod_desc, prod_price) \
                                (%d, 'product %s', 1.59)" % (k, str(k))
    with c.execute(stmt) as r:
       if not r.ok():
          raise db.TestFailed("cannot insert %d: %s" % (k,r.details()))

    stmt = "select prod_desc from product where prod_key = %d" % k
    with c.execute(stmt) as cur:
        if not cur.ok():
          raise db.TestFailed("cannot find %d: %s" % (k,cur.details()))
        n=0
        for row in cur:
            if n != 0:
               raise db.TestFailed("multiple rows for %d" % k)
            n+=1
            if row.field(0) != ("product %d" % k):
               raise db.TestFailed("wrong product %d: %s" % (k, row.field(0)))

    # insert with field list (complete)
    k = getNextKey()
    stmt = "insert into product (prod_key, prod_desc, prod_cat, prod_packing, prod_price) \
                                (%d, 'product %s', 1, 3, 1.59)" % (k, str(k))
    with c.execute(stmt) as r:
       if not r.ok():
          raise db.TestFailed("cannot insert %d: %s" % (k,r.details()))

    stmt = "select prod_desc from product where prod_key = %d" % k
    with c.execute(stmt) as cur:
        if not cur.ok():
          raise db.TestFailed("cannot find %d: %s" % (k,cur.details()))
        n=0
        for row in cur:
            if n != 0:
               raise db.TestFailed("multiple rows for %d" % k)
            n+=1
            if row.field(0) != ("product %d" % k):
               raise db.TestFailed("wrong product %d: %s" % (k, row.field(0)))

    # insert with field list (complete, vary order)
    k = getNextKey()
    stmt = "insert into product (prod_cat, prod_desc, prod_packing, prod_key, prod_price) \
                                (1, 'product %s', 3, %d, 1.59)" % (str(k), k)
    with c.execute(stmt) as r:
       if not r.ok():
          raise db.TestFailed("cannot insert %d: %s" % (k,r.details()))

    stmt = "select prod_desc from product where prod_key = %d" % k
    with c.execute(stmt) as cur:
        if not cur.ok():
          raise db.TestFailed("cannot find %d: %s" % (k,cur.details()))
        n=0
        for row in cur:
            if n != 0:
               raise db.TestFailed("multiple rows for %d" % k)
            n+=1
            if row.field(0) != ("product %d" % k):
               raise db.TestFailed("wrong product %d: %s" % (k, row.field(0)))

    # insert with field list (complete, no fields)
    k = getNextKey()
    stmt = "insert into product (%d, 'product %s', 1, 3, 1.59)" % (k, str(k))
    with c.execute(stmt) as r:
       if not r.ok():
          raise db.TestFailed("cannot insert %d: %s" % (k,r.details()))

    stmt = "select prod_desc from product where prod_key = %d" % k
    with c.execute(stmt) as cur:
        if not cur.ok():
          raise db.TestFailed("cannot find %d: %d" % (k,cur.code()))
        n=0
        for row in cur:
            if n != 0:
               raise db.TestFailed("multiple rows for %d" % k)
            n+=1
            if row.field(0) != ("product %d" % k):
               raise db.TestFailed("wrong product %d: %s" % (k, row.field(0)))

    # insert with field list (incomplete, no fields)
    k = getNextKey()
    stmt = "insert into product (%d, 'product %s', 1, 1.59)" % (k, str(k))
    with c.execute(stmt) as r:
       if r.ok():
          raise db.TestFailed("can insert incompletely")
       print "%d: %s" % (r.code(), r.details())

    stmt = "select prod_desc from product where prod_key = %d" % k
    with c.execute(stmt) as cur:
        if cur.ok():
          raise db.TestFailed("%d inserted in spite of error" % k)
        if cur.code() != now.EOF:
          raise db.TestFailed("Expecting EOF, but have %d: %s" % (cur.code(),cur.details()))

    # insert with field list (complete, no fields, wrong type)
    k = getNextKey()
    stmt = "insert into product (%d, 'product %s', 1, '3', 1.59)" % (k, str(k))
    with c.execute(stmt) as r:
       if r.ok():
          raise db.TestFailed("can insert with wrong type")
       print "%d: %s" % (r.code(), r.details())

    stmt = "select prod_desc from product where prod_key = %d" % k
    with c.execute(stmt) as cur:
        if cur.ok():
          raise db.TestFailed("%d inserted in spite of error" % k)
        if cur.code() != now.EOF:
          raise db.TestFailed("Expecting EOF, but have %d: %s" % (cur.code(),cur.details()))

def insertalledge(c,ps,cs):

    print "RUNNING TEST 'insertalledge'"

    # insert with field list (partial)
    p = getProduct(ps).key
    k = getClient(cs).key
    dt = datetime.datetime.utcnow()
    s = dt.strftime(now.TIMEFORMAT)
    t = now.dt2now(dt)
    stmt = "insert into sales (edge, origin, destin, stamp, weight, weight2) \
                              ('buys', %d, %d, '%s', 1, 1.49)" % (k, p, s)
    with c.execute(stmt) as r:
       if not r.ok():
          raise db.TestFailed("cannot insert edge: %d -- %s" % (r.code(),r.details()))

    stmt = "select origin, destin, stamp from sales \
             where edge = 'buys' \
               and origin = %d \
               and destin = %d \
               and stamp  = '%s'" % (k, p, s)
    with c.execute(stmt) as cur:
        if not cur.ok():
          raise db.TestFailed("cannot find %d, %d, %d: %d: %s" % (k,p,s,cur.code(),cur.details()))
        n=0
        for row in cur:
            n+=1
            if row.field(0) != k or row.field(1) != p or row.field(2) != t:
               raise db.TestFailed("wrong edge %d/%d/%s (%d) -- %d/%d/%s (%d)" % \
                                  (k,p,s,t,row.field(0),row.field(1),\
                                   now.now2dt(row.field(2)).strftime(now.TIMEFORMAT),
                                   row.field(2)))

    # insert with field list (complete)
    p = getProduct(ps).key
    k = getClient(cs).key
    dt = datetime.datetime.utcnow()
    s = dt.strftime(now.TIMEFORMAT)
    t = now.dt2now(dt)
    stmt = "insert into sales (edge, origin, destin, label, stamp, weight, weight2) \
                              ('buys', %d, %d, 0, '%s', 1, 1.49)" % (k, p, s)
    with c.execute(stmt) as r:
       if not r.ok():
          raise db.TestFailed("cannot insert edge: %d -- %s" % (r.code(),r.details()))

    stmt = "select origin, destin, label, stamp from sales \
             where edge = 'buys' \
               and origin = %d \
               and destin = %d \
               and stamp  = '%s'" % (k, p, s)
    with c.execute(stmt) as cur:
        if not cur.ok():
          raise db.TestFailed("cannot find %d, %d, %d: %d: %s" % (k,p,s,cur.code(),cur.details()))
        n=0
        for row in cur:
            n+=1
            if row.field(0) != k or row.field(1) != p or row.field(2) != 0 or row.field(3) != t:
               raise db.TestFailed("wrong edge %d/%d/%s (%d) -- %d/%d/%s (%d)" % \
                                  (k,p,s,t,row.field(0),row.field(1),\
                                   now.now2dt(row.field(3)).strftime(now.TIMEFORMAT),
                                   row.field(3)))

    # insert without field list (complete)
    p = getProduct(ps).key
    k = getClient(cs).key
    dt = datetime.datetime.utcnow()
    s = dt.strftime(now.TIMEFORMAT)
    t = now.dt2now(dt)
    stmt = "insert into sales ('buys', %d, %d, 0, '%s', 1, 1.49)" % (k, p, s)
    with c.execute(stmt) as r:
       if not r.ok():
          raise db.TestFailed("cannot insert edge: %d -- %s" % (r.code(),r.details()))

    stmt = "select origin, destin, label, stamp from sales \
             where edge = 'buys' \
               and origin = %d \
               and destin = %d \
               and stamp  = '%s'" % (k, p, s)
    with c.execute(stmt) as cur:
        if not cur.ok():
          raise db.TestFailed("cannot find %d, %d, %d: %d: %s" % (k,p,s,cur.code(),cur.details()))
        n=0
        for row in cur:
            n+=1
            if row.field(0) != k or row.field(1) != p or row.field(2) != 0 or row.field(3) != t:
               raise db.TestFailed("wrong edge %d/%d/%s (%d) -- %d/%d/%s (%d)" % \
                                  (k,p,s,t,row.field(0),row.field(1),\
                                   now.now2dt(row.field(3)).strftime(now.TIMEFORMAT),
                                   row.field(3)))

    # insert without field list (incomplete)
    p = getProduct(ps).key
    k = getClient(cs).key
    dt = datetime.datetime.utcnow()
    s = dt.strftime(now.TIMEFORMAT)
    t = now.dt2now(dt)
    stmt = "insert into sales ('buys', %d, %d, '%s', 1, 1.49)" % (k, p, s)
    with c.execute(stmt) as r:
       if r.ok():
          raise db.TestFailed("can insert incomplete edge")
       print "%d: %s" % (r.code(), r.details())

    stmt = "select origin, destin, label, stamp from sales \
             where edge = 'buys' \
               and origin = %d \
               and destin = %d \
               and stamp  = '%s'" % (k, p, s)
    with c.execute(stmt) as cur:
        if cur.ok():
          raise db.TestFailed("%d inserted in spite of error" % k)
        if cur.code() != now.EOF:
          raise db.TestFailed("Expecting EOF, but have %d: %s" % (cur.code(),cur.details()))

    # insert without field list (wrong type)
    p = getProduct(ps).key
    k = getClient(cs).key
    dt = datetime.datetime.utcnow()
    s = dt.strftime(now.TIMEFORMAT)
    t = now.dt2now(dt)
    stmt = "insert into sales ('buys', %d, 'my product', 0, '%s', 1, 1.49)" % (k, s)
    with c.execute(stmt) as r:
       if r.ok():
          raise db.TestFailed("can insert edge with wrong type!")
       print "%d: %s" % (r.code(), r.details())

    stmt = "select origin, destin, label, stamp from sales \
             where edge = 'buys' \
               and origin = %d \
               and destin = %d \
               and stamp  = '%s'" % (k, p, s)
    with c.execute(stmt) as cur:
        if cur.ok():
          raise db.TestFailed("%d inserted in spite of error" % k)
        if cur.code() != now.EOF:
          raise db.TestFailed("Expecting EOF, but have %d: %s" % (cur.code(),cur.details()))

    # insert without field list (wrong type in edge)
    p = getProduct(ps).key
    k = getClient(cs).key
    dt = datetime.datetime.utcnow()
    s = dt.strftime(now.TIMEFORMAT)
    t = now.dt2now(dt)
    stmt = "insert into sales (1, %d, %d, 0, '%s', 1, 1.49)" % (k, p, s)
    with c.execute(stmt) as r:
       if r.ok():
          raise db.TestFailed("can insert edge with wrong edge type!")
       print "%d: %s" % (r.code(), r.details())

    stmt = "select origin, destin, label, stamp from sales \
             where edge = 'buys' \
               and origin = %d \
               and destin = %d \
               and stamp  = '%s'" % (k, p, s)
    with c.execute(stmt) as cur:
        if cur.ok():
          raise db.TestFailed("%d inserted in spite of error" % k)
        if cur.code() != now.EOF:
          raise db.TestFailed("Expecting EOF, but have %d: %s" % (cur.code(),cur.details()))

    # insert without field list (wrong edge)
    p = getProduct(ps).key
    k = getClient(cs).key
    dt = datetime.datetime.utcnow()
    s = dt.strftime(now.TIMEFORMAT)
    t = now.dt2now(dt)
    stmt = "insert into sales ('doesnotexist', %d, %d, 0, '%s', 1, 1.49)" % (k, p, s)
    with c.execute(stmt) as r:
       if r.ok():
          raise db.TestFailed("can insert edge with non-existing edge!")
       print "%d: %s" % (r.code(), r.details())

    stmt = "select origin, destin, label, stamp from sales \
             where edge = 'buys' \
               and origin = %d \
               and destin = %d \
               and stamp  = '%s'" % (k, p, s)
    with c.execute(stmt) as cur:
        if cur.ok():
          raise db.TestFailed("%d inserted in spite of error" % k)
        if cur.code() != now.EOF:
          raise db.TestFailed("Expecting EOF, but have %d: %s" % (cur.code(),cur.details()))

###########################################################################
# MAIN 
###########################################################################
if __name__ == "__main__":
    with now.Connection("localhost", "55505", None, None) as c:
        (ps, cs, es) = db.loadDB(c, "db100")

        dupkeyvertex(c)
        failedinsert(c)
        insertallvertex(c)
        insertalledge(c,ps,cs)

        print "PASSED"
