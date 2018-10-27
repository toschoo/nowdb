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

if __name__ == "__main__":
    with now.Connection("localhost", "55505", None, None) as c:
        (ps, cs, es) = db.loadDB(c, "db100")

        dupkeyvertex(c)
        failedinsert(c)




