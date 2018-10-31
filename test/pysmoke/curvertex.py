#!/usr/bin/env python

# to be run after basicsmoke

import now
import db
import datetime
import random

MYRANGE = (6000000, 6999999)
MYKEY = MYRANGE[0]
ERR_DUP_KEY = 27

def getNextKey():
    global MYKEY
    MYKEY += 1
    return MYKEY

# select primary key where primary key
def vidwherevid(c):

    print "RUNNING TEST 'vidwherevid'"

    idx = random.randint(1,len(ps)-1)
    k = ps[idx].key
    stmt = "select prod_key from product where prod_key = %d" % k
    with c.execute(stmt) as cur:
        if not cur.ok():
          raise db.TestFailed("cannot find %d: %s" % (k,cur.details()))
        n=0
        for row in cur:
            if n != 0:
               raise db.TestFailed("multiple rows for %d" % k)
            n+=1
            if row.field(0) != k:
               raise db.TestFailed("wrong product %d: %d" % (k, row.field(0)))

# select anything where primary key
def attswherevid(c):

    print "RUNNING TEST 'attswherevid'"

    idx = random.randint(1,len(ps)-1)
    k = ps[idx].key
    stmt = "select prod_desc from product where prod_key = %d" % k
    with c.execute(stmt) as cur:
        if not cur.ok():
          raise db.TestFailed("cannot find %d: %s" % (k,cur.details()))
        n=0
        for row in cur:
            if n != 0:
               raise db.TestFailed("multiple rows for %d" % k)
            n+=1
            if row.field(0) != ps[idx].desc:
               raise db.TestFailed("wrong product %d: %s" % (k, row.field(0)))

    stmt = "select prod_desc, prod_price from product where prod_key = %d" % k
    with c.execute(stmt) as cur:
        if not cur.ok():
          raise db.TestFailed("cannot find %d: %s" % (k,cur.details()))
        n=0
        for row in cur:
            if n != 0:
               raise db.TestFailed("multiple rows for %d" % k)
            n+=1
            if row.field(0) != ps[idx].desc:
               raise db.TestFailed("wrong product %d: %s" % (k, row.field(0)))
            if row.field(1) != ps[idx].price:
               raise db.TestFailed("wrong price %d: %f" % (k, row.field(1)))

    stmt = "select prod_key, prod_desc, prod_price from product where prod_key = %d" % k
    with c.execute(stmt) as cur:
        if not cur.ok():
          raise db.TestFailed("cannot find %d: %s" % (k,cur.details()))
        n=0
        for row in cur:
            if n != 0:
               raise db.TestFailed("multiple rows for %d" % k)
            n+=1
            if row.field(0) != ps[idx].key:
               raise db.TestFailed("wrong key %d: %d" % (k, row.field(0)))
            if row.field(1) != ps[idx].desc:
               raise db.TestFailed("wrong product %d: %s" % (k, row.field(1)))
            if row.field(2) != ps[idx].price:
               raise db.TestFailed("wrong price %d: %f" % (k, row.field(2)))

# select primary key where one att
def vidwhere1att(c):

    print "RUNNING TEST 'vidwhere1att'"

    idx = random.randint(1,len(ps)-1)
    print "LENGTH: %d, idx: %d" % (len(ps), idx)
    k = ps[idx].key

    # prod_key where 1 condition
    stmt = "select prod_key from product where prod_desc = %s" % ps[idx].desc
    with c.execute(stmt) as cur:
        if not cur.ok():
          raise db.TestFailed("cannot find %d: %s" % (k,cur.details()))
        n=0
        for row in cur:
            n+=1
        if n<1:
          raise db.TestFailed("cannot find %d: %s" % (k, ps[idx].desc))

    # prod_key where 2 conditions
    stmt = "select prod_key from product \
             where prod_price <= %f \
               and prod_price >= %f" % (ps[idx].price+0.0001, ps[idx].price-0.0001)
    with c.execute(stmt) as cur:
        if not cur.ok():
           raise db.TestFailed("cannot find %d: %s" % (k,cur.details()))

        n=0
        for row in cur:
            if row.field(0) == k:
               n+=1

        if n != 1:
           raise db.TestFailed("found product %d more than once: %d" % (k, n))

    # prod_key, att where 2 conditions
    stmt = "select prod_key, prod_desc from product \
             where prod_price <= %f \
               and prod_price >= %f" % (ps[idx].price+0.0001, ps[idx].price-0.0001)
    with c.execute(stmt) as cur:
        if not cur.ok():
           raise db.TestFailed("cannot find %d: %s" % (k,cur.details()))

        n=0
        for row in cur:
            if row.field(0) == k:
               n+=1
               if row.field(1) != ps[idx].desc:
                  raise db.TestFailed("wrong product %d: %s" % (k, row.field(1)))

        if n != 1:
           raise db.TestFailed("found product %d more than once: %d" % (k, n))

    # prod_key where key and 2 conditions
    stmt = "select prod_key from product \
             where prod_key = %d    \
               and prod_price <= %f \
               and prod_price >= %f" % (k, ps[idx].price+0.0001, ps[idx].price-0.0001)
    with c.execute(stmt) as cur:
        if not cur.ok():
           raise db.TestFailed("cannot find %d: %s" % (k,cur.details()))

        n=0
        for row in cur:
            n+=1
            if row.field(0) != k:
               raise db.TestFailed("wrong key %d: %d" % (k, row.field(0)))

        if n != 1:
           raise db.TestFailed("found product %d more than once: %d" % (k, n))

    # prod_key, att where key and 2 conditions
    stmt = "select prod_key, prod_desc from product \
             where prod_key = %d    \
               and prod_price <= %f \
               and prod_price >= %f" % (k, ps[idx].price+0.0001, ps[idx].price-0.0001)
    with c.execute(stmt) as cur:
        if not cur.ok():
           raise db.TestFailed("cannot find %d: %s" % (k,cur.details()))

        n=0
        for row in cur:
            n+=1
            if row.field(0) != k:
               raise db.TestFailed("wrong key %d: %d" % (k, row.field(0)))
            if row.field(1) != ps[idx].desc:
               raise db.TestFailed("wrong product %d: %s" % (k, row.field(1)))

        if n != 1:
           raise db.TestFailed("found product %d more than once: %d" % (k, n))

# select atts where one att
def attswhere1att(c):

    print "RUNNING TEST 'attswhere1att'"

    idx = random.randint(1,len(ps)-1)
    print "LENGTH: %d, idx: %d" % (len(ps), idx)
    k = ps[idx].key

    # 1 att where 1 condition
    stmt = "select prod_price from product where prod_desc = %s" % ps[idx].desc
    with c.execute(stmt) as cur:
        if not cur.ok():
          raise db.TestFailed("cannot find %d: %s" % (k,cur.details()))
        n=0
        for row in cur:
            n+=1

        if n < 1:
           raise db.TestFailed("cannot find product %d: %s" % (k, ps[idx].desc))

    # 2 atts where 1 condition
    stmt = "select prod_desc, prod_price from product where prod_desc = %s" % ps[idx].desc
    with c.execute(stmt) as cur:
        if not cur.ok():
          raise db.TestFailed("cannot find %d: %s" % (k,cur.details()))
        n=0
        for row in cur:
            n+=1
            if row.field(0) != ps[idx].desc:
               raise db.TestFailed("wrong product %d: %s" % (k, row.field(0)))
        if n < 1:
           raise db.TestFailed("cannot find product %d: %s" % (k, ps[idx].desc))

    # 1 att where 2 conditions
    stmt = "select prod_desc from product \
             where prod_price <= %f \
               and prod_price >= %f" % (ps[idx].price+0.0001, ps[idx].price-0.0001)
    with c.execute(stmt) as cur:
        if not cur.ok():
          raise db.TestFailed("cannot find %d: %s" % (k,cur.details()))
        n=0
        for row in cur:
            n+=1
        if n < 1:
           raise db.TestFailed("found product %d more than once: %d" % (k, n))

    # 2 atts (including vid) where 2 conditions
    stmt = "select prod_key, prod_desc from product \
             where prod_price <= %f \
               and prod_price >= %f" % (ps[idx].price+0.0001, ps[idx].price-0.0001)
    with c.execute(stmt) as cur:
        if not cur.ok():
          raise db.TestFailed("cannot find %d: %s" % (k,cur.details()))
        n=0
        for row in cur:
            if k == row.field(0):
               n+=1
               if row.field(1) != ps[idx].desc:
                  raise db.TestFailed("wrong product %d: %f" % (k, row.field(0)))
        if n != 1:
           raise db.TestFailed("found product %d more than once: %d" % (k, n))

    # 2 atts where 2 conditions
    stmt = "select prod_desc, prod_price from product \
             where prod_price <= %f \
               and prod_price >= %f" % (ps[idx].price+0.0001, ps[idx].price-0.0001)
    with c.execute(stmt) as cur:
        if not cur.ok():
          raise db.TestFailed("cannot find %d: %s" % (k,cur.details()))
        n=0
        for row in cur:
            n+=1
        if n < 1:
           raise db.TestFailed("found product %d more than once: %d" % (k, n))

# select constatns where primary key
# does not yet work!!!
def constwherevid(c):

    print "RUNNING TEST 'constwherevid'"

    idx = random.randint(1,len(ps)-1)
    k = ps[idx].key

    stmt = "select prod_key, prod_desc, prod_price, True, 'x' from product where prod_key = %d" % k
    with c.execute(stmt) as cur:
        if not cur.ok():
          raise db.TestFailed("cannot find %d (%d): %s" % (k, cur.code(), cur.details()))
        n=0
        for row in cur:
            if n != 0:
               raise db.TestFailed("multiple rows for %d" % k)
            n+=1
            if row.field(0) != ps[idx].key:
               raise db.TestFailed("wrong key %d: %d" % (k, row.field(0)))
            if row.field(1) != ps[idx].desc:
               raise db.TestFailed("wrong product %d: %s" % (k, row.field(1)))
            if row.field(2) != ps[idx].price:
               raise db.TestFailed("wrong price %d: %f" % (k, row.field(2)))
            if not row.field(3):
               raise db.TestFailed("True not True %d: %s" % (k, row.field(3)))
            if row.field(4) != 'x':
               raise db.TestFailed("'x' not 'x' %d: %s" % (k, row.field(4)))

    stmt = "select prod_key, True, 'x', prod_desc, prod_price from product where prod_key = %d" % k
    with c.execute(stmt) as cur:
        if not cur.ok():
          raise db.TestFailed("cannot find %d (%d): %s" % (k, cur.code(), cur.details()))
        n=0
        for row in cur:
            if n != 0:
               raise db.TestFailed("multiple rows for %d" % k)
            n+=1
            if row.field(0) != ps[idx].key:
               raise db.TestFailed("wrong key %d: %d" % (k, row.field(0)))
            if row.field(3) != ps[idx].desc:
               raise db.TestFailed("wrong product %d: %s" % (k, row.field(1)))
            if row.field(4) != ps[idx].price:
               raise db.TestFailed("wrong price %d: %f" % (k, row.field(2)))
            if not row.field(1):
               raise db.TestFailed("True not True %d: %s" % (k, row.field(3)))
            if row.field(2) != 'x':
               raise db.TestFailed("'x' not 'x' %d: %s" % (k, row.field(4)))

    stmt = "select prod_key, True, prod_desc, 'x', prod_price from product where prod_key = %d" % k
    with c.execute(stmt) as cur:
        if not cur.ok():
          raise db.TestFailed("cannot find %d (%d): %s" % (k, cur.code(), cur.details()))
        n=0
        for row in cur:
            if n != 0:
               raise db.TestFailed("multiple rows for %d" % k)
            n+=1
            if row.field(0) != ps[idx].key:
               raise db.TestFailed("wrong key %d: %d" % (k, row.field(0)))
            if row.field(2) != ps[idx].desc:
               raise db.TestFailed("wrong product %d: %s" % (k, row.field(2)))
            if row.field(4) != ps[idx].price:
               raise db.TestFailed("wrong price %d: %f" % (k, row.field(4)))
            if not row.field(1):
               raise db.TestFailed("True not True %d: %s" % (k, row.field(1)))
            if row.field(3) != 'x':
               raise db.TestFailed("'x' not 'x' %d: %s" % (k, row.field(3)))

    stmt = "select True, prod_desc, 'x', prod_price from product where prod_key = %d" % k
    with c.execute(stmt) as cur:
        if not cur.ok():
          raise db.TestFailed("cannot find %d (%d): %s" % (k, cur.code(), cur.details()))
        n=0
        for row in cur:
            if n != 0:
               raise db.TestFailed("multiple rows for %d" % k)
            n+=1
            if row.field(1) != ps[idx].desc:
               raise db.TestFailed("wrong product %d: %s" % (k, row.field(1)))
            if row.field(3) != ps[idx].price:
               raise db.TestFailed("wrong price %d: %f" % (k, row.field(3)))
            if not row.field(0):
               raise db.TestFailed("True not True %d: %s" % (k, row.field(0)))
            if row.field(2) != 'x':
               raise db.TestFailed("'x' not 'x' %d: %s" % (k, row.field(2)))

    stmt = "select True, 'x' from product where prod_key = %d" % k
    with c.execute(stmt) as cur:
        if not cur.ok():
          raise db.TestFailed("cannot find %d (%d): %s" % (k, cur.code(), cur.details()))
        n=0
        for row in cur:
            if n != 0:
               raise db.TestFailed("multiple rows for %d" % k)
            n+=1
            if not row.field(0):
               raise db.TestFailed("True not True %d: %s" % (k, row.field(0)))
            if row.field(1) != 'x':
               raise db.TestFailed("'x' not 'x' %d: %s" % (k, row.field(1)))

    stmt = "select True from product where prod_key = %d" % k
    with c.execute(stmt) as cur:
        if not cur.ok():
          raise db.TestFailed("cannot find %d (%d): %s" % (k, cur.code(), cur.details()))
        n=0
        for row in cur:
            if n != 0:
               raise db.TestFailed("multiple rows for %d" % k)
            n+=1
            if not row.field(0):
               raise db.TestFailed("True not True %d: %s" % (k, row.field(0)))

# select primary key where primary key IN
# does not yet work!!!
def whereinvid(c):

    print "RUNNING TEST 'whereinvid'"

    idx = random.randint(1,len(ps)-1)
    k = ps[idx].key

    l = ps[idx-1].key

    if idx < len(ps)-1:
       m = ps[idx+1].key
    else:
       m = ps[idx-2].key

    if k == l:
        if l == m:
           mx = 1
        else:
           mx = 2
    elif l == m or k == m:
       mx = 2
    else:
       mx = 3

    # select key where key in (...)
    stmt = "select prod_key from product where prod_key in (%d, %d, %d)" % (k,l,m)
    with c.execute(stmt) as cur:
        if not cur.ok():
          raise db.TestFailed("cannot find %d (%d): %s" % (k, cur.code(), cur.details()))
        n=0
        for row in cur:
            n+=1
            if row.field(0) != k and row.field(0) != l and row.field(0) != m:
                raise db.TestFailed("wrong key: %d,%d,%d != %d" % (k,l,m,row.field(0)))
        if n != mx:
            raise db.TestFailed("wrong key: %d,%d,%d != %d" % (k,l,m,row.field(0)))

    # select anything where key in (...)
    stmt = "select prod_key, prod_desc from product where prod_key in (%d, %d, %d)" % (k,l,m)
    with c.execute(stmt) as cur:
        if not cur.ok():
          raise db.TestFailed("cannot find %d (%d): %s" % (k, cur.code(), cur.details()))
        n=0
        for row in cur:
            n+=1
            if row.field(0) != k and row.field(0) != l and row.field(0) != m:
                raise db.TestFailed("wrong key: %d,%d,%d != %d" % (k,l,m,row.field(0)))
        if n != mx:
            raise db.TestFailed("wrong key: %d,%d,%d != %d" % (k,l,m,row.field(0)))

if __name__ == "__main__":
    with now.Connection("localhost", "55505", None, None) as c:
        (ps, cs, es) = db.loadDB(c, "db100")

        vidwherevid(c)
        attswherevid(c)
        # constwherevid(c)
        vidwhere1att(c)
        attswhere1att(c)
        # vidwhereatts(c)
        # constwhereatts(c)
        # whereinvid(c)

        print "PASSED"


