#!/usr/bin/env python

# to be run after basicsmoke

import now
import db
import datetime
import time
import random

ERR_DUP_KEY = 27

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
    # print "LENGTH: %d, idx: %d" % (len(ps), idx)
    k = ps[idx].key

    # prod_key where 1 condition
    stmt = "select prod_key from product where prod_desc = '%s'" % ps[idx].desc
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

# select primary key where several atts
def vidwhereatts(c):

    print "RUNNING TEST 'vidwhereatts'"

    idx = random.randint(1,len(ps)-1)
    # print "LENGTH: %d, idx: %d" % (len(ps), idx)
    k = ps[idx].key

    # prod_key where key and non-key
    stmt = "select prod_key from product where prod_desc = '%s' and prod_key=%d" % (ps[idx].desc, ps[idx].key)
    with c.execute(stmt) as cur:
        if not cur.ok():
          raise db.TestFailed("cannot find %d: %s" % (k,cur.details()))
        n=0
        for row in cur:
            n+=1
        if n<1:
          raise db.TestFailed("cannot find %d: %s" % (k, ps[idx].desc))

    # non-key and non-key
    stmt = "select prod_key from product \
             where prod_desc = '%s' \
               and prod_price < 100.0" % ps[idx].desc
    with c.execute(stmt) as cur:
        if not cur.ok():
          raise db.TestFailed("cannot find %d: %s" % (cur.code(),cur.details()))
        n=0
        for row in cur:
            n+=1
        if n<1:
          raise db.TestFailed("cannot find %d: %s" % (k, ps[idx].desc))

    # non-key and non-key
    stmt = "select prod_key from product \
             where prod_cat  < 5\
               and prod_price < 100.0"
    with c.execute(stmt) as cur:
        if not cur.ok():
          raise db.TestFailed("cannot find %d: %s" % (cur.code(),cur.details()))
        n=0
        for row in cur:
            n+=1
        if n<1:
          raise db.TestFailed("nothing found")

    # non-key and non-key and non-key
    stmt = "select prod_key from product \
             where prod_cat  < 5\
               and prod_packing < 3 \
               and prod_price < 100.0"
    with c.execute(stmt) as cur:
        if not cur.ok():
          raise db.TestFailed("cannot find %d: %s" % (cur.code(),cur.details()))
        n=0
        for row in cur:
            n+=1
        if n<1:
          raise db.TestFailed("nothing found")

    # non-key and non-key and non-key (variant)
    stmt = "select prod_key from product \
             where prod_cat  = %d\
               and prod_packing = %d \
               and prod_price < 100.0" % (ps[idx].cat, ps[idx].packing)
    with c.execute(stmt) as cur:
        if not cur.ok():
          raise db.TestFailed("cannot find %d: %s" % (cur.code(),cur.details()))
        n=0
	found=False
        for row in cur:
            n+=1
            print "%d" % row.field(0)
            if row.field(0) == k:
               found=True
        if n<1:
          raise db.TestFailed("nothing found")
        if not found:
          raise db.TestFailed("cannot find %d: %s (%f)" % 
                          (k, ps[idx].desc, ps[idx].price))

    # non-key and non-key or non-key
    stmt = "select prod_key from product \
             where prod_cat  = %d\
               and prod_packing = %d \
                or prod_price < 100.0" % (ps[idx].cat, ps[idx].packing)
    with c.execute(stmt) as cur:
        if not cur.ok():
          raise db.TestFailed("cannot find %d: %s" % (cur.code(),cur.details()))
        n=0
	found=False
        for row in cur:
            n+=1
            if row.field(0) == k:
               found=True
        if n<1:
          raise db.TestFailed("nothing found")
        if not found:
          raise db.TestFailed("cannot find %d: %s" % (k, ps[idx].desc))

    # non-key or non-key and non-key (variant)
    stmt = "select prod_key from product \
             where prod_cat  = %d\
                or prod_packing = %d \
               and prod_price < 100.0" % (ps[idx].cat, ps[idx].packing)
    with c.execute(stmt) as cur:
        if not cur.ok():
          raise db.TestFailed("cannot find %d: %s" % (cur.code(),cur.details()))
        n=0
	found=False
        for row in cur:
            n+=1
            if row.field(0) == k:
               found=True
        if n<1:
          raise db.TestFailed("nothing found")
        if not found:
          raise db.TestFailed("cannot find %d: %s" % (k, ps[idx].desc))

    # non-key or non-key or non-key
    stmt = "select prod_key from product \
             where prod_cat  = %d\
                or prod_packing = %d \
                or prod_price < 100.0" % (ps[idx].cat, ps[idx].packing)
    with c.execute(stmt) as cur:
        if not cur.ok():
          raise db.TestFailed("cannot find %d: %s" % (cur.code(),cur.details()))
        n=0
	found=False
        for row in cur:
            n+=1
            if row.field(0) == k:
               found=True
        if n<1:
          raise db.TestFailed("nothing found")
        if not found:
          raise db.TestFailed("cannot find %d: %s" % (k, ps[idx].desc))

    # non-key or non-key or non-key (variant)
    stmt = "select prod_key from product \
             where prod_cat  = 10 \
                or prod_packing = 100 \
                or prod_price < 100.0" 
    with c.execute(stmt) as cur:
        if not cur.ok():
          raise db.TestFailed("cannot find %d: %s" % (cur.code(),cur.details()))
        n=0
	found=False
        for row in cur:
            n+=1
            if row.field(0) == k:
               found=True
        if n<1:
          raise db.TestFailed("nothing found")
        if not found:
          raise db.TestFailed("cannot find %d: %s" % (k, ps[idx].desc))

    # non-key and non-key or non-key (wrong condition fails)
    stmt = "select prod_key from product \
             where prod_cat  = 10 \
               and prod_packing = 100 \
                or prod_price < 100.0" 
    with c.execute(stmt) as cur:
        if not cur.ok():
          raise db.TestFailed("cannot find %d: %s" % (cur.code(),cur.details()))
        n=0
	found=False
        for row in cur:
            n+=1
            if row.field(0) == k:
               found=True
        if n<1:
          raise db.TestFailed("nothing found")
        if not found:
          raise db.TestFailed("cannot find %d: %s" % (k, ps[idx].desc))

    # non-key and (non-key or non-key) (fails)
    stmt = "select prod_key from product \
             where prod_cat  = 10 \
               and (prod_packing < 5 \
                or  prod_price < 100.0)" 
    with c.execute(stmt) as cur:
        if cur.ok():
           raise db.TestFailed("found data for an impposible query")
        if cur.code() != now.EOF:
           raise db.TestFailed("unexpected error: %d: %s" % (cur.code(), cur.details()))

    # non-key or (non-key and non-key)
    stmt = "select prod_key from product \
             where prod_cat  = 10 \
                or (prod_packing < 100 \
                and prod_price < 100.0)" 
    with c.execute(stmt) as cur:
        if not cur.ok():
          raise db.TestFailed("cannot find %d: %s" % (cur.code(),cur.details()))
        n=0
	found=False
        for row in cur:
            n+=1
            if row.field(0) == k:
               found=True
        if n<1:
          raise db.TestFailed("nothing found")
        if not found:
          raise db.TestFailed("cannot find %d: %s" % (k, ps[idx].desc))

# select atts where several atts
def attswhereatts(c):

    print "RUNNING TEST 'attswhereatts'"

    idx = random.randint(1,len(ps)-1)
    # print "LENGTH: %d, idx: %d" % (len(ps), idx)
    k = ps[idx].key

    # prod_key where key and non-key
    stmt = "select prod_desc, prod_cat from product \
             where prod_desc = '%s' \
               and prod_key=%d" % (ps[idx].desc, ps[idx].key)
    print stmt
    with c.execute(stmt) as cur:
        if not cur.ok():
            raise db.TestFailed("cursor not ok: %d/%s" % 
                             (cur.code(), cur.details()))
        n=0
        for row in cur:
            n+=1
        if n<1:
          raise db.TestFailed("found nothing for %d/%s" % (ps[idx].key,ps[idx].desc))

    # prod_desc, prod_key where key and non-key
    stmt = "select prod_desc, prod_key from product \
             where prod_cat = %d \
               and prod_packing = %d" % (ps[idx].cat, ps[idx].packing)
    with c.execute(stmt) as cur:
        if not cur.ok():
          raise db.TestFailed("cannot find %d: %s" % (k,cur.details()))
        n=0
        found=0
        for row in cur:
            n+=1
            if row.field(1) == k:
               found=1
        if n<1:
          raise db.TestFailed("found nothing")
        if not found:
          raise db.TestFailed("cannot find %d: %s" % (k, ps[idx].desc))

    # where non-key and (non-key and non-key)
    stmt = "select prod_desc, prod_price from product \
             where prod_cat = %d \
               and (prod_price >= %f \
               and  prod_price <= %f)" % (ps[idx].cat, ps[idx].price-0.001, ps[idx].price+0.001)
    with c.execute(stmt) as cur:
        if not cur.ok():
          raise db.TestFailed("not ok %d: %s" % (cur.code(),cur.details()))
        n=0
        found=0
        for row in cur:
            n+=1
            if row.field(0) == ps[idx].desc:
               found=1
        if n<1:
          raise db.TestFailed("found nothing")
        if not found:
          raise db.TestFailed("cannot find %d: %s" % (k, ps[idx].desc))

    # where non-key or (non-key and non-key)
    stmt = "select prod_desc, prod_price from product \
             where prod_cat = 100 \
                or (prod_price <= %f \
               and  prod_price >= %f)" % (ps[idx].price+0.001, ps[idx].price-0.001)
    with c.execute(stmt) as cur:
        if not cur.ok():
          raise db.TestFailed("not ok %d: %s" % (cur.code(),cur.details()))
        n=0
        found=0
        for row in cur:
            n+=1
            if row.field(0) == ps[idx].desc:
               found=1
        if n<1:
          raise db.TestFailed("found nothing")
        if not found:
          raise db.TestFailed("cannot find %d: %s" % (k, ps[idx].desc))
        
# select atts where one att
def attswhere1att(c):

    print "RUNNING TEST 'attswhere1att'"

    idx = random.randint(1,len(ps)-1)
    # print "LENGTH: %d, idx: %d" % (len(ps), idx)
    k = ps[idx].key

    # 1 att where 1 condition
    stmt = "select prod_price from product where prod_desc = '%s'" % ps[idx].desc
    with c.execute(stmt) as cur:
        if not cur.ok():
          raise db.TestFailed("cannot find %d: %s" % (k,cur.details()))
        n=0
        for row in cur:
            n+=1

        if n < 1:
           raise db.TestFailed("cannot find product %d: %s" % (k, ps[idx].desc))

    # 2 atts where 1 condition
    stmt = "select prod_desc, prod_price from product where prod_desc = '%s'" % ps[idx].desc
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
    stmt = "select prod_key, prod_price from product \
             where prod_price <= %f \
               and prod_price >= %f" % (ps[idx].price+0.0001, ps[idx].price-0.0001)
    with c.execute(stmt) as cur:
        if not cur.ok():
          raise db.TestFailed("cannot find %d: %s" % (k,cur.details()))
        n=0
        for row in cur:
            if k == row.field(0):
               n+=1
               if row.field(1) != ps[idx].price:
                  raise db.TestFailed("wrong price %d: %f" % (k, row.field(0)))
        if n < 1:
           raise db.TestFailed("did not find product %d" % k)

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

# select primary key where att IN
def wherein1att(c):

    print "RUNNING TEST 'wherein1att'"

    idx = random.randint(1,len(ps)-1)
    k = ps[idx].key
    d1 = ps[idx].desc

    l = ps[idx-1].key
    d2 = ps[idx-1].desc

    if idx < len(ps)-1:
       m = ps[idx+1].key
       d3 = ps[idx+1].desc
    else:
       m = ps[idx-2].key
       d3 = ps[idx-2].desc

    # select key where att in (...)
    stmt = "select prod_key from product where prod_desc in ('%s', '%s', '%s')" % (d1,d2,d3)
    with c.execute(stmt) as cur:
        if not cur.ok():
          raise db.TestFailed("cannot find %d (%d): %s" % (k, cur.code(), cur.details()))
        n=0
        for row in cur:
            n+=1
        if n < 1:
            raise db.TestFailed("nothing found")

    # select att where att in (...)
    stmt = "select prod_desc from product where prod_desc in ('%s', '%s', '%s')" % (d1,d2,d3)
    with c.execute(stmt) as cur:
        if not cur.ok():
          raise db.TestFailed("cannot find %d (%d): %s" % (k, cur.code(), cur.details()))
        for row in cur:
            if row.field(0) != d1 and row.field(0) != d2 and row.field(0) != d3:
                raise db.TestFailed("wrong product: %d,%d,%d != %d" % (k,l,m,row.field(0)))
        if n < 1:
            raise db.TestFailed("nothing found")

    # select atts where att in (...)
    stmt = "select prod_desc, prod_cat from product \
             where prod_desc in ('%s', '%s', '%s')" % (d1,d2,d3)
    with c.execute(stmt) as cur:
        if not cur.ok():
          raise db.TestFailed("cannot find %d (%d): %s" % (k, cur.code(), cur.details()))
        n=0
        for row in cur:
            n+=1
            if row.field(0) != d1 and row.field(0) != d2 and row.field(0) != d3:
                raise db.TestFailed("wrong product: %d,%d,%d != %d" % (k,l,m,row.field(0)))
        if n < 1:
            raise db.TestFailed("nothing found")

# select primary key where att IN
def whereinatts(c):

    print "RUNNING TEST 'whereinatts'"

    idx = random.randint(2,len(ps)-1)
    k = ps[idx].key
    d1 = ps[idx].desc
    c1 = ps[idx].cat

    l = ps[idx-1].key
    d2 = ps[idx-1].desc
    c2 = ps[idx-1].cat

    if idx < len(ps)-1:
       m = ps[idx+1].key
       d3 = ps[idx+1].desc
       c3 = ps[idx+1].cat
    else:
       m = ps[idx-2].key
       d3 = ps[idx-2].desc
       c3 = ps[idx-2].cat

    # select atts where att in (...)
    stmt = "select prod_desc, prod_price from product where prod_desc in ('%s', '%s', '%s')" % (d1,d2,d3)
    with c.execute(stmt) as cur:
        if not cur.ok():
          raise db.TestFailed("cannot find %d (%d): %s" % (k, cur.code(), cur.details()))
        n=0
        for row in cur:
            n+=1
            if row.field(0) != d1 and \
               row.field(0) != d2 and \
               row.field(0) != d3:
                raise db.TestFailed("wrong desc selected: %d" % row.field(0))
        if n < 1:
            raise db.TestFailed("nothing found")

    # select atts where att in (...) and key in
    stmt = "select prod_desc, prod_price from product \
             where prod_desc in ('%s', '%s', '%s')    \
               and prod_key in (%d, %d, %d)" % (d1,d2,d3,k,l,m)
    with c.execute(stmt) as cur:
        if not cur.ok():
          raise db.TestFailed("cannot find %d (%d): %s" % (k, cur.code(), cur.details()))
        n=0
        for row in cur:
            n+=1
            if row.field(0) != d1 and \
               row.field(0) != d2 and \
               row.field(0) != d3:
                raise db.TestFailed("wrong desc selected: %d" % row.field(0))
        if n < 1:
            raise db.TestFailed("nothing found")

    # this one causes a mem leak in filter -- but why???
    # select atts where att in (...) and att in
    stmt = "select prod_desc, prod_cat, prod_price from product \
             where prod_desc in ('%s', '%s', '%s')    \
               and prod_cat in (%d, %d, %d)" % (d1,d2,d3,c1,c2,c3)
    with c.execute(stmt) as cur:
        if not cur.ok():
          raise db.TestFailed("cannot find %d (%d): %s" % (k, cur.code(), cur.details()))
        n=0
        for row in cur:
            n+=1
            if row.field(0) != d1 and \
               row.field(0) != d2 and \
               row.field(0) != d3:
                raise db.TestFailed("wrong desc selected: %d" % row.field(0))
            if row.field(1) != c1 and \
               row.field(1) != c2 and \
               row.field(1) != c3:
                raise db.TestFailed("wrong cat selected: %d" % row.field(1))
        if n < 1:
            raise db.TestFailed("nothing found")

if __name__ == "__main__":
    with now.Connection("localhost", "55505", None, None) as c:
        (ps, cs, es) = db.loadDB(c, "db100")

        vidwherevid(c)
        attswherevid(c)
        vidwhere1att(c)
        attswhere1att(c)
        vidwhereatts(c)
        attswhereatts(c)
        whereinvid(c)
        wherein1att(c)
        whereinatts(c)

        print "PASSED"
