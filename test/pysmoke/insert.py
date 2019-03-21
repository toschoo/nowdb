#!/usr/bin/env python

# to be run after basicsmoke

import now
import db
import datetime
import random

ERR_DUP_KEY = 27

def getNextKey(c):
    with c.execute("select max(prod_key) from product2") as r:
       if r.code() == now.EOF:
          return 50001
           
       if not r.ok():
          raise db.TestFailed("cannot get next key %d: %s" % (r.code(),r.details()))

       for row in r:
           if row.field(0) is None:
              return 50002
           return row.field(0) + 1

def getProduct(p):
    i = random.randint(0,len(p)-1)
    return p[i]

def getClient(c):
    i = random.randint(0,len(c)-1)
    return c[i]

def prefill(c):

    print "RUNNING TEST 'prefill'"

    stmt = "insert into product2 (prod_key, prod_desc, prod_cat, prod_packing, prod_price) \
                                 (%d, 'product %s', 1, 2, 1.99)"
    for i in range(1,100):
        k = getNextKey(c)
        with c.execute(stmt % (k, str(k))) as r:
           if not r.ok():
              raise db.TestFailed("cannot insert %d: %s" % (k,r.details()))

def dupkeyvertex(c):

    print "RUNNING TEST 'dupkeyvertex'"

    k = getNextKey(c)
    stmt = "insert into product2 (prod_key, prod_desc, prod_cat, prod_price) \
                                 (%d, 'product %s', 2, 1.99)" % (k, str(k))
    with c.execute(stmt) as r:
       if not r.ok():
          raise db.TestFailed("cannot insert %d: %s" % (k,r.details()))

    stmt = "select prod_desc from product2 where prod_key = %d" % k
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

    stmt = "insert into product2 (prod_key, prod_desc, prod_price) \
                                 (%d, 'product %s', 1.99)" % (k, str(k))
    with c.execute(stmt) as r:
       if r.ok():
          raise db.TestFailed("I can insert %d twice!" % k)
       if r.code() != ERR_DUP_KEY:
          raise db.TestFailed("wrong error: %s" % r.details())

    stmt = "select prod_desc from product2 where prod_key = %d" % k
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

    k = getNextKey(c)
    stmt = "insert into product2 (prod_key, prod_desc, prod_price) \
                                 (%d, 'product %s', 'this is not a number')" % (k, str(k))
    with c.execute(stmt) as r:
       if r.ok():
          raise db.TestFailed("I can insert a vertex with wrong type")
       print r.details()

    stmt = "select prod_desc from product2 where prod_key = %d" % k
    with c.execute(stmt) as cur:
        if cur.ok():
          raise db.TestFailed("%d inserted in spite of error" % k)
        if cur.code() != now.EOF:
          raise db.TestFailed("Expecting EOF, but have %d: %s" % (cur.code(),cur.details()))

def insertallvertex(c):

    print "RUNNING TEST 'insertallvertex'"

    # insert with field list (partial)
    k = getNextKey(c)
    dsc = "product %d" % k
    price = 1.59
    stmt = "insert into product2 (prod_key, prod_desc, prod_price) \
                                 (%d, 'product %s', 1.59)" % (k, str(k))
    with c.execute(stmt) as r:
       if not r.ok():
          raise db.TestFailed("cannot insert %d: %s" % (k,r.details()))

    stmt = "select prod_desc from product2 where prod_key = %d" % k
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

    # select a non-existing prop
    stmt = "select prod_desc, prod_cat from product2 where prod_key = %d" % k
    with c.execute(stmt) as cur:
        if not cur.ok():
          raise db.TestFailed("cannot find %d: %s" % (cur.code(),cur.details()))
        n=0
        for row in cur:
            if n != 0:
               raise db.TestFailed("multiple rows for %d" % k)
            n+=1
            if row.field(0) != ("product %d" % k):
               raise db.TestFailed("wrong product %d: %s" % (k, row.field(0)))
            if row.field(1) is not None:
               raise db.TestFailed("NOT NULL %d: %s" % (k, row.field(1)))
            print "%s, %s" % (row.field(0), row.field(1))

    # select only a non-existing prop
    stmt = "select prod_cat from product2 where prod_key = %d" % k
    with c.execute(stmt) as cur:
        if not cur.ok():
          raise db.TestFailed("cannot find %d: %s" % (cur.code(),cur.details()))
        n=0
        for row in cur:
            if n != 0:
               raise db.TestFailed("multiple rows for %d" % k)
            n+=1
            if row.field(0) is not None:
               raise db.TestFailed("NOT NULL %d: %s" % (k, row.field(0)))
            print "%s" % (row.field(0))

    # select non-existing prop with complex where
    stmt = "select prod_cat, prod_desc from product2 \
             where prod_key = %d \
                or prod_desc = 'product %d'" % (k,k)
    with c.execute(stmt) as cur:
        if not cur.ok():
          raise db.TestFailed("cannot find %d: %s" % (cur.code(),cur.details()))
        n=0
        for row in cur:
            n+=1
            print "%s;%s" % (row.field(0), row.field(1))
            if row.field(0) is not None:
               raise db.TestFailed("NOT NULL %d: %s" % (k, row.field(0)))
            if row.field(1) != ('product %d' % k):
               raise db.TestFailed("wrong guy %s: %s" % ('product %d' % k, row.field(1)))

    # select non-existing prop repeatedly
    stmt = "select prod_desc, prod_cat, prod_cat, prod_price, prod_cat \
              from product2 where prod_key = %d" % k
    with c.execute(stmt) as cur:
        if not cur.ok():
          raise db.TestFailed("cannot find %d: %s" % (cur.code(),cur.details()))
        n=0
        for row in cur:
            if n != 0:
               raise db.TestFailed("multiple rows for %d" % k)
            n+=1
            if row.field(0) != ("product %d" % k):
               raise db.TestFailed("wrong product %d: %s" % (k, row.field(0)))
            if row.field(1) is not None:
               raise db.TestFailed("NOT NULL (1) %d: %s" % (k, row.field(1)))
            if row.field(2) is not None:
               raise db.TestFailed("NOT NULL (2) %d: %s" % (k, row.field(1)))
            if row.field(4) is not None:
               raise db.TestFailed("NOT NULL (3) %d: %s" % (k, row.field(1)))
            if row.field(3) != 1.59:
               raise db.TestFailed("wrong price %d: %f (%f)" % (k, row.field(3), price))
            print "%s, %s" % (row.field(0), row.field(1))

    # sum over a non-existing prop
    stmt = "select avg(prod_cat) from product2 where prod_key = %d" % k
    with c.execute(stmt) as cur:
        if not cur.ok():
          raise db.TestFailed("cannot find %d: %s" % (cur.code(),cur.details()))
        for row in cur:
            if row.field(0) != 0:
               raise db.TestFailed("NULL field not ignored %d: %s" % (k, row.field(0)))
            print "%s" % (row.field(0))

    # sum over existing props
    cnt = 0
    stmt = "select avg(prod_cat) from product2 where prod_desc != '%s'" % dsc
    with c.execute(stmt) as cur:
        if not cur.ok():
          raise db.TestFailed("cannot find %d: %s" % (cur.code(),cur.details()))
        for row in cur:
            if row.field(0) == 0:
               raise db.TestFailed("all cats are zero for %d: %s" % (k, row.field(0)))
            print "avg cat: %d" % (row.field(0))
            cnt = row.field(0)

    # sum over existing and non-existing props
    stmt = "select avg(prod_cat) from product2"
    with c.execute(stmt) as cur:
        if not cur.ok():
          raise db.TestFailed("cannot find %d: %s" % (cur.code(),cur.details()))
        for row in cur:
            if row.field(0) != cnt:
               raise db.TestFailed("NULL not ignored %d: %d (%d)" % (k, row.field(0), cnt))
            print "avg cat: %d (%d)" % (row.field(0), cnt)

    # stddev over existing props
    std = 0.0
    stmt = "select stddev(coal(prod_cat,0)) from product2 where prod_desc != '%s'" % dsc
    print stmt
    with c.execute(stmt) as cur:
        if not cur.ok():
          raise db.TestFailed("cannot find %d: %s" % (cur.code(),cur.details()))
        for row in cur:
            if row.field(0) == 0:
               raise db.TestFailed("all cats are zero for %d: %f" % (k, row.field(0)))
            print "stddev cat: %d" % (row.field(0))
            std = row.field(0)

    # stddev over existing and non existing props
    """
    stmt = "select stddev(coal(prod_cat,0)) from product2"
    with c.execute(stmt) as cur:
        if not cur.ok():
          raise db.TestFailed("cannot find %d: %s" % (cur.code(),cur.details()))
        for row in cur:
            if row.field(0) != std:
               raise db.TestFailed("stddev not correct %d: %f / %f" % (k, row.field(0), std))
            print "stddev cat: %d" % (row.field(0))
    """

    # median over existing props
    md = 0.0
    stmt = "select median(prod_cat) from product2 where prod_desc != '%s'" % dsc
    with c.execute(stmt) as cur:
        if not cur.ok():
          raise db.TestFailed("cannot find %d: %s" % (cur.code(),cur.details()))
        for row in cur:
            if row.field(0) == 0:
               raise db.TestFailed("all cats are zero for %d: %f" % (k, row.field(0)))
            print "median cat: %d" % (row.field(0))
            md = row.field(0)

    # median over existing and non-existing props
    stmt = "select median(prod_cat) from product2" 
    with c.execute(stmt) as cur:
        if not cur.ok():
          raise db.TestFailed("cannot find %d: %s" % (cur.code(),cur.details()))
        for row in cur:
            if row.field(0) != md:
               raise db.TestFailed("median no correct %d: %f" % (k, row.field(0)))
            print "median cat: %d" % (row.field(0))

    # filter a non-existing prop
    stmt = "select prod_key from product2 where prod_cat = 3"
    with c.execute(stmt) as cur:
        if cur.ok():
           for row in cur:
              if row.field(0) == k:
                 raise db.TestFailed("NULL field filtered %d: %s" % (k, row.field(0)))
              print "%s" % (row.field(0))

    # filter out a non-existing prop
    stmt = "select prod_key from product2 where prod_cat != 3"
    with c.execute(stmt) as cur:
        if cur.ok():
           for row in cur:
              if row.field(0) == k:
                 raise db.TestFailed("NULL field filtered %d: %s" % (k, row.field(0)))

    # filter a non-existing prop using is null
    stmt = "select prod_key, prod_cat from product2 where prod_cat is null"
    with c.execute(stmt) as cur:
        if not cur.ok():
               raise db.TestFailed("not ok: %d (%s)" % (cur.code(), cur.details()))
        found=False
        n=0
        for row in cur:
            n+=1
            if row.field(0) == k:
               found=True
            if row.field(1) is not None:
               raise db.TestFailed("NULL is not None")
            print "NULL: %d" % row.field(0)
        if not found:
           raise db.TestFailed("NULL not found for %d" % k)

    # filter out non-existing prop using is not null
    stmt = "select prod_key, prod_cat from product2 where prod_cat is not null"
    with c.execute(stmt) as cur:
        if not cur.ok():
               raise db.TestFailed("not ok: %d (%s)" % (cur.code(), cur.details()))
        m=0
        for row in cur:
            m+=1
            if row.field(0) == k:
               raise db.TestFailed("NULL found with NOT NULL: %d" % k)
            if row.field(1) is None:
               raise db.TestFailed("not NULL is None")
        if m < 99:
           raise db.TestFailed("Couldn't find some of them (%d)" % m)

    # case with null
    mycase = "case " \
             "   when prod_cat is null or prod_packing is null then 0 " \
             "   when prod_cat = 1 and prod_packing is not null then prod_cat * 100 + prod_packing " \
             "   when prod_cat = 2 and prod_packing is not null then prod_cat * 1000 + prod_packing " \
             "   when prod_cat = 3 and prod_packing is not null then prod_cat * 10000 + prod_packing " \
             "   else prod_packing*(-1) " \
             "end"

    stmt = "select %s from product2" % mycase
    print stmt

    sm = 0
    for p in ps:
        if p.cat == 1:
           sm += p.cat * 100 + p.packing
        elif p.cat == 2:
           sm += p.cat * 1000 + p.packing
        elif p.cat == 3:
           sm += p.cat * 10000 + p.packing
        else:
           sm += p.packing * (-1)

    print sm

    # case with null in where
    mycase = "case when prod_cat is null then true else false end"

    stmt = "select prod_key, prod_cat from product2 where %s" % mycase
    print stmt

    with c.execute(stmt) as cur:
        if not cur.ok():
               raise db.TestFailed("not ok: %d (%s)" % (cur.code(), cur.details()))
        for row in cur:
            if row.field(1) is not None:
               raise db.TestFailed("not null %d: %s" % (row.field(0), row.field(1)))
            print "%d: %s" % (row.field(0), row.field(1))

    # case with null in where, logic reversed
    mycase = "case when prod_cat is not null then true else false end"

    stmt = "select prod_key, prod_cat from product2 where %s" % mycase
    print stmt

    with c.execute(stmt) as cur:
        if not cur.ok():
               raise db.TestFailed("not ok: %d (%s)" % (cur.code(), cur.details()))
        for row in cur:
            if row.field(1) is None:
               raise db.TestFailed("null %d: %s" % (row.field(0), row.field(1)))
            print "%d: %s" % (row.field(0), row.field(1))

    # coalesce non-existing prop
    stmt = "select prod_desc, coal(prod_cat, 6) from product2 where prod_key = %d" % k
    with c.execute(stmt) as cur:
        if not cur.ok():
          raise db.TestFailed("cannot find %d: %s" % (cur.code(),cur.details()))
        n=0
        for row in cur:
            if n != 0:
               raise db.TestFailed("multiple rows for %d" % k)
            n+=1
            if row.field(0) != ("product %d" % k):
               raise db.TestFailed("wrong product %d: %s" % (k, row.field(0)))
            if row.field(1) != 6:
               raise db.TestFailed("%d: %d != 6" % (k, row.field(1)))
            print "%s, %s" % (row.field(0), row.field(1))

    # coalesce on 2 non-existing props
    stmt = "select prod_desc, coal(prod_cat, prod_packing, 6) \
              from product2 where prod_key = %d" % k
    with c.execute(stmt) as cur:
        if not cur.ok():
          raise db.TestFailed("cannot find %d: %s" % (cur.code(),cur.details()))
        n=0
        for row in cur:
            if n != 0:
               raise db.TestFailed("multiple rows for %d" % k)
            n+=1
            if row.field(0) != ("product %d" % k):
               raise db.TestFailed("wrong product %d: %s" % (k, row.field(0)))
            if row.field(1) != 6:
               raise db.TestFailed("%d: %d != 6" % (k, row.field(1)))
            print "%s, %s" % (row.field(0), row.field(1))

    # coalesce all null
    stmt = "select prod_desc, coal(prod_cat, prod_packing) \
              from product2 where prod_key = %d" % k
    with c.execute(stmt) as cur:
        if not cur.ok():
          raise db.TestFailed("cannot find %d: %s" % (cur.code(),cur.details()))
        n=0
        for row in cur:
            if n != 0:
               raise db.TestFailed("multiple rows for %d" % k)
            n+=1
            if row.field(0) != ("product %d" % k):
               raise db.TestFailed("wrong product %d: %s" % (k, row.field(0)))
            if row.field(1) is not None:
               raise db.TestFailed("%d: not null" % (k, row.field(1)))
            print "%s, %s" % (row.field(0), row.field(1))

    # coalesce non-existing prop in where
    stmt = "select prod_key, prod_cat from product2 \
             where coal(prod_cat, 6) = 6"
    with c.execute(stmt) as cur:
        if not cur.ok():
          raise db.TestFailed("cannot find %d: %d (%s)" % (k, cur.code(),cur.details()))
        for row in cur:
            if not cur.ok():
               raise db.TestFailed("not ok: %d (%s)" % (cur.code(), cur.details()))
            if row.field(1) is not None:
               raise db.TestFailed("wrong product %d: %s" % (k, row.field(0)))

    # insert with field list (without desc)
    k = getNextKey(c)
    dsc = "product %d" % k
    price = 1.69
    stmt = "insert into product2 (prod_key, prod_price) \
                                 (%d, 1.69)" % k
    with c.execute(stmt) as r:
       if not r.ok():
          raise db.TestFailed("cannot insert %d: %s" % (k,r.details()))

    # is null on text prop
    stmt = "select prod_key, prod_desc from product2 \
             where prod_desc is null"
    found=False
    with c.execute(stmt) as cur:
        if not cur.ok():
          raise db.TestFailed("cannot find %d: %d (%s)" % (k, cur.code(),cur.details()))
        for row in cur:
            if not cur.ok():
               raise db.TestFailed("not ok: %d (%s)" % (cur.code(), cur.details()))
            if row.field(1) is not None:
               raise db.TestFailed("wrong product %d: %s" % (k, row.field(0)))
            if row.field(0) == k:
               found=True
        if not found:
           raise db.TestFailed("product %d not found" % k)

    # is not null on text prop
    stmt = "select prod_key, prod_desc from product2 \
             where prod_desc is not null"
    with c.execute(stmt) as cur:
        if not cur.ok():
          raise db.TestFailed("cannot find %d: %d (%s)" % (k, cur.code(),cur.details()))
        for row in cur:
            if not cur.ok():
               raise db.TestFailed("not ok: %d (%s)" % (cur.code(), cur.details()))
            if row.field(1) is None:
               raise db.TestFailed("is null %d: %s" % (k, row.field(0)))
            if row.field(0) == k:
               raise db.TestFailed("wrong product %d: %s" % (k, row.field(0)))

    # coalesce non-existing prop in where
    stmt = "select prod_key, prod_desc from product2 \
             where coal(prod_desc, ' ') = ' '"
    with c.execute(stmt) as cur:
        if not cur.ok():
          raise db.TestFailed("cannot find %d: %d (%s)" % (k, cur.code(),cur.details()))
        for row in cur:
            if not cur.ok():
               raise db.TestFailed("not ok: %d (%s)" % (cur.code(), cur.details()))
            if row.field(1) is not None:
               raise db.TestFailed("wrong product %d: %s" % (k, row.field(0)))
        if not found:
           raise db.TestFailed("product %d not found" % k)

    # coalesce non-existing exclude null
    stmt = "select prod_key, prod_desc from product2 \
             where coal(prod_desc, ' ') != ' '"
    with c.execute(stmt) as cur:
        if not cur.ok():
          raise db.TestFailed("cannot find %d: %d (%s)" % (k, cur.code(),cur.details()))
        for row in cur:
            if not cur.ok():
               raise db.TestFailed("not ok: %d (%s)" % (cur.code(), cur.details()))
            if row.field(1) is None:
               raise db.TestFailed("wrong product %d: %s" % (k, row.field(0)))
        if not found:
           raise db.TestFailed("product %d not found" % k)

    # insert with field list (complete)
    k = getNextKey(c)
    stmt = "insert into product2 (prod_key, prod_desc, prod_cat, prod_packing, prod_price) \
                                 (%d, 'product %s', 1, 3, 1.59)" % (k, str(k))
    with c.execute(stmt) as r:
       if not r.ok():
          raise db.TestFailed("cannot insert %d: %s" % (k,r.details()))

    stmt = "select prod_desc from product2 where prod_key = %d" % k
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
    k = getNextKey(c)
    stmt = "insert into product2 (prod_cat, prod_desc, prod_packing, prod_key, prod_price) \
                                 (1, 'product %s', 3, %d, 1.59)" % (str(k), k)
    with c.execute(stmt) as r:
       if not r.ok():
          raise db.TestFailed("cannot insert %d: %s" % (k,r.details()))

    stmt = "select prod_desc from product2 where prod_key = %d" % k
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

    # insert without field list (complete, no fields)
    k = getNextKey(c)
    stmt = "insert into product2 values (%d, 'product %s', 1, 3, 1.59)" % (k, str(k))
    with c.execute(stmt) as r:
       if not r.ok():
          raise db.TestFailed("cannot insert %d: %s" % (k,r.details()))

    stmt = "select prod_desc from product2 where prod_key = %d" % k
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

    # insert without field list (incomplete, no fields)
    k = getNextKey(c)
    stmt = "insert into product2 values (%d, 'product %s', 1, 1.59)" % (k, str(k))
    with c.execute(stmt) as r:
       if r.ok():
          raise db.TestFailed("can insert incompletely")
       print "%d: %s" % (r.code(), r.details())

    stmt = "select prod_desc from product2 where prod_key = %d" % k
    with c.execute(stmt) as cur:
        if cur.ok():
          raise db.TestFailed("%d inserted in spite of error" % k)
        if cur.code() != now.EOF:
          raise db.TestFailed("Expecting EOF, but have %d: %s" % (cur.code(),cur.details()))

    # insert without field list (complete, no fields, wrong type)
    k = getNextKey(c)
    stmt = "insert into product2 values (%d, 'product %s', 1, '3', 1.59)" % (k, str(k))
    with c.execute(stmt) as r:
       if r.ok():
          raise db.TestFailed("can insert with wrong type")
       print "%d: %s" % (r.code(), r.details())

    stmt = "select prod_desc from product2 where prod_key = %d" % k
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
    stmt = "insert into buys (origin, destin, stamp, quantity, price) \
                             (%d, %d, '%s', 1, 1.49)" % (k, p, s)
    with c.execute(stmt) as r:
       if not r.ok():
          raise db.TestFailed("cannot insert edge: %d -- %s" % (r.code(),r.details()))

    stmt = "select origin, destin, stamp from buys \
             where origin = %d \
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
    stmt = "insert into buys (origin, destin, stamp, quantity, price) \
                              (%d, %d, '%s', 1, 1.49)" % (k, p, s)
    with c.execute(stmt) as r:
       if not r.ok():
          raise db.TestFailed("cannot insert edge: %d -- %s" % (r.code(),r.details()))

    stmt = "select origin, destin, stamp from buys \
             where origin = %d \
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

    # insert without field list (complete)
    p = getProduct(ps).key
    k = getClient(cs).key
    dt = datetime.datetime.utcnow()
    s = dt.strftime(now.TIMEFORMAT)
    t = now.dt2now(dt)
    stmt = "insert into buys values (%d, %d, '%s', 1, 1.49)" % (k, p, s)
    with c.execute(stmt) as r:
       if not r.ok():
          raise db.TestFailed("cannot insert edge: %d -- %s" % (r.code(),r.details()))

    stmt = "select origin, destin, stamp from buys \
             where origin = %d \
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

    # insert without field list (incomplete)
    p = getProduct(ps).key
    k = getClient(cs).key
    dt = datetime.datetime.utcnow()
    s = dt.strftime(now.TIMEFORMAT)
    t = now.dt2now(dt)
    stmt = "insert into buys values (%d, %d, '%s', 1.49)" % (k, p, s)
    with c.execute(stmt) as r:
       if r.ok():
          raise db.TestFailed("can insert incomplete edge")
       print "%d: %s" % (r.code(), r.details())

    stmt = "select origin, destin, stamp from buys \
             where origin = %d \
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
    stmt = "insert into buys values (%d, 'my product', '%s', 1, 1.49)" % (k, s)
    with c.execute(stmt) as r:
       if r.ok():
          raise db.TestFailed("can insert edge with wrong type!")
       print "%d: %s" % (r.code(), r.details())

    stmt = "select origin, destin, stamp from buys \
             where origin = %d \
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
        (ps, cs, ss, es, vs) = db.loadDB(c, "db100")

	prefill(c)
        dupkeyvertex(c)
        failedinsert(c)
        insertallvertex(c)
        insertalledge(c,ps,cs)

        print "PASSED"
