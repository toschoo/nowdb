#!/usr/bin/env python

# to be run after basicsmoke

import now
import db
import datetime
import time
import random

IT = 10

# simple expressions on vertex
def simplevrtx(c):

    print "RUNNING TEST 'simplevrtx'"

    idx = random.randint(1,len(ps)-1)
    k = ps[idx].key
    p = ps[idx].price

    # very simple
    stmt = "select 1 from product where prod_key = %d" % k
    with c.execute(stmt) as cur:
        if not cur.ok():
          raise db.TestFailed("cannot find %d: %s" % (k,cur.details()))
        n=0
        for row in cur:
            n+=1
            if row.field(0) != 1:
               raise db.TestFailed("wrong value %d: %d" % (k, row.field(0)))
        if n != 1:
           raise db.TestFailed("wrong number of rows: %d" % (n))

    # calculator: nice would be a calculator that accepts
    #             randomly generated formulas
    a = random.randint(0,1000)
    b = random.randint(1,1000)
    if random.randint(0,2) == 0:
       a *= -1
    r = ((p + a) / b)**2
    stmt = "select prod_price, \
                 ((prod_price + %d)/%d)^2 \
              from product where prod_key = %d" % (a,b,k)
    with c.execute(stmt) as cur:
        if not cur.ok():
          raise db.TestFailed("cannot find %d: %s" % (k,cur.details()))
        n=0
        for row in cur:
            n+=1
            print "%f ?= %f" % (r, row.field(1))
            if row.field(0) != ps[idx].price:
               raise db.TestFailed("wrong value %d: %d" % (k, row.field(0)))
        if n != 1:
           raise db.TestFailed("wrong number of rows: %d" % (n))

    # round up prod_price to 1 digit
    stmt = "select prod_price, \
                   ceil(prod_price * 10)/10, \
                   floor(prod_price * 10)/10, \
                   round(prod_price * 10)/10 \
              from product where prod_key = %d" % k
    with c.execute(stmt) as cur:
        if not cur.ok():
          raise db.TestFailed("cannot find %d: %s" % (k,cur.details()))
        n=0
        for row in cur:
            n+=1
	    print "%f --> %f" % (row.field(0), row.field(1))
            if row.field(0) != ps[idx].price:
               raise db.TestFailed("wrong value %f: %f" % (k, row.field(0)))
            if row.field(1) < ps[idx].price or \
               row.field(1) > ps[idx].price + 0.1:
               raise db.TestFailed("wrong ceiling %f: %f" % (ps[idx].price, row.field(1)))
            if row.field(2) < ps[idx].price - 0.1 or \
               row.field(2) > ps[idx].price:
               raise db.TestFailed("wrong flooring %f: %f" % (ps[idx].price, row.field(2)))
            if row.field(3) < ps[idx].price - 0.1 or \
               row.field(3) > ps[idx].price + 0.1:
               raise db.TestFailed("wrong rounding %f: %f" % (ps[idx].price, row.field(3)))
        if n != 1:
           raise db.TestFailed("wrong number of rows: %d" % (n))

# simple expressions on edge
def simpleedge(c):

    print "RUNNING TEST 'simpleedge'"

    idx = random.randint(1,len(es)-1)
    o = es[idx].origin
    d = es[idx].destin
    w1 = es[idx].weight
    w2 = es[idx].weight2

    # very simple
    stmt = "select 1 from sales  \
             where edge = 'buys' \
               and origin = %d   \
               and destin = %d" % (o, d)
    with c.execute(stmt) as cur:
        if not cur.ok():
          raise db.TestFailed("cannot find %d: %s" % (cur.code(),cur.details()))
        n=0
        for row in cur:
            n+=1
            if row.field(0) != 1:
               raise db.TestFailed("wrong value %d: %d" % (cur.code(), row.field(0)))
        if n < 1:
           raise db.TestFailed("nothing found: %d" % (n))

    # calculator: nice would be a calculator that accepts
    #             randomly generated formulas
    a = float(random.randint(0,1000))
    b = float(random.randint(1,1000))
    if random.randint(0,2) == 0:
       a *= -1
    stmt = "select weight, \
                 ((weight + %f)/%f)^2 \
              from sales \
             where edge ='buys' \
               and origin = %d" % (a,b,o)
             #  and destin = %d" % (a,b,o,d)
    with c.execute(stmt) as cur:
        if not cur.ok():
          raise db.TestFailed("cannot find %d: %s" % (cur.code(),cur.details()))
        for row in cur:

            r = ((row.field(0) + a) / b)**2

            print "%f ?= %f" % (r, row.field(1))

            if row.field(1) != r:
               raise db.TestFailed("wrong value %f: %f" % (r, row.field(1)))

    # round up weight2 to 1 digit
    stmt = "select weight2, \
                   ceil(weight2 * 10)/10, \
                   floor(weight2 * 10)/10, \
                   round(weight2 * 10)/10 \
              from sales \
             where edge = 'buys' \
               and origin=%d" % o
               # and destin=%d" % (o,d)
    with c.execute(stmt) as cur:
        if not cur.ok():
          raise db.TestFailed("cannot find: %d %s" % (cur.code(),cur.details()))
        for row in cur:
	    print "%f --> %f" % (row.field(0), row.field(1))
            if row.field(1) < row.field(0) or \
               row.field(1) > row.field(0) + 0.1:
               raise db.TestFailed("wrong ceiling %f: %f" % (row.field(0), row.field(1)))
            if row.field(2) < row.field(0) - 0.1 or \
               row.field(2) > row.field(0):
               raise db.TestFailed("wrong flooring %f: %f" % (row.field(0), row.field(2)))
            if row.field(3) < row.field(0) - 0.1 or \
               row.field(3) > row.field(0) + 0.1:
               raise db.TestFailed("wrong rounding %f: %f" % (row.field(0), row.field(3)))

# agg expressions on vertex
def aggvrtx(c):

    print "RUNNING TEST 'aggvrtx'"

    stmt = "select sum(1), count(*) from product"
    with c.execute(stmt) as cur:
        if not cur.ok():
          raise db.TestFailed("cannot find %d: %s" % (cur.code(),cur.details()))
        n=0
        for row in cur:
            n+=1
            if row.field(0) != row.field(1):
               raise db.TestFailed("sum(1) != count(*): %d: %d" % (row.field(0), row.field(1)))
            if row.field(0) != len(ps):
               raise db.TestFailed("expected count: %d, but have %d" % (len(ps), row.field(1)))
        if n != 1:
           raise db.TestFailed("wrong number of rows: %d" % (n))

    # average
    s = 0.0
    for p in ps:
        s += p.price
    a = s/len(ps)

    stmt = "select avg(prod_price), \
                   sum(prod_price)/count(*) from product"
    with c.execute(stmt) as cur:
        if not cur.ok():
          raise db.TestFailed("cannot find %d: %s" % (cur.code(),cur.details()))
        n=0
        for row in cur:
            n+=1
            if row.field(0) != row.field(1):
               raise db.TestFailed("avg != sum/count: %f: %f" % (row.field(0), row.field(1)))
            if row.field(0) != a:
               raise db.TestFailed("expected avg: %f, but have %f" % (a, row.field(0)))
        if n != 1:
           raise db.TestFailed("wrong number of rows: %d" % (n))

    # median
    sp = sorted(ps, key=lambda p: p.price)
    l = len(ps) - 1
    if l%2 != 0: # length is even
       m = (sp[l/2].price + sp[l/2+1].price)/2
    else: # length is odd
       m = sp[(l+1)/2].price

    stmt = "select median(prod_price) from product"
    with c.execute(stmt) as cur:
        if not cur.ok():
          raise db.TestFailed("cannot find %d: %s" % (cur.code(),cur.details()))
        n=0
        for row in cur:
            n+=1
            print "median price: %f" % row.field(0)
            if row.field(0) != m:
               raise db.TestFailed("expected median: %f, but have %f" % (m, row.field(0)))
        if n != 1:
           raise db.TestFailed("wrong number of rows: %d" % (n))

# agg expressions on edge
def aggedge(c):

    print "RUNNING TEST 'aggedge'"

    stmt = "select sum(1), count(*) from sales"
    with c.execute(stmt) as cur:
        if not cur.ok():
          raise db.TestFailed("cannot find %d: %s" % (cur.code(),cur.details()))
        n=0
        for row in cur:
            n+=1
            if row.field(0) != row.field(1):
               raise db.TestFailed("sum(1) != count(*): %d: %d" % (row.field(0), row.field(1)))
            if row.field(0) != len(es):
               raise db.TestFailed("expected count: %d, but have %d" % (len(es), row.field(1)))
        if n != 1:
           raise db.TestFailed("wrong number of rows: %d" % (n))

    # average
    s = 0.0
    for e in es:
        s += e.weight2
    a = s/len(es)

    stmt = "select avg(weight2), sum(weight2)/count(*) from sales"
    with c.execute(stmt) as cur:
        if not cur.ok():
          raise db.TestFailed("cannot find %d: %s" % (cur.code(),cur.details()))
        n=0
        for row in cur:
            n+=1
            if row.field(0) != row.field(1):
               raise db.TestFailed("avg(w2) != sum(w2)/count(*): %f: %f" % (row.field(0), row.field(1)))
            if row.field(0) != a:
               raise db.TestFailed("expected avg: %f, but have %f" % (a, row.field(1)))
        if n != 1:
           raise db.TestFailed("wrong number of rows: %d" % (n))

    # median
    se = sorted(es, key=lambda e: e.weight2)
    l = len(es) - 1
    if l%2 != 0: # length is even
       m = (se[l/2].weight2 + se[l/2+1].weight2)/2
    else: # length is odd
       m = se[(l+1)/2].weight2

    stmt = "select median(weight2) from sales"
    with c.execute(stmt) as cur:
        if not cur.ok():
          raise db.TestFailed("cannot find %d: %s" % (cur.code(),cur.details()))
        n=0
        for row in cur:
            n+=1
            print "median weight: %f" % row.field(0)
            if row.field(0) != m:
               raise db.TestFailed("expected median: %f, but have %f" % (m, row.field(0)))
        if n != 1:
           raise db.TestFailed("wrong number of rows: %d" % (n))

# grouped expressions on edge
def grpedge(c):

    print "RUNNING TEST 'grpedge'"

    stmt = "select edge, origin, sum(1), count(*) \
              from sales group by edge, origin"
    with c.execute(stmt) as cur:
        if not cur.ok():
          raise db.TestFailed("cannot find %d: %s" % (cur.code(),cur.details()))
        for row in cur:
            if row.field(2) != row.field(3):
               raise db.TestFailed("sum(1) != count(*): %d: %d" % (row.field(0), row.field(1)))

            stmt = "select count(*) from sales \
                     where edge='%s' and origin=%d" % (row.field(0), row.field(1))
            print "executing %s" % stmt
            with c.execute(stmt) as cur2:
                if not cur2.ok():
                  raise db.TestFailed("cur2 not ok: %d, %s" % (cur2.code(),cur2.details()))
                for rr in cur2:
                    if rr.field(0) != row.field(2):
                       raise db.TestFailed("expected count: %d, but have %d" % (rr.field(0), row.field(2)))

    stmt = "select edge, origin, avg(weight), sum(tofloat(weight))/count(*) \
              from sales group by edge, origin"
    with c.execute(stmt) as cur:
        if not cur.ok():
          raise db.TestFailed("cannot find %d: %s" % (cur.code(),cur.details()))
        for row in cur:
            if row.field(2) != row.field(3):
               raise db.TestFailed("avg(w) != sum(w)/count(*): %f: %f" % (row.field(2), row.field(3)))

            stmt = "select avg(weight) from sales \
                     where edge='%s' and origin=%d" % (row.field(0), row.field(1))
            print "executing %s" % stmt
            with c.execute(stmt) as cur2:
                if not cur2.ok():
                  raise db.TestFailed("cur2 not ok: %d, %s" % (cur2.code(),cur2.details()))
                for rr in cur2:
                    if rr.field(0) != row.field(2):
                       raise db.TestFailed("expected avg: %f, but have %f" % (rr.field(0), row.field(2)))

    stmt = "select edge, origin, median(weight) \
              from sales group by edge, origin"
    with c.execute(stmt) as cur:
        if not cur.ok():
          raise db.TestFailed("cannot find %d: %s" % (cur.code(),cur.details()))
        for row in cur:
            stmt = "select median(weight) from sales \
                     where edge='%s' and origin=%d" % (row.field(0), row.field(1))
            print "executing %s" % stmt
            with c.execute(stmt) as cur2:
                if not cur2.ok():
                  raise db.TestFailed("cur2 not ok: %d, %s" % (cur2.code(),cur2.details()))
                for rr in cur2:
                    if rr.field(0) != row.field(2):
                       raise db.TestFailed("expected median: %f, but have %f" % (rr.field(0), row.field(2)))

# Beloved fibonacci
def fibonacci(one,two,x):

    if x == 0:
       return

    idx = random.randint(1,len(ps)-1)
    k = ps[idx].key

    stmt = "select %d + %d from product \
             where prod_key = %d" % (one, two, k)
    with c.execute(stmt) as cur:
        if not cur.ok():
          raise db.TestFailed("cannot find %d: %s" % (k,cur.details()))
        n=0
        for row in cur:
            n+=1
            if row.field(0) != one + two:
               raise db.TestFailed("wrong value %d + %d: %d" % (one, two, row.field(0)))
            print "%d" % row.field(0)
            fibonacci(two, row.field(0), x-1)
        if n != 1:
           raise db.TestFailed("wrong number of rows: %d" % (n))

def fibotest(x):

    print "RUNNING TEST 'fibonacci %d'" % x

    print "1\n1"
    fibonacci(1,1,x)

if __name__ == "__main__":
    with now.Connection("localhost", "55505", None, None) as c:
        (ps, cs, es) = db.loadDB(c, "db100")

        for i in range(IT):
            simplevrtx(c)
            simpleedge(c)

        fibotest(20)

        aggvrtx(c)
        aggedge(c)
        grpedge(c)

        print "PASSED"
