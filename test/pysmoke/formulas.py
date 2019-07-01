#!/usr/bin/env python

# to be run after basicsmoke

import now
import db
import datetime
import time
import random
import math

IT = 10

# simple expressions on vertex
def simplevrtx(c):

    print("RUNNING TEST 'simplevrtx'")

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
            print("%f ?= %f" % (r, row.field(1)))
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
            print("%f --> %f" % (row.field(0), row.field(1)))
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

    print("RUNNING TEST 'simpleedge'")

    idx = random.randint(1,len(es)-1)

    o = es[idx].origin
    d = es[idx].destin
    w1 = es[idx].quantity
    w2 = es[idx].price

    # very simple
    stmt = "select 1 from buys  \
             where origin = %d   \
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
    stmt = "select quantity, \
                 ((quantity + %f)/%f)^2 \
              from buys \
             where origin = %d" % (a,b,o)
             #  and destin = %d" % (a,b,o,d)
    with c.execute(stmt) as cur:
        if not cur.ok():
          raise db.TestFailed("cannot find %d: %s" % (cur.code(),cur.details()))
        for row in cur:

            r = ((row.field(0) + a) / b)**2

            print("%f ?= %f" % (r, row.field(1)))

            if row.field(1) != r:
               raise db.TestFailed("wrong value %f: %f" % (r, row.field(1)))

    # round up price to 1 digit
    stmt = "select price, \
                   ceil(price * 10)/10, \
                   floor(price * 10)/10, \
                   round(price * 10)/10 \
              from buys \
             where origin=%d" % o
               # and destin=%d" % (o,d)
    with c.execute(stmt) as cur:
        if not cur.ok():
          raise db.TestFailed("cannot find: %d %s" % (cur.code(),cur.details()))
        for row in cur:
            print("%f --> %f" % (row.field(0), row.field(1)))
            if row.field(1) < row.field(0) - 0.1 or \
               row.field(1) > row.field(0) + 0.1:
               raise db.TestFailed("wrong ceiling %f: %f" % (row.field(0), row.field(1)))
            if row.field(2) < row.field(0) - 0.1 or \
               row.field(2) > row.field(0) + 0.1:
               raise db.TestFailed("wrong flooring %f: %f" % (row.field(0), row.field(2)))
            if row.field(3) < row.field(0) - 0.1 or \
               row.field(3) > row.field(0) + 0.1:
               raise db.TestFailed("wrong rounding %f: %f" % (row.field(0), row.field(3)))

# punktrechnung vor strichtrechnung
def punktvorstrich(c):

    print("RUNNING TEST 'punktvorstrich'")

    idx = random.randint(1,len(ps)-1)
    k = ps[idx].key
    p = ps[idx].price

    x=7
    print("expected: %d" % x)
    stmt = "select 3*2 + 1 from product where prod_key = %d" % k
    with c.execute(stmt) as cur:
        if not cur.ok():
          raise db.TestFailed("cannot find: %d %s" % (cur.code(),cur.details()))
        n=0
        for row in cur:
            n+=1
            if row.field(0) != x:
               raise db.TestFailed("wrong result: %d (expected: %d)" % (row.field(0), x))
        if n != 1:
               raise db.TestFailed("wrong number of results: %d (expected: 1)" % (n, 1))

    x=5
    print("expected: %d" % x)
    stmt = "select 3*2 - 1 from product where prod_key = %d" % k
    with c.execute(stmt) as cur:
        if not cur.ok():
          raise db.TestFailed("cannot find: %d %s" % (cur.code(),cur.details()))
        n=0
        for row in cur:
            n+=1
            if row.field(0) != x:
               raise db.TestFailed("wrong result: %d (expected: %d)" % (row.field(0), x))
        if n != 1:
               raise db.TestFailed("wrong number of results: %d (expected: 1)" % (n, 1))

    x=9
    print("expected: %d" % x)
    stmt = "select 3*(2 + 1) from product where prod_key = %d" % k
    with c.execute(stmt) as cur:
        if not cur.ok():
          raise db.TestFailed("cannot find: %d %s" % (cur.code(),cur.details()))
        n=0
        for row in cur:
            n+=1
            if row.field(0) != x:
               raise db.TestFailed("wrong result: %d (expected: %d)" % (row.field(0), x))
        if n != 1:
               raise db.TestFailed("wrong number of results: %d (expected: 1)" % (n, 1))

    x=3
    print("expected: %d" % x)
    stmt = "select 3*(2 - 1) from product where prod_key = %d" % k
    with c.execute(stmt) as cur:
        if not cur.ok():
          raise db.TestFailed("cannot find: %d %s" % (cur.code(),cur.details()))
        n=0
        for row in cur:
            n+=1
            if row.field(0) != x:
               raise db.TestFailed("wrong result: %d (expected: %d)" % (row.field(0), x))
        if n != 1:
               raise db.TestFailed("wrong number of results: %d (expected: 1)" % (n, 1))

    x=12
    print("expected: %f" % x)
    stmt = "select 3*2^2 from product where prod_key = %d" % k
    with c.execute(stmt) as cur:
        if not cur.ok():
          raise db.TestFailed("cannot find: %d %s" % (cur.code(),cur.details()))
        n=0
        for row in cur:
            n+=1
            if row.field(0) != x:
               raise db.TestFailed("wrong result: %f (expected: %f)" % (row.field(0), x))
        if n != 1:
               raise db.TestFailed("wrong number of results: %d (expected: 1)" % (n, 1))

    x=36
    print("expected: %f" % x)
    stmt = "select (3*2)^2 from product where prod_key = %d" % k
    with c.execute(stmt) as cur:
        if not cur.ok():
          raise db.TestFailed("cannot find: %d %s" % (cur.code(),cur.details()))
        n=0
        for row in cur:
            n+=1
            if row.field(0) != x:
               raise db.TestFailed("wrong result: %f (expected: %f)" % (row.field(0), x))
        if n != 1:
               raise db.TestFailed("wrong number of results: %d (expected: 1)" % (n, 1))

    x=3
    print("expected: %f" % x)
    stmt = "select 3*2^1/2 from product where prod_key = %d" % k
    with c.execute(stmt) as cur:
        if not cur.ok():
          raise db.TestFailed("cannot find: %d %s" % (cur.code(),cur.details()))
        n=0
        for row in cur:
            n+=1
            if row.field(0) != x:
               raise db.TestFailed("wrong result: %f (expected: %f)" % (row.field(0), x))
        if n != 1:
               raise db.TestFailed("wrong number of results: %d (expected: 1)" % (n, 1))

    x=3.0*2.0**(1.0/2.0)
    print("expected: %f" % x)
    stmt = "select 3*2^(1.0/2.0) from product where prod_key = %d" % k
    with c.execute(stmt) as cur:
        if not cur.ok():
          raise db.TestFailed("cannot find: %d %s" % (cur.code(),cur.details()))
        n=0
        for row in cur:
            n+=1
            if row.field(0) != x:
               raise db.TestFailed("wrong result: %f (expected: %f)" % (row.field(0), x))
        if n != 1:
               raise db.TestFailed("wrong number of results: %d (expected: 1)" % (n, 1))

    x=3.0*2.0**math.log(2) # 0.6931471805599453
    print("expected: %f" % x)
    stmt = "select 3*2^log(2.0) from product where prod_key = %d" % k
    with c.execute(stmt) as cur:
        if not cur.ok():
          raise db.TestFailed("cannot find: %d %s" % (cur.code(),cur.details()))
        n=0
        for row in cur:
            n+=1
            if row.field(0) != x:
               raise db.TestFailed("wrong result: %f (expected: %f)" % (row.field(0), x))
        if n != 1:
               raise db.TestFailed("wrong number of results: %d (expected: 1)" % (n, 1))
            
# agg expressions on vertex
def aggvrtx(c):

    print("RUNNING TEST 'aggvrtx'")

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
       m = (sp[l//2].price + sp[l//2+1].price)/2
    else: # length is odd
       m = sp[(l+1)/2].price

    stmt = "select median(prod_price) from product"
    with c.execute(stmt) as cur:
        if not cur.ok():
          raise db.TestFailed("cannot find %d: %s" % (cur.code(),cur.details()))
        n=0
        for row in cur:
            n+=1
            print("median price: %f" % row.field(0))
            if row.field(0) != m:
               raise db.TestFailed("expected median: %f, but have %f" % (m, row.field(0)))
        if n != 1:
           raise db.TestFailed("wrong number of rows: %d" % (n))

# agg expressions on edge
def aggedge(c):

    print("RUNNING TEST 'aggedge'")

    stmt = "select sum(1), count(*) from buys"
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
        s += e.price
    a = s/len(es)

    stmt = "select avg(price), sum(price)/count(*) from buys"
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
    se = sorted(es, key=lambda e: e.price)
    l = len(es) - 1
    if l%2 != 0: # length is even
       m = (se[l//2].price + se[l//2+1].price)/2
    else: # length is odd
       m = se[(l+1)//2].price

    stmt = "select median(price) from buys"
    with c.execute(stmt) as cur:
        if not cur.ok():
          raise db.TestFailed("cannot find %d: %s" % (cur.code(),cur.details()))
        n=0
        for row in cur:
            n+=1
            print("median quantity: %f" % row.field(0))
            if row.field(0) != m:
               # raise db.TestFailed("expected median: %f, but have %f" % (m, row.field(0)))
               print("expected median: %f, but have %f" % (m, row.field(0)))
        if n != 1:
           raise db.TestFailed("wrong number of rows: %d" % (n))

# grouped expressions on edge
def grpedge(c):

    print("RUNNING TEST 'grpedge'")

    stmt = "select origin, sum(1), count(*) \
              from buys \
             group by origin"
    with c.execute(stmt) as cur:
        if not cur.ok():
          raise db.TestFailed("cannot find %d: %s" % (cur.code(),cur.details()))
        for row in cur:
            if row.field(1) != row.field(2):
               raise db.TestFailed("sum(1) != count(*): %d: %d" % (row.field(0), row.field(1)))

            stmt = "select count(*) from buys \
                     where origin=%d" % (row.field(0))
            print("executing %s" % stmt)
            with c.execute(stmt) as cur2:
                if not cur2.ok():
                  raise db.TestFailed("cur2 not ok: %d, %s" % (cur2.code(),cur2.details()))
                for rr in cur2:
                    if rr.field(0) != row.field(1):
                       raise db.TestFailed("expected count: %d, but have %d" % (rr.field(0), row.field(1)))

    stmt = "select origin, avg(quantity), sum(tofloat(quantity))/count(*) \
              from buys \
             group by origin"
    with c.execute(stmt) as cur:
        if not cur.ok():
          raise db.TestFailed("cannot find %d: %s" % (cur.code(),cur.details()))
        for row in cur:
            if row.field(1) != row.field(2):
               raise db.TestFailed("avg(w) != sum(w)/count(*): %f: %f" % (row.field(1), row.field(2)))

            stmt = "select avg(quantity) from buys \
                     where origin=%d" % (row.field(0))
            print("executing %s" % stmt)
            with c.execute(stmt) as cur2:
                if not cur2.ok():
                  raise db.TestFailed("cur2 not ok: %d, %s" % (cur2.code(),cur2.details()))
                for rr in cur2:
                    if rr.field(0) != row.field(2):
                       raise db.TestFailed("expected avg: %f, but have %f" % (rr.field(0), row.field(1)))

    stmt = "select origin, median(quantity) \
              from buys \
             group by origin" 
    with c.execute(stmt) as cur:
        if not cur.ok():
          raise db.TestFailed("cannot find %d: %s" % (cur.code(),cur.details()))
        for row in cur:
            stmt = "select median(quantity) from buys \
                     where origin=%d" % (row.field(0))
            print("executing %s" % stmt)
            with c.execute(stmt) as cur2:
                if not cur2.ok():
                  raise db.TestFailed("cur2 not ok: %d, %s" % (cur2.code(),cur2.details()))
                for rr in cur2:
                    if rr.field(0) != row.field(1):
                       raise db.TestFailed("expected median: %f, but have %f" % (rr.field(0), row.field(1)))

# Beloved fibonacci
def fibonacci(c,one,two,x):

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
            print("%d" % row.field(0))
            fibonacci(c, two, row.field(0), x-1)
        if n != 1:
           raise db.TestFailed("wrong number of rows: %d" % (n))


def pipitest(c):

    print("RUNNING TEST 'pipitest'")

    stmt = "select pi() from product"
    with c.execute(stmt) as cur:
        if not cur.ok():
          raise db.TestFailed("not ok %d: %s" % (cur.code(),cur.details()))
        n=0
        for row in cur:
            if n==0:
               print("%.9f" % row.field(0))
               n+=1
            if row.field(0) > math.pi + 0.00000001 or \
               row.field(0) < math.pi - 0.00000001:
                   raise db.TestFailed("this is not pi: %f" % (row.field(0)))

    stmt = "select e() from product"
    with c.execute(stmt) as cur:
        if not cur.ok():
          raise db.TestFailed("not ok %d: %s" % (cur.code(),cur.details()))
        n=0
        for row in cur:
            if n == 0:
               print("%.9f" % row.field(0))
               n+=1
            if row.field(0) > math.e + 0.00000001 or \
               row.field(0) < math.e - 0.00000001:
                   raise db.TestFailed("this is not e: %f" % (row.field(0)))

def fibotest(c,x):

    print("RUNNING TEST 'fibonacci %d'" % x)

    print("1\n1")
    fibonacci(c,1,1,x)

# constant values
def constvalues(c):

    print("RUNNING TEST 'constvalues'")

    idx = random.randint(1,len(ps)-1)
    k = ps[idx].key

    # constant string
    stmt = "select 'hello' from product \
             where prod_key = %d" % k
    with c.execute(stmt) as cur:
        if not cur.ok():
          raise db.TestFailed("cannot find %d: %s" % (k,cur.details()))
        n=0
        for row in cur:
            n+=1
            if row.field(0) != 'hello':
               raise db.TestFailed("wrong value 'hello': '%s'" % (row.field(0)))
        if n != 1:
           raise db.TestFailed("wrong number of rows: %d" % (n))

    # true
    stmt = "select true from product \
             where prod_key = %d" % k
    with c.execute(stmt) as cur:
        if not cur.ok():
          raise db.TestFailed("cannot find %d: %s" % (k,cur.details()))
        n=0
        for row in cur:
            n+=1
            if not row.field(0):
               raise db.TestFailed("wrong value True: %s" % (row.field(0)))
        if n != 1:
           raise db.TestFailed("wrong number of rows: %d" % (n))

    # false
    stmt = "select false from product \
             where prod_key = %d" % k
    with c.execute(stmt) as cur:
        if not cur.ok():
          raise db.TestFailed("cannot find %d: %s" % (k,cur.details()))
        n=0
        for row in cur:
            n+=1
            if row.field(0):
               raise db.TestFailed("wrong value False: %s" % (row.field(0)))
        if n != 1:
           raise db.TestFailed("wrong number of rows: %d" % (n))

    # a birthdate
    stmt = "select '1971-05-18T06:23:10.123456000' from product \
             where prod_key = %d" % k
    
    dt = datetime.datetime(1971, 5, 18, 6, 23, 10, 123456, now.utc)
    print("%s" % dt)
    with c.execute(stmt) as cur:
        if not cur.ok():
          raise db.TestFailed("cannot find %d: %s" % (k,cur.details()))
        n=0
        for row in cur:
            n+=1
            if now.now2dt(row.field(0)) != dt:
               raise db.TestFailed("wrong value False: %s" % (row.field(0)))
        if n != 1:
           raise db.TestFailed("wrong number of rows: %d" % (n))

# test trigonometry functions
def trigofun(c):

    print("RUNNING TEST 'trigofun'")

    idx = random.randint(1,len(ps)-1)
    k = ps[idx].key

    # -- sine
    stmt = "select sin(prod_price) \
              from product where prod_key = %d" % k
    with c.execute(stmt) as cur:
        if not cur.ok():
          raise db.TestFailed("not ok %d: %s" % (cur.code(),cur.details()))
        for row in cur:
            s = math.sin(ps[idx].price)
            print("sine: %f %f" % (s, row.field(0)))
            if row.field(0) + 0.0001 < s or \
               row.field(0) - 0.0001 > s:
               raise db.TestFailed("sine confusion: %f != %f" % (row.field(0), s))

    # -- arcsine
    stmt = "select asin(sin(prod_price)) \
              from product where prod_key = %d" % k
    with c.execute(stmt) as cur:
        if not cur.ok():
          raise db.TestFailed("not ok %d: %s" % (cur.code(),cur.details()))
        for row in cur:
            s = math.asin(math.sin(ps[idx].price))
            print("asine: %f %f" % (s, row.field(0)))
            if row.field(0) + 0.0001 < s or \
               row.field(0) - 0.0001 > s:
               raise db.TestFailed("asine confusion: %f != %f" % (row.field(0), s))

    # -- cosine
    stmt = "select cos(prod_price) \
              from product where prod_key = %d" % k
    with c.execute(stmt) as cur:
        if not cur.ok():
          raise db.TestFailed("not ok %d: %s" % (cur.code(),cur.details()))
        for row in cur:
            s = math.cos(ps[idx].price)
            print("cosine: %f %f" % (s, row.field(0)))
            if row.field(0) + 0.0001 < s or \
               row.field(0) - 0.0001 > s:
               raise db.TestFailed("cosine confusion: %f != %f" % (row.field(0), s))

    # -- arccosine
    stmt = "select acos(cos(prod_price)) \
              from product where prod_key = %d" % k
    with c.execute(stmt) as cur:
        if not cur.ok():
          raise db.TestFailed("not ok %d: %s" % (cur.code(),cur.details()))
        for row in cur:
            s = math.acos(math.cos(ps[idx].price))
            print("acosine: %f %f" % (s, row.field(0)))
            if row.field(0) + 0.0001 < s or \
               row.field(0) - 0.0001 > s:
               raise db.TestFailed("acosine confusion: %f != %f" % (row.field(0), s))

    # -- tangent
    stmt = "select tan(prod_price) \
              from product where prod_key = %d" % k
    with c.execute(stmt) as cur:
        if not cur.ok():
          raise db.TestFailed("not ok %d: %s" % (cur.code(),cur.details()))
        for row in cur:
            s = math.tan(ps[idx].price)
            print("studio tan: %f %f" % (s, row.field(0)))
            if row.field(0) + 0.0001 < s or \
               row.field(0) - 0.0001 > s:
               raise db.TestFailed("tangent confusion: %f != %f" % (row.field(0), s))

    # -- arctangent
    stmt = "select atan(tan(prod_price)) \
              from product where prod_key = %d" % k
    with c.execute(stmt) as cur:
        if not cur.ok():
          raise db.TestFailed("not ok %d: %s" % (cur.code(),cur.details()))
        for row in cur:
            s = math.atan(math.tan(ps[idx].price))
            print("atan: %f %f" % (s, row.field(0)))
            if row.field(0) + 0.0001 < s or \
               row.field(0) - 0.0001 > s:
               raise db.TestFailed("atan confusion: %f != %f" % (row.field(0), s))

    # -- sineh
    stmt = "select sinh(prod_price) \
              from product where prod_key = %d" % k
    with c.execute(stmt) as cur:
        if not cur.ok():
          raise db.TestFailed("not ok %d: %s" % (cur.code(),cur.details()))
        for row in cur:
            s = math.sinh(ps[idx].price)
            print("sineh: %f %f" % (s, row.field(0)))
            if row.field(0) + 0.0001 < s or \
               row.field(0) - 0.0001 > s:
               raise db.TestFailed("sineh confusion: %f != %f" % (row.field(0), s))

    # -- arcsineh
    stmt = "select asinh(sinh(prod_price)) \
              from product where prod_key = %d" % k
    with c.execute(stmt) as cur:
        if not cur.ok():
          raise db.TestFailed("not ok %d: %s" % (cur.code(),cur.details()))
        for row in cur:
            s = math.asinh(math.sinh(ps[idx].price))
            print("asineh: %f %f" % (s, row.field(0)))
            if row.field(0) + 0.0001 < s or \
               row.field(0) - 0.0001 > s:
               raise db.TestFailed("asineh confusion: %f != %f" % (row.field(0), s))

    # -- cosineh
    stmt = "select cosh(prod_price) \
              from product where prod_key = %d" % k
    with c.execute(stmt) as cur:
        if not cur.ok():
          raise db.TestFailed("not ok %d: %s" % (cur.code(),cur.details()))
        for row in cur:
            s = math.cosh(ps[idx].price)
            print("cosineh: %f %f" % (s, row.field(0)))
            if row.field(0) + 0.0001 < s or \
               row.field(0) - 0.0001 > s:
               raise db.TestFailed("cosineh confusion: %f != %f" % (row.field(0), s))

    # -- arccosineh
    stmt = "select acosh(cosh(prod_price)) \
              from product where prod_key = %d" % k
    with c.execute(stmt) as cur:
        if not cur.ok():
          raise db.TestFailed("not ok %d: %s" % (cur.code(),cur.details()))
        for row in cur:
            s = math.acosh(math.cosh(ps[idx].price))
            print("acosineh: %f %f" % (s, row.field(0)))
            if row.field(0) + 0.0001 < s or \
               row.field(0) - 0.0001 > s:
               raise db.TestFailed("acosineh confusion: %f != %f" % (row.field(0), s))

    # -- tangenth
    stmt = "select tanh(prod_price) \
              from product where prod_key = %d" % k
    with c.execute(stmt) as cur:
        if not cur.ok():
          raise db.TestFailed("not ok %d: %s" % (cur.code(),cur.details()))
        for row in cur:
            s = math.tanh(ps[idx].price)
            print("tanh: %f %f" % (s, row.field(0)))
            if row.field(0) + 0.0001 < s or \
               row.field(0) - 0.0001 > s:
               raise db.TestFailed("tangenth confusion: %f != %f" % (row.field(0), s))

    # -- arctangent
    stmt = "select atanh(tanh(prod_price)) \
              from product where prod_key = %d" % k
    with c.execute(stmt) as cur:
        if not cur.ok():
          raise db.TestFailed("not ok %d: %s" % (cur.code(),cur.details()))
        for row in cur:
            if row.field(0) >= 1.0:
                continue
            s = math.atanh(math.tanh(ps[idx].price))
            print("atanh: %f %f" % (s, row.field(0)))
            if row.field(0) + 0.0001 < s or \
               row.field(0) - 0.0001 > s:
               raise db.TestFailed("atanh confusion: %f != %f" % (row.field(0), s))

# test time functions
def timefun(c):

    print("RUNNING TEST 'timefun'")

    idx = random.randint(1,len(ps)-1)
    k = ps[idx].key

    # a birthdate
    d = "1971-05-18T06:23:10.123456000"

    # year, month, mday
    stmt = "select year('%s'), month('%s'), mday('%s') from product \
             where prod_key = %d" % (d,d,d,k)

    with c.execute(stmt) as cur:
        if not cur.ok():
          raise db.TestFailed("cannot find %d: %s" % (k,cur.details()))
        n=0
        for row in cur:
            n+=1
            if row.field(0) != 1971:
               raise db.TestFailed("wrong value for year: %d" % (row.field(0)))
            if row.field(1) != 5:
               raise db.TestFailed("wrong value for month: %d" % (row.field(1)))
            if row.field(2) != 18:
               raise db.TestFailed("wrong value for day: %d" % (row.field(2)))
        if n != 1:
           raise db.TestFailed("wrong number of rows: %d" % (n))

    # hour, minute, second
    stmt = "select hour('%s'), minute('%s'), second('%s') from product \
             where prod_key = %d" % (d,d,d,k)

    with c.execute(stmt) as cur:
        if not cur.ok():
          raise db.TestFailed("cannot find %d: %s" % (k,cur.details()))
        n=0
        for row in cur:
            n+=1
            if row.field(0) != 6:
               raise db.TestFailed("wrong value for hour: %d" % (row.field(0)))
            if row.field(1) != 23:
               raise db.TestFailed("wrong value for minute: %d" % (row.field(1)))
            if row.field(2) != 10:
               raise db.TestFailed("wrong value for second: %d" % (row.field(2)))
        if n != 1:
           raise db.TestFailed("wrong number of rows: %d" % n)

    # milli, micro, nano
    stmt = "select milli('%s'), micro('%s'), nano('%s') from product \
             where prod_key = %d" % (d,d,d,k)

    with c.execute(stmt) as cur:
        if not cur.ok():
          raise db.TestFailed("cannot find %d: %s" % (k,cur.details()))
        n=0
        for row in cur:
            n+=1
            if row.field(0) != 123:
               raise db.TestFailed("wrong value for milli: %d" % (row.field(0)))
            if row.field(1) != 123456:
               raise db.TestFailed("wrong value for micro: %d" % (row.field(1)))
            if row.field(2) != 123456000:
               raise db.TestFailed("wrong value for nano: %d" % (row.field(2)))
        if n != 1:
           raise db.TestFailed("wrong number of rows: %d" % n)

    # it was a Friday
    stmt = "select wday('2016-01-01') from product \
             where prod_key = %d" % k
    with c.execute(stmt) as cur:
        if not cur.ok():
          raise db.TestFailed("cannot find %d: %s" % (k,cur.details()))
        n=0
        for row in cur:
            n+=1
            if row.field(0) != 5:
               raise db.TestFailed("wrong value for Jan, 1: %d" % (row.field(0)))
        if n != 1:
           raise db.TestFailed("wrong number of rows: %d" % n)

    # it was a Saturday
    stmt = "select wday('2016-01-02') from product \
             where prod_key = %d" % k
    with c.execute(stmt) as cur:
        if not cur.ok():
          raise db.TestFailed("cannot find %d: %s" % (k,cur.details()))
        n=0
        for row in cur:
            n+=1
            if row.field(0) != 6:
               raise db.TestFailed("wrong value for Jan, 2: %d" % (row.field(0)))
        if n != 1:
           raise db.TestFailed("wrong number of rows: %d" % n)

    # it was a Sunday
    stmt = "select wday('2016-01-03') from product \
             where prod_key = %d" % k
    with c.execute(stmt) as cur:
        if not cur.ok():
          raise db.TestFailed("cannot find %d: %s" % (k,cur.details()))
        n=0
        for row in cur:
            n+=1
            if row.field(0) != 0:
               raise db.TestFailed("wrong value for Jan, 3: %d" % (row.field(0)))
        if n != 1:
           raise db.TestFailed("wrong number of rows: %d" % n)

    # it was a Monday
    stmt = "select wday('2016-01-04') from product \
             where prod_key = %d" % k
    with c.execute(stmt) as cur:
        if not cur.ok():
          raise db.TestFailed("cannot find %d: %s" % (k,cur.details()))
        n=0
        for row in cur:
            n+=1
            if row.field(0) != 1:
               raise db.TestFailed("wrong value for Jan, 4: %d" % (row.field(0)))
        if n != 1:
           raise db.TestFailed("wrong number of rows: %d" % n)

    # it was a Tuesday
    stmt = "select wday('2016-01-05') from product \
             where prod_key = %d" % k
    with c.execute(stmt) as cur:
        if not cur.ok():
          raise db.TestFailed("cannot find %d: %s" % (k,cur.details()))
        n=0
        for row in cur:
            n+=1
            if row.field(0) != 2:
               raise db.TestFailed("wrong value for Jan, 5: %d" % (row.field(0)))
        if n != 1:
           raise db.TestFailed("wrong number of rows: %d" % n)

    # it was a Wednesday
    stmt = "select wday('2016-01-06') from product \
             where prod_key = %d" % k
    with c.execute(stmt) as cur:
        if not cur.ok():
          raise db.TestFailed("cannot find %d: %s" % (k,cur.details()))
        n=0
        for row in cur:
            n+=1
            if row.field(0) != 3:
               raise db.TestFailed("wrong value for Jan, 6: %d" % (row.field(0)))
        if n != 1:
           raise db.TestFailed("wrong number of rows: %d" % n)

    # it was a Thursday
    stmt = "select wday('2016-01-07') from product \
             where prod_key = %d" % k
    with c.execute(stmt) as cur:
        if not cur.ok():
          raise db.TestFailed("cannot find %d: %s" % (k,cur.details()))
        n=0
        for row in cur:
            n+=1
            if row.field(0) != 4:
               raise db.TestFailed("wrong value for Jan, 7: %d" % (row.field(0)))
        if n != 1:
           raise db.TestFailed("wrong number of rows: %d" % n)

    # from dusk to dawn
    stmt = "select epoch(), dawn(), dusk() from product \
             where prod_key = %d" % (k)

    with c.execute(stmt) as cur:
        if not cur.ok():
          raise db.TestFailed("cannot find %d: %s" % (k,cur.details()))
        n=0
        for row in cur:
            n+=1
            if row.field(0) != 0:
               raise db.TestFailed("wrong value for epoch: %d" % (row.field(0)))
            if row.field(1) != -2**63:
               raise db.TestFailed("wrong value for dawn: %d" % (row.field(1)))
            if row.field(2) != 2**63-1:
               raise db.TestFailed("wrong value for dusk: %d" % (row.field(2)))
        if n != 1:
           raise db.TestFailed("wrong number of rows: %d" % n)

    # now
    stmt = "select now() from product \
             where prod_key = %d" % (k)

    tp = now.dt2now(datetime.datetime.now(now.utc)) / 10000000
    with c.execute(stmt) as cur:
        if not cur.ok():
          raise db.TestFailed("cannot find %d: %s" % (k,cur.details()))
        n=0
        for row in cur:
            n+=1
            tn = row.field(0) / 10000000
            if tn > tp + 1 or  \
               tn < tp - 1:
               raise db.TestFailed("wrong value for now: %d / %d" % (tn, tp))
        if n != 1:
           raise db.TestFailed("wrong number of rows: %d" % n)

# test case 
def casefun(c):

    print("RUNNING TEST 'casefun'")

    idx = random.randint(1,len(ps)-1)
    k = ps[idx].key

    # three alternatives and else
    mycase = "case " \
             "  when prod_packing = 1 then 'tiny'" \
             "  when prod_packing = 2 then 'medium'" \
             "  when prod_packing = 3 then 'big'" \
             "  else 'unknown'" \
             " end"

    stmt = "select prod_desc, %s from product where prod_key = %d" % (mycase, k)

    with c.execute(stmt) as cur:
        if not cur.ok():
          raise db.TestFailed("not ok %d: %s" % (cur.code(),cur.details()))
        for row in cur:
            if row.field(0) != ps[idx].desc:
               raise db.TestFailed("wrong value %s: %s" % (row.field(0), ps[idx].desc))
            if (row.field(1) == "tiny" and ps[idx].packing != 1) or \
               (row.field(1) == "medium" and ps[idx].packing != 2) or \
               (row.field(1) == "big" and ps[idx].packing != 3) or \
               (row.field(1) == "unknown"):
               raise db.TestFailed("unexpected value %s: %d" % (row.field(1), ps[idx].packing))
            print("%s" % row.field(1))

    # three alternatives and else with different type
    mycase = "case " \
             "  when prod_cat = 1 then 'one'" \
             "  when prod_cat = 2 then 'two'" \
             "  when prod_cat = 3 then 'three'" \
             "  else prod_cat" \
             " end"

    stmt = "select prod_desc, %s from product where prod_key = %d" % (mycase, k)

    with c.execute(stmt) as cur:
        if not cur.ok():
          raise db.TestFailed("not ok %d: %s" % (cur.code(),cur.details()))
        for row in cur:
            if row.field(0) != ps[idx].desc:
               raise db.TestFailed("wrong value %s: %s" % (row.field(0), ps[idx].desc))
            if (row.field(1) == "one" and ps[idx].cat != 1) or \
               (row.field(1) == "two" and ps[idx].cat != 2) or \
               (row.field(1) == "three" and ps[idx].cat != 3) or \
               (row.field(1) == 4 and ps[idx].cat != 4) or \
               (row.field(1) == 5 and ps[idx].cat != 5):
               raise db.TestFailed("unexpected value %s: %d" % (row.field(1), ps[idx].cat))
            print("%s" % row.field(1))

    # three alternatives else is NULL
    mycase = "case " \
             "  when prod_cat = 1 then 'one'" \
             "  when prod_cat = 2 then 'two'" \
             "  when prod_cat = 3 then 'three'" \
             "  else NULL" \
             " end"

    stmt = "select prod_desc, %s from product where prod_key = %d" % (mycase, k)
    with c.execute(stmt) as cur:
        if not cur.ok():
          raise db.TestFailed("not ok %d: %s" % (cur.code(),cur.details()))
        for row in cur:
            if row.field(0) != ps[idx].desc:
               raise db.TestFailed("wrong value %s: %s" % (row.field(0), ps[idx].desc))
            if (row.field(1) == "one" and ps[idx].cat != 1) or \
               (row.field(1) == "two" and ps[idx].cat != 2) or \
               (row.field(1) == "three" and ps[idx].cat != 3) or \
               (row.field(1) is None and \
                ps[idx].cat != 4 and ps[idx].cat != 5):
               raise db.TestFailed("unexpected value %s: %d" % (row.field(1), ps[idx].cat))
            print("%d: %s" % (ps[idx].cat,row.field(1)))

    # three alternatives no else
    mycase = "case " \
             "  when prod_cat = 1 then 'one'" \
             "  when prod_cat = 2 then 'two'" \
             "  when prod_cat = 3 then 'three'" \
             " end"

    stmt = "select prod_desc, %s from product where prod_key = %d" % (mycase, k)
    with c.execute(stmt) as cur:
        if not cur.ok():
          raise db.TestFailed("not ok %d: %s" % (cur.code(),cur.details()))
        for row in cur:
            if row.field(0) != ps[idx].desc:
               raise db.TestFailed("wrong value %s: %s" % (row.field(0), ps[idx].desc))
            if (row.field(1) == "one" and ps[idx].cat != 1) or \
               (row.field(1) == "two" and ps[idx].cat != 2) or \
               (row.field(1) == "three" and ps[idx].cat != 3) or \
               (row.field(1) is None and \
                ps[idx].cat != 4 and ps[idx].cat != 5):
               raise db.TestFailed("unexpected value %s: %d" % (row.field(1), ps[idx].cat))
            print("%d: %s" % (ps[idx].cat,row.field(1)))

    # when is null else is not
    mycase = "case " \
             "  when prod_cat = 1 then NULL" \
             "  else prod_cat" \
             " end"

    stmt = "select prod_desc, %s from product where prod_key = %d" % (mycase, k)
    with c.execute(stmt) as cur:
        if not cur.ok():
          raise db.TestFailed("not ok %d: %s" % (cur.code(),cur.details()))
        for row in cur:
            if row.field(0) != ps[idx].desc:
               raise db.TestFailed("wrong value %s: %s" % (row.field(0), ps[idx].desc))
            if (row.field(1) is None and ps[idx].cat != 1) or \
               (row.field(1) is not None and row.field(1) != ps[idx].cat):
               raise db.TestFailed("unexpected value %s: %d" % (row.field(1), ps[idx].cat))
            print("%d: %s" % (ps[idx].cat,row.field(1)))

    # complex when
    mycase = "case " \
             "  when prod_cat = 1 and prod_packing = 1 then 'tiny one'" \
             "  when prod_cat = 1 and prod_packing = 3 then 'big  one'" \
             "  when prod_cat = 2 and prod_packing = 1 then 'tiny two'" \
             "  when prod_cat = 2 and prod_packing = 3 then 'big  two'" \
             "  when prod_cat = 1 then 'one'" \
             "  when prod_cat = 2 then 'two'" \
             "  when prod_cat = 3 then 'three'" \
             "  else 'a whole bunch'" \
             " end"

    stmt = "select prod_desc, %s from product where prod_key = %d" % (mycase, k)
    print(stmt)
    with c.execute(stmt) as cur:
        if not cur.ok():
          raise db.TestFailed("not ok %d: %s" % (cur.code(),cur.details()))
        for row in cur:
            print("%d / %d: %s" % (ps[idx].cat,ps[idx].packing,row.field(1)))
            if row.field(0) != ps[idx].desc:
               raise db.TestFailed("wrong value %s: %s" % (row.field(0), ps[idx].desc))
            if (ps[idx].cat == 1 and ps[idx].packing ==1 and \
                row.field(1) != "tiny one") or \
               (ps[idx].cat == 1 and ps[idx].packing ==3 and \
                row.field(1) != "big  one") or \
               (ps[idx].cat == 2 and ps[idx].packing ==1 and \
                row.field(1) != "tiny two") or \
               (ps[idx].cat == 2 and ps[idx].packing ==3 and \
                row.field(1) != "big  two") or \
               (ps[idx].cat == 1 and ps[idx].packing != 1 and \
                ps[idx].packing != 3 and row.field(1) != "one") or \
               (ps[idx].cat == 2 and ps[idx].packing != 1 and \
                ps[idx].packing != 3 and row.field(1) != "two") or \
               (ps[idx].cat == 3 and row.field(1) != "three") or \
               ((ps[idx].cat == 4 or ps[idx].cat == 5) and \
                row.field(1) != "a whole bunch"):
               print("%d / %d: '%s'" % (ps[idx].cat,ps[idx].packing,row.field(1)))
               raise db.TestFailed("unexpected value '%s': %d" % (row.field(1), ps[idx].cat))
            print("%d / %d: '%s'" % (ps[idx].cat,ps[idx].packing,row.field(1)))

    # case with agg
    mycase = "case " \
             "  when count(*) > 100 then 'too much'" \
             "  when count(*) = 100 then 'full'" \
             "  when count(*) >= 75 then '3/4'" \
             "  when count(*) >= 66 then '2/3'" \
             "  when count(*) >  50 then 'majority'" \
             "  when count(*) >= 33 then '1/3'" \
             "  when count(*) >= 25 then '1/4'" \
             "  when count(*) >= 10 then 'tens'" \
             "  when count(*) >   1 then 'some'" \
             "  when count(*) =   1 then 'one'" \
             "  else                     'zero'" \
             " end"

    stmt = "select %s from product where prod_cat = %d" % (mycase, ps[idx].cat)

    cnt=0
    for p in ps:
        if p.cat == ps[idx].cat:
           cnt+=1

    print("counted: %d" % cnt)

    with c.execute(stmt) as cur:
        for row in cur:
            if (cnt >  100 and row.field(0) != "too much") or \
               (cnt == 100 and row.field(0) != "full") or \
               (cnt >=  75 and cnt < 100 and row.field(0) != "3/4") or \
               (cnt >=  66 and cnt <  75 and row.field(0) != "2/3") or \
               (cnt >   50 and cnt <  66  and row.field(0) != "majority") or \
               (cnt >=  33 and cnt <= 50  and row.field(0) != "1/3") or \
               (cnt >=  25 and cnt <  33 and row.field(0) != "1/4") or \
               (cnt >=  10 and cnt <  25 and row.field(0) != "tens") or \
               (cnt >    1 and cnt <  10 and row.field(0) != "some") or \
               (cnt ==   1 and row.field(0) != "one") or \
               (cnt ==   0 and row.field(0) != "zero"):
               raise db.TestFailed("unexpected value %s: %d" % (row.field(0), ps[idx].cat))
            print("%s" % row.field(0))

    # agg with case
    mycase = "case " \
             "  when prod_cat = 1 and prod_packing = 1 then 101" \
             "  when prod_cat = 1 and prod_packing = 3 then 103" \
             "  when prod_cat = 2 and prod_packing = 1 then 201" \
             "  when prod_cat = 2 and prod_packing = 3 then 203" \
             "  when prod_cat = 3 and prod_packing = 1 then 301" \
             "  when prod_cat = 3 and prod_packing = 3 then 303" \
             "  when prod_cat = 4 and prod_packing = 1 then 401" \
             "  when prod_cat = 4 and prod_packing = 3 then 403" \
             "  when prod_cat = 5 and prod_packing = 1 then 501" \
             "  when prod_cat = 5 and prod_packing = 3 then 503" \
             "  else prod_cat" \
             " end"

    stmt = "select sum(%s) from product" % mycase

    sm=0
    for p in ps:
        if p.packing == 1 or p.packing == 3:
           sm+=p.cat*100 + p.packing
        else:
           sm+=p.cat

    print("sum: %d" % sm)

    with c.execute(stmt) as cur:
        for row in cur:
            if sm != row.field(0):
               raise db.TestFailed("unexpected value %d: %d" % (row.field(0), sm))
            print("%s" % row.field(0))

if __name__ == "__main__":

    random.seed()

    with now.Connection("localhost", "55505", None, None) as c:
        (ps, cs, ss, es, vs) = db.loadDB(c, "db100")

        constvalues(c)
        timefun(c)
        pipitest(c)

        for i in range(IT):
            simplevrtx(c)
            simpleedge(c)
            punktvorstrich(c)
            casefun(c)
            trigofun(c)

        fibotest(c,20)

        aggvrtx(c)
        aggedge(c)
        grpedge(c)

        print("PASSED")
