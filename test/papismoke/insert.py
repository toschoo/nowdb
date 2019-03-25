#!/usr/bin/env python

import db
import nowapi as na
import random as rnd
import math

def countdb(c,t):
    sql = "select count(*) as cnt from %s" % t
    for row in c.execute(sql):
        return row['cnt']

def findlast(c):
    sql = "select max(prod_key) as mx from product"
    with c.execute(sql) as cur:
        for row in cur:
            return row['mx']

def findrand(c):
    l = countdb(c,'client') - 1
    x = rnd.randint(0,l)
    i = 0
    sql = "select client_key as key from client"
    for row in c.execute(sql):
        if i == x:
           return row['key']
        i+=1

def price(c,p):
    sql = "select prod_price as price from product where prod_key = %d" % p
    for row in c.execute(sql):
        return row['price']

def testnoresult(c):
    sql = "select prod_price as price from product where prod_key = 99999"
    for row in c.execute(sql):
        return row['price']
    
def insertvertex(c,k):
    print "RUNNING TEST 'insertvertex'"
    s = 'product %d' % k
    sql = "insert into product (prod_key, prod_desc, prod_cat, prod_packing, prod_price) \
                       values  (%d, '%s', 1, 2, 1.99)" % (k, s)
    c.execute(sql).close() # if you want to close it immediately

    count=0
    sql = "select prod_key, prod_desc from product where prod_key = %d" % k
    with c.execute(sql) as cur:
        for row in cur:
            if row['prod_key'] != k:
               raise db.TestFailed('wrong key...')
            if row['prod_desc'] != s:
               raise db.TestFailed('wrong desc...')
            count+=1
    if count != 1:
       raise db.TestFailed('expecting one row, having: %d' % count)

def insertnullvertex(c,k):
    print "RUNNING TEST 'insertnullvertex'"
    s = 'product %d' % k
    sql = "insert into product (prod_key, prod_desc, prod_cat, prod_packing, prod_price) \
                       values  (%d, '%s', 1, NULL, 1.99)" % (k, s)
    c.execute(sql).close() 

    count=0
    sql = "select prod_key, prod_desc, prod_packing \
             from product where prod_key = %d" % k
    with c.execute(sql) as cur:
        for row in cur:
            if row['prod_key'] != k:
               raise db.TestFailed('wrong key...')
            if row['prod_desc'] != s:
               raise db.TestFailed('wrong desc...')
            if row['prod_packing'] is not None:
               raise db.TestFailed('wrong packing...')
            count+=1
    if count != 1:
       raise db.TestFailed('expecting one row, having: %d' % count)

def insertedge(c,o,d):
    print "RUNNING TEST 'insertedge'"

    before = countdb(c,'buys')

    for i in range(100):
        sql1 = "insert into buys (origin, destin, stamp, quantity, price) \
                          values (%d, %d, " % (o,d)
        sql = sql1 + "%s,%s,%s)"

        c.execute(sql, parameters=[na.Date(2019, 3, 20),(i+1),(i+1)*price(c,d)]).close()
        after = countdb(c,'buys')
        if after != before + i + 1:
           raise db.TestFailed('not inserted or wrong count...')

    sql = "select sum(price) as sum, \
                  avg(price) as mean, \
                  median(price) as med \
             from buys \
            where origin = %d \
              and destin = %d" % (o,d)

    p = price(c,d)
    for row in c.execute(sql):
        print("sum: %f, mean: %f, med: %f" % (row['sum'], row['mean'], row['med']))
        if row['sum'] != (100*(p+100*p))/2:
           raise db.TestFailed("sum differs")
        if row['mean'] != row['sum']/100:
           raise db.TestFailed("mean differs")
        if row['med'] != (50*p+51*p)/2:
           raise db.TestFailed("median differs")

def insertnulledge(c,o,d):
    print "RUNNING TEST 'insertnulledge'"

    before = countdb(c,'buys')

    for i in range(100):
        sql1 = "insert into buys (origin, destin, stamp, quantity, price) \
                          values (%d, %d, " % (o,d)
        sql = sql1 + "%s,%s,%s)"

        c.execute(sql, parameters=[na.Date(2019, 3, 19),None,(i+1)*price(c,d)]).close()
        after = countdb(c,'buys')
        if after != before + i + 1:
           raise db.TestFailed('not inserted or wrong count...')

    sql = "select sum(coal(quantity,0)) as q, \
                  sum(price) as sum, \
                  avg(price) as mean, \
                  median(price) as med \
             from buys \
            where origin = %d \
              and destin = %d" % (o,d)

    p = price(c,d)
    for row in c.execute(sql):
        print("q: %d, sum: %f, mean: %f, med: %f" % \
             (row['q'], row['sum'], row['mean'], row['med']))
        if row['q'] != 0:
           raise db.TestFailed("quantity not null")
        if row['sum'] != (100*(p+100*p))/2:
           raise db.TestFailed("sum differs")
        if row['mean'] != row['sum']/100:
           raise db.TestFailed("mean differs")
        if row['med'] != (50*p+51*p)/2:
           raise db.TestFailed("median differs")

def insertfun(c):
     print("RUNNING TEST 'insertfun'")

     p = findlast(c)
     p += 10

     sql = "insert into product (prod_key, prod_desc, prod_price) \
                        values  (%d, '%s', pi())" % (p, 'pi')
     c.execute(sql).close()
     for row in c.execute("select * from product where prod_key = %d" % p):
         print("%d (%s): %.6f" % (row['prod_key'], row['prod_desc'], row['prod_price']))
         if float(round(row['prod_price'] * 1000))/1000 != \
            float(round(math.pi*1000))/1000:
            db.TestFailed("this is not pi: %.f" % row['prod_price'])

     o = findrand(c)
     sql = "insert into buys (origin, destin, stamp, quantity, price) \
                        values  (%d, %d, now(), touint(round(e()*1000.0)), e()*pi())" % (o, p)
     c.execute(sql).close()
     for row in c.execute("select * from buys where origin = %d and destin = %d" % (o,p)):
         print("%d (%d): %s | %.6f | %.6f" % (row['origin'], row['destin'], row['stamp'], \
                                              row['quantity'], row['price']))
         if row['quantity'] * 1000 != round(math.e*1000):
            db.TestFailed("this is not e: %.f" % row['quantity'])
         if float(round(row['price'] * 1000))/1000 != \
            float(round(math.e*math.pi*1000))/1000:
            db.TestFailed("this is not e*pi: %.f" % row['price'])

if __name__ == "__main__":
    with na.connect("localhost", "55505", None, None, 'db150') as c:

        testnoresult(c)

        for i in range(1):

          d = findlast(c)
          d += 10

          o = findrand(c)
          print("%d -> %d" % (o,d))

          insertvertex(c,d)
          insertedge(c,o,d)

          d += 10
          print("%d -> %d" % (o,d))
          insertnullvertex(c,d)
          insertnulledge(c,o,d)

        insertfun(c)

    print("PASSED")
