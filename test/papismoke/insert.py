#!/usr/bin/env python

import db
import nowapi as na
import random as rnd

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
    l = countdb(c,'client')
    x = rnd.randint(0,l)
    i = 0
    sql = "select client_key as key from client"
    for row in c.execute(sql):
        if i == x:
           return row['key']
        i+=1
    
def insertvertex(c,k):
    print "RUNNING TEST 'insertvertex'"
    s = 'product %d' % k
    sql = "insert into product (prod_key, prod_desc, prod_cat, prod_packing, prod_price) \
                               (%d, '%s', 1, 2, 1.99)" % (k, s)
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

def insertedge(c,o,d):
    print "RUNNING TEST 'insertedge'"

    before = countdb(c,'buys')

    for i in range(100):
        sql1 = "insert into buys (origin, destin, stamp, quantity, price) \
                                 (%d, %d, " % (o,d)
        sql = sql1 + "%d,%d,%f)"

        c.execute(sql, parameters=[na.Date(2019, 3, 20),i,i*1.99]).close()
        after = countdb(c,'buys')
        if after != before + i + 1:
           raise db.TestFailed('not inserted or wong count...')

if __name__ == "__main__":
    with na.connect("localhost", "55505", None, None, 'db150') as c:

        o = findlast(c)
        o += 10

        d = findrand(c)
        print("%d" % d)

        insertvertex(c,o)
        insertedge(c,o,d)

    print("PASSED")
