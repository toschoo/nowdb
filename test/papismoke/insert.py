#!/usr/bin/env python

import nowapi as na
import random

def findlast(c):
    sql = "select max(prod_key) from product"
    with c.execute(sql, rowformat=na.listrow) as cur:
        for row in cur:
            return row[0]
    
def insertvertex(c,k):
    print "RUNNING TEST 'insertvertex'"
    s = 'product %d' % k
    sql = "insert into product (prod_key, prod_desc, prod_cat, prod_packing, prod_price) \
                               (%d, '%s', 1, 2, 1.99)" % (k, s)
    c.execute(sql).close() # this is not nice

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

def insertedge(c,prod,client):
    print "RUNNING TEST 'insertedge'"

if __name__ == "__main__":
    with na.connect("localhost", "55505", None, None, 'db150') as c:

        k = findlast(c)
        k += 10

        insertvertex(c,k)

        #insertedge(c, k)

    print("PASSED")
