#!/usr/bin/env python

import nowapi as na
import random

def countdb(c,t):
    with c.execute("select count(*) from %s" % t, rowformat=na.listrow) as cur:
        for row in cur:
            return row[0]

def fibonacci(c,key,one,two,n):
    if n == 0:
       return

    sql = "select %d + %d from product where prod_key = %d" % (one, two, key)
    # with c.execute(sql, rowformat=na.listrow) as cur:
    for row in c.execute(sql, rowformat=na.listrow):
        print("%d" % row[0])
        return fibonacci(c,key,two,row[0],n-1)

def fibotest(c, n):
    
    print("RUNNING TEST 'fibonacci %d'" % n)

    l = countdb(c, 'product')
    x = random.randint(0,l)
    k = 0
    i = 0
    with c.execute("select prod_key from product") as cur:
        for row in cur:
           if i == x:
              k = row['prod_key']
              break
           i+=1

    print("1\n1")
    fibonacci(c,k,1,1,n)

if __name__ == "__main__":

    random.seed()

    with na.connect("localhost", "55505", None, None, 'db150') as c:

        fibotest(c,55)

    print("PASSED")
