#!/usr/bin/env python

import db
import now
import nowapi as na
import random as rnd
import math
import sys

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

def createprocs(c, lang):
    print "RUNNING 'createprocs' %s" % lang

    c.execute("drop procedure locktest if exists").close()
    c.execute("create procedure db.locktest(lk text) language %s" % lang).close()

    c.execute("drop procedure uniquetest if exists").close()
    c.execute("create procedure db.uniquetest(u text) language %s" % lang).close()

    c.execute("drop procedure recachetest if exists").close()
    c.execute("create procedure db.recachetest(name text, m uint, v bool) language %s" % lang).close()

    c.execute("drop procedure dropcache if exists").close()
    c.execute("create procedure db.dropcache(name text) language %s" % lang).close()

def locktest(c):
    print("RUNNING 'locktest'")

    c.execute("exec locktest('testlock10')")

def uniquetest(c):
    print("RUNNING 'uniquetest'")

    us = set() 

    for i in range(100):
      with c.execute("exec uniquetest('unid10')") as cur:
        for row in cur:
          s = len(us)
          us.add(row[0])
          if s == len(us):
             db.TestFailed("value not unique: %d" % row[0])

def recachetest(c):
    print("RUNNING 'recachetest'")

    # modulo 3
    stmt = "select count(*) as cnt from visits where origin%3 = 0"
    cnt = 0
    with c.execute(stmt) as cur:
         for row in cur:
             cnt = row['cnt']

    # fill cache for modulo 3
    x = 0
    with c.execute("exec recachetest('cachetest10', 3, false)") as cur:
         for row in cur:
             x+=1
    if x != cnt:
       raise db.TestFailed("counts differ: %d != %d" % (cnt, x))
    
    # reuse cache for modulo 3
    x = 0
    with c.execute("exec recachetest('cachetest10', 3, true)") as cur:
         for row in cur:
             x+=1
    if x != cnt:
       raise db.TestFailed("counts differ: %d != %d" % (cnt, x))

    # insert one more
    c.execute("insert into visits (origin, destin, stamp, quantity, price) \
                           values (3,'cachetest10',now(), 1, 3.5)").close()

    # reuse cache for modulo 3
    x = 0
    with c.execute("exec recachetest('cachetest10', 3, true)") as cur:
         for row in cur:
             x+=1
    if x != cnt:
       raise db.TestFailed("counts differ: %d != %d" % (cnt, x))

    # modulo 2
    stmt = "select count(*) as cnt from visits where origin%2 = 0"
    cnt2 = 0
    with c.execute(stmt) as cur:
         for row in cur:
             cnt2 = row['cnt']
    x = 0

    # fill cache for modulo 2
    with c.execute("exec recachetest('cachetest10', 2, false)") as cur:
         for row in cur:
             x+=1
    if x != cnt2:
       raise db.TestFailed("counts differ: %d != %d" % (cnt2, x))

    # reuse cache for modulo 2
    x = 0
    with c.execute("exec recachetest('cachetest10', 2, true)") as cur:
         for row in cur:
             x+=1
    if x != cnt2:
       raise db.TestFailed("counts differ: %d != %d" % (cnt2, x))

    # reuse cache for modulo 3
    x = 0
    with c.execute("exec recachetest('cachetest10', 3, true)") as cur:
         for row in cur:
             x+=1
    if x != cnt:
       raise db.TestFailed("counts differ: %d != %d" % (cnt, x))

    # refill cache for modulo 3
    x = 0
    with c.execute("exec recachetest('cachetest10', 3, false)") as cur:
         for row in cur:
             x+=1
    if x != cnt+1:
       raise db.TestFailed("counts differ: %d != %d" % (cnt+1, x))

    c.execute("exec dropcache('cachetest10')").close()

if __name__ == "__main__":

    print("running exectest for language lua")

    with na.connect("localhost", "55505", None, None, 'db150') as c:

       createprocs(c, 'lua')

       locktest(c)
       uniquetest(c)
       recachetest(c)

       for i in range(10):
           pass

    print("PASSED")
