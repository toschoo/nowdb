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

def createprocs(c):
    print "RUNNING 'createprocs'"

    c.execute("drop procedure mycount if exists").close()
    c.execute("create procedure db.mycount(t text) language python").close()

    c.execute("drop procedure sayhello if exists").close()
    c.execute("create procedure db.sayhello() language python").close()

    c.execute("drop procedure groupbuy if exists").close()
    c.execute("create procedure db.groupbuy(p uint) language python").close()

    c.execute("drop type unique if exists").close()
    c.execute("create type unique (id uint primary key, desc text)").close()
    c.execute("insert into unique (id, desc) values (1, 'bootstrap')").close()

    c.execute("drop edge result if exists").close()
    c.execute("create stamped edge result (\
                  origin unique, \
                  destin product, \
                  f1     float, \
                  f2     float, \
                  f3     float, \
                  f4     float, \
                  f5     float, \
                  f6     float)").close()

def hellotest(c):
    print "RUNNING TEST 'hellotest'"

    c.execute("exec sayhello()").close()

def counttest(c):
    print "RUNNING TEST 'counttest'"

    n = countdb(c,'buys')
    for row in c.execute("exec mycount('buys')"):
        if row[0] != n:
           raise db.TestFailed('buys differs')

    n = countdb(c,'visits')
    for row in c.execute("exec mycount('visits')"):
        if row[0] != n:
           raise db.TestFailed('visits differs')

    n = countdb(c,'product')
    for row in c.execute("exec mycount('product')"):
        if row[0] != n:
           raise db.TestFailed('product differs')

    n = countdb(c,'client')
    for row in c.execute("exec mycount('client')"):
        if row[0] != n:
           raise db.TestFailed('client differs')

def grouptest(c):
    print "RUNNING TEST 'grouptest'"

    l = countdb(c,'product')
    x = rnd.randint(0,l-1)
    i = 0
    p = 0

    for row in c.execute('select prod_key from product'):
        if i == x:
           p = row['prod_key']
        i+=1

    cur = c.execute("exec groupbuy(%d)" % p)
    row = cur.fetchone()
    cur.close()

    print('%f | %f | %f | %f | %f | %f' % \
         (row[1], row[2], row[3], row[4], row[5], row[6]))

    sql = "select coal(sum(quantity),0) as qs, coal(avg(quantity),0) as qa, coal(max(quantity),0) as qm, \
                  coal(sum(price),0) as ps,    coal(avg(price),0) as pa,    coal(max(price),0) as pm \
             from buys where destin = %d" % p
    for myrow in c.execute(sql):
        if myrow['qs'] != row[1]:
           raise db.TestFailed("qs differs")
        if float(round(1000*myrow['qa']))/1000 !=  \
           float(round(1000*row[2]))/1000:
           raise db.TestFailed("qa differs: %f - %f" % (myrow['qa'], row[2]))
        if myrow['qm'] != row[3]:
           raise db.TestFailed("qm differs")
        if float(round(1000*myrow['ps']))/1000 != \
           float(round(1000*row[4]))/1000:
           raise db.TestFailed("ps differs: %f - %f" % (myrow['ps'], row[4]))
        if float(round(1000*myrow['pa']))/1000 != \
           float(round(1000*row[5]))/1000:
           raise db.TestFailed("ps differs: %f - %f" % (myrow['pa'], row[5]))
        if float(round(1000*myrow['pm']))/1000 != \
           float(round(1000*row[6]))/1000:
           raise db.TestFailed("pm differs")

if __name__ == "__main__":
    with na.connect("localhost", "55505", None, None, 'db150') as c:

       createprocs(c)
       hellotest(c)

       for i in range(100):
           counttest(c)
           grouptest(c)

    print("PASSED")
