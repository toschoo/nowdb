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

def hellotest(c):
    print "RUNNING TEST 'hellotest'"

    c.execute("exec sayhello()")

def counttest(c):
    print "RUNNING TEST 'counttest'"

    n = countdb(c,'buys')
    for row in c.execute("exec mycount('buys')"):
        if row[0] != n:
           raise db.TestFailed('buys differs')

    '''
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
    '''

if __name__ == "__main__":
    with na.connect("localhost", "55505", None, None, 'db150') as c:

       createprocs(c)
       hellotest(c)
       # counttest(c)

    print("PASSED")
