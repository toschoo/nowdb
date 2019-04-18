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

if __name__ == "__main__":

    print("running exectest for language lua")

    with na.connect("localhost", "55505", None, None, 'db150') as c:

       createprocs(c, 'lua')

       locktest(c)
       uniquetest(c)
       # recache

       for i in range(10):
           pass

    print("PASSED")
