#!/usr/bin/env python

import db
import now
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

def createprocs(c):
    print "RUNNING 'createprocs'"

    c.execute("drop procedure timetest if exists").close()
    c.execute("create procedure db.timetest(t time) language python").close()

    c.execute("drop procedure easter if exists").close()
    c.execute("create procedure db.easter(y uint) language python").close()

    c.execute("drop procedure myfloatadd if exists").close()
    c.execute("create procedure db.myfloatadd(a float, b float) language python").close()

    c.execute("drop procedure myintadd if exists").close()
    c.execute("create procedure db.myintadd(a int, b int) language python").close()

    c.execute("drop procedure myuintadd if exists").close()
    c.execute("create procedure db.myuintadd(a uint, b uint) language python").close()

    c.execute("drop procedure mylogic if exists").close()
    c.execute("create procedure db.mylogic(o text, a bool, b bool) language python").close()

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

def addtest(c):

    print "RUNNING TEST 'addtest'"

    # float
    a = rnd.uniform(0.1,100.0)
    b = rnd.uniform(0.1,100.0)
    
    c1 = a+b

    for row in c.execute("exec myfloatadd(%f, %f)" % (a,b)):
        c2 = row[0]
        print("%f ?= %f" % (c1,c2))

    if (round(c1*100)/100) != (round(c2*100))/100:
       raise db.TestFailed("float add differs: %f | %f" % (c1,c2))

    # signed int 
    a = rnd.randint(0,100)
    b = rnd.randint(0,100)

    if rnd.randint(0,2) == 0:
       a *= -1

    if rnd.randint(0,2) == 0:
       b *= -1
    
    c1 = a+b

    for row in c.execute("exec myintadd(%d, %d)" % (a,b)):
        c2 = row[0]
        print("%d ?= %d" % (c1,c2))

    if c1 != c2:
       raise db.TestFailed("int add differs: %f | %f" % (c1,c2))

    # unsigned int 
    a = rnd.randint(0,100)
    b = rnd.randint(0,100)

    c1 = a+b

    for row in c.execute("exec myuintadd(%d, %d)" % (a,b)):
        c2 = row[0]
        print("%d ?= %d" % (c1,c2))

    if c1 != c2:
       raise db.TestFailed("uint add differs: %f | %f" % (c1,c2))
    

def logictest(c):

    print "RUNNING TEST 'logictest'"

    a = True if rnd.randint(0,1) == 0 else False
    b = False if rnd.randint(0,1) == 0 else True

    c1 = a and b 

    for row in c.execute("exec mylogic('and', %s, %s)" % (a,b)):
        c2 = row[0]
        print("%s ?= %s" % (c1,c2))

    if c1 != c2:
       raise db.TestFailed("AND failed: %s | %s" % (c1,c2))

    c1 = a or b 

    for row in c.execute("exec mylogic('or', %s, %s)" % (a,b)):
        c2 = row[0]
        print("%s ?= %s" % (c1,c2))

    if c1 != c2:
       raise db.TestFailed("OR failed: %s | %s" % (c1,c2))

    c1 = (a and not b) or (not a and b) 

    for row in c.execute("exec mylogic('xor', %s, %s)" % (a,b)):
        c2 = row[0]
        print("%s ?= %s" % (c1,c2))

    if c1 != c2:
       raise db.TestFailed("XOR failed: %s | %s" % (c1,c2))

def express(c):

    print "RUNNING TEST 'express'"

    # arithmetic
    a = rnd.randint(0,100)
    b = rnd.randint(0,100)

    d = -1 if rnd.randint(0,2) == 0 else 1

    c1 = math.pi*a+d*b

    for row in c.execute("exec myfloatadd(pi()*%d, %d*%d)" % (a,d,b)):
        c2 = row[0]
        print("%d ?= %d" % (c1,c2))

    if float(round(c1*1000)/1000) != float(round(c2*1000))/1000:
       raise db.TestFailed("float add differs: %f | %f" % (c1,c2))

    # logic
    c1 = (a > b) and (d <= 0)
    
    for row in c.execute("exec mylogic('and', %d>%d, %d<=0)" % (a,b,d)):
        c2 = row[0]
        print("%s ?= %s" % (c1,c2))

    if c1 != c2:
       raise db.TestFailed("LOGIC failed: %s | %s" % (c1,c2))

def invalidpars(c):

    print "RUNNING TEST 'invalidpars'"

    try:
       c.execute("exec myfloatadd(price1, price2)")
       raise db.TestFailed("called exec with fields")
    except Exception as x:
       print("caught %s" % str(x))

    try:
       c.execute("exec myfloatadd(pi(), sum(1))")
       raise db.TestFailed("called exec with agg")
    except Exception as x:
       print("caught %s" % str(x))

def timetest(c):

    print "RUNNING TEST 'timetest'"

    x = countdb(c,'buys')

    for row in c.execute("exec timetest(now())"):
        print('time: %d' % row[0])
        if row[0] != x:
           raise db.TestFailed("there are edges from the future")

    for row in c.execute("select now() as now"):
        ds = row['now'].strftime(now.TIMEFORMAT)

    for row in c.execute("exec timetest('%s')" % ds):
        print('time: %d' % row[0])
        if row[0] != x:
           raise db.TestFailed("there are edges from the future")

def eastertest(c):

    print "RUNNING TEST 'eastertest'"

    for i in range(50):
        for row in c.execute("exec easter(%d)" % (i+2000)):
            print('easter %d: %s' % (i+2000,row[0].strftime(now.DATEFORMAT)))

if __name__ == "__main__":
    with na.connect("localhost", "55505", None, None, 'db150') as c:

       createprocs(c)
       hellotest(c)
       timetest(c)
       eastertest(c)

       for i in range(100):
           counttest(c)
           grouptest(c)
           addtest(c)
           logictest(c)
           express(c)
           invalidpars(c)

    print("PASSED")
