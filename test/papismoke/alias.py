#!/usr/bin/env python

import db
import nowapi as na
import random

IT = 1

def countdb(c,t):
    for row in c.execute("select count(*) from %s" % t, rowformat=na.listrow):
        return row[0]

def checkprod(c,k):
    n=0
    for row in c.execute("select prod_key from product where prod_key = %d" % k):
        if row['prod_key'] != k:
           raise db.TestFailed('keys differ')
        n+=1
    if n != 1:
       raise db.TestFailed('expected: one row (have %d)' % n)

def wrongtest(c):

    print("RUNNING TEST 'wrongtest'")

    try:
       c.execute("select count(*) cnt from product")
       raise db.TestFailed("'field alias' passed")
    except Exception as x:
       print(str(x))

    try:
       c.execute("select count(*) is cnt from product")
       raise db.TestFailed("'field is alias' passed")
    except Exception as x:
       print(str(x))

    try:
       c.execute("select count(*) as 123 from product")
       raise db.TestFailed("'field as number' passed")
    except Exception as x:
       print(str(x))

    try:
       c.execute("select count(*) as 'count' from product")
       raise db.TestFailed("'field as string' passed")
    except Exception as x:
       print(str(x))

    try:
       c.execute("select count(*) as a b from product")
       raise db.TestFailed("'field as a b' passed")
    except Exception as x:
       print(str(x))

    try:
       c.execute("select count(*) as a as b from product")
       raise db.TestFailed("'field as a as b' passed")
    except Exception as x:
       print(str(x))

    try:
       c.execute("select count(*) sa a from product")
       raise db.TestFailed("'field sa a' passed")
    except Exception as x:
       print(str(x))

    try:
       c.execute("select count(*) as from from product")
       raise db.TestFailed("'field as from' passed")
    except Exception as x:
       print(str(x))

    try:
       c.execute("select count(*) as from product")
       raise db.TestFailed("empty alias passed")
    except Exception as x:
       print(str(x))

    '''
    this won't work (parser waits for more input)
    try:
       c.execute("select count(*) as c")
       raise db.TestFailed("no from passed")
    except Exception as x:
       print(str(x))
    '''

def aliastest(c):
    
    print("RUNNING TEST 'aliastest'")

    l = countdb(c, 'product')
    for row in c.execute("select count(*) as count from product"):
        if row['count'] != l:
           raise db.TestFailed('count differs')

    for row in c.execute("select count(*) + 1 as c1 from product"):
        if row['c1'] != l+1:
           raise db.TestFailed('c1 differs')

    for row in c.execute("select null as nix from product"):
        if row['nix'] is not None:
           raise db.TestFailed('not null!')

    x = random.randint(0,l)
    i = 0
    for row in c.execute("select prod_key as key from product"):
        if i == x:
           print(row['key'])
           checkprod(c,row['key'])
           break
        i+=1
    i = 0
    for row in c.execute("select prod_key as key, prod_desc as d from product"):
        if i == x:
           checkprod(c,row['key'])
           break
        i+=1
    i = 0
    for row in c.execute("select prod_key as askey, prod_desc as d from product"):
        if i == x:
           checkprod(c,row['askey'])
           break
        i+=1
    i = 0
    for row in c.execute("select prod_desc as d, prod_key as key, prod_desc as d from product"):
        if i == x:
           checkprod(c,row['key'])
           break
        i+=1
    i = 0
    for row in c.execute("select sum(coal(prod_price,0)) as sum, prod_key as key, prod_desc as d from product"):
        if i == x:
           checkprod(c,row['key'])
           break
        i+=1
    i = 0
    for row in c.execute("select * from product"):
        if i == x:
           k = row['prod_key']
           checkprod(c,row['prod_key'])
           break
        i+=1
    i = 0
    for row in c.execute("select * from product where prod_key = %d" % k):
        checkprod(c,row['prod_key'])

    for row in c.execute("select 'from' as one, 'bla' as from_bla, prod_key as key, 'from' as my_from from product"):
        if i == x:
           checkprod(c,row['key'])
           break
        i+=1

if __name__ == "__main__":

    random.seed()

    with na.connect("localhost", "55505", None, None, 'db150') as c:

        wrongtest(c)
        for i in range(IT):
            aliastest(c)

    print("PASSED")
