#!/usr/bin/env python

# to be run after basicsmoke

import now
import db
import datetime
import time
import random

IT = 10

def countvrtxby(vs, key, pivot):
    n=0
    for v in vs:
        if key(v) == pivot:
           n += 1
    return n

def roundfloats(c):
    print "RUNNING TEST 'roundfloats'"

    idx = random.randint(1,len(ps)-1)

    print "PIVOT: %d - %f" % (ps[idx].key, \
                              ps[idx].price)

    print "TOINT"
    w = int(round(ps[idx].price*100))/100
    stmt = "select prod_key, prod_price \
              from product \
             where toint(round(prod_price*100))/100 = %d" % w
    found = False
    with c.execute(stmt) as cur:
         if not cur.ok():
            raise db.TestFailed("not ok: %d (%s)" % (cur.code(), cur.details()))
         for row in cur:
             print "FOUND: %d - %f" % (row.field(0), \
                                       row.field(1))
             if row.field(0) == ps[idx].key:
                found=True
         if not found:
            raise db.TestFailed("could not find my product")

    print "CEIL/FLOOR"
    stmt = "select prod_key, prod_price \
              from product \
             where ceil(prod_price)  >= %f \
               and floor(prod_price) <= %f" % (ps[idx].price, \
                                               ps[idx].price)
    found = False
    with c.execute(stmt) as cur:
         if not cur.ok():
            raise db.TestFailed("not ok: %d (%s)" % (cur.code(), cur.details()))
         for row in cur:
             print "FOUND: %d - %f" % (row.field(0), \
                                       row.field(1))
             if row.field(0) == ps[idx].key:
                found=True
         if not found:
            raise db.TestFailed("could not find my product")

    print "SQUARE"
    stmt = "select prod_key, prod_price \
              from product \
             where (prod_price^2)/prod_price     >= %f \
               and (prod_price^(1/2))*prod_price <= %f" % (ps[idx].price-0.1, \
                                                           ps[idx].price+0.1)
    found = False
    with c.execute(stmt) as cur:
         if not cur.ok():
            raise db.TestFailed("not ok: %d (%s)" % (cur.code(), cur.details()))
         for row in cur:
             print "FOUND: %d - %f" % (row.field(0), \
                                       row.field(1))
             if row.field(0) == ps[idx].key:
                found=True
         if not found:
            raise db.TestFailed("could not find my edge")

def weekdays(c):
    print "RUNNING TEST 'weekdays'"

    idx = random.randint(1,len(cs)-1)

    print "PIVOT: %d - %d - %d, %d, %d, %d, %d, %d" % (cs[idx].key, \
                                                       now.dt2now(cs[idx].birthdate),
                                                       cs[idx].birthdate.weekday(),
                                                       cs[idx].birthdate.day,
                                                       cs[idx].birthdate.hour,
                                                       cs[idx].birthdate.minute,
                                                       cs[idx].birthdate.second,
                                                       cs[idx].birthdate.microsecond)
    w = (cs[idx].birthdate.weekday()+1)%7

    print "WEEKDAY"
    stmt = "select client_key, wday(birthdate) \
              from client \
             where wday(birthdate) = %d" % w
    found = False
    with c.execute(stmt) as cur:
         if not cur.ok():
            raise db.TestFailed("not ok: %d (%s)" % (cur.code(), cur.details()))
         for row in cur:
             print "FOUND: %d - %d" % (row.field(0), \
                                       row.field(1))
             if row.field(0) == cs[idx].key:
                found=True
         if not found:
            raise db.TestFailed("could not find my client")

    print "DAY OF MONTH"
    stmt = "select client_key, mday(birthdate) \
              from client \
             where mday(birthdate) = %d" % (cs[idx].birthdate.day)
    found = False
    with c.execute(stmt) as cur:
         if not cur.ok():
            raise db.TestFailed("not ok: %d (%s)" % (cur.code(), cur.details()))
         for row in cur:
             print "FOUND: %d - %d" % (row.field(0), \
                                       row.field(1))
             if row.field(0) == cs[idx].key:
                found=True
         if not found:
            raise db.TestFailed("could not find my client")

    print "HOUR OF DAY"
    stmt = "select client_key, hour(birthdate) \
              from client \
             where hour(birthdate) = %d" % (cs[idx].birthdate.hour)
    found = False
    with c.execute(stmt) as cur:
         if not cur.ok():
            raise db.TestFailed("not ok: %d (%s)" % (cur.code(), cur.details()))
         for row in cur:
             print "FOUND: %d - %d" % (row.field(0), \
                                       row.field(1))
             if row.field(0) == cs[idx].key:
                found=True
         if not found:
            raise db.TestFailed("could not find my client")

    print "MINUTE OF HOUR"
    stmt = "select client_key, minute(birthdate) \
              from client \
             where minute(birthdate) = %d" % (cs[idx].birthdate.minute)
    found = False
    with c.execute(stmt) as cur:
         if not cur.ok():
            raise db.TestFailed("not ok: %d (%s)" % (cur.code(), cur.details()))
         for row in cur:
             print "FOUND: %d - %d" % (row.field(0), \
                                       row.field(1))
             if row.field(0) == cs[idx].key:
                found=True
         if not found:
            raise db.TestFailed("could not find my client")

    print "SECOND OF MINUTE"
    stmt = "select client_key, second(birthdate) \
              from client \
             where second(birthdate) = %d" % (cs[idx].birthdate.second)
    found = False
    with c.execute(stmt) as cur:
         if not cur.ok():
            raise db.TestFailed("not ok: %d (%s)" % (cur.code(), cur.details()))
         for row in cur:
             print "FOUND: %d - %d" % (row.field(0), \
                                       row.field(1))
             if row.field(0) == cs[idx].key:
                found=True
         if not found:
            raise db.TestFailed("could not find my client")

    print "MICROSECONDs"
    stmt = "select client_key, micro(birthdate) \
              from client \
             where micro(birthdate) = %d" % (cs[idx].birthdate.microsecond)
    found = False
    with c.execute(stmt) as cur:
         if not cur.ok():
            raise db.TestFailed("not ok: %d (%s)" % (cur.code(), cur.details()))
         for row in cur:
             print "FOUND: %d - %d" % (row.field(0), \
                                       row.field(1))
             if row.field(0) == cs[idx].key:
                found=True
         if not found:
            raise db.TestFailed("could not find my client")

def shortcircuit(c):

    print "RUNNING TEST 'shortcircuit'"

    idx = random.randint(1,len(cs)-1)
    l = len(cs)

    print "PIVOT: %d " % (cs[idx].key)

    print "short circuit: false or key=x"
    stmt = "select client_key from client \
             where 1=0 or client_key = %d" % cs[idx].key
    with c.execute(stmt) as cur:
         if not cur.ok():
            raise db.TestFailed("not ok: %d (%s)" % (cur.code(), cur.details()))
         n=0
         for row in cur:
             n+=1
             if row.field(0) != cs[idx].key:
                raise db.TestFailed("wrong client: %d / %d" % (row.field(0), cs[idx].key))
         if n != 1:
             raise db.TestFailed("wrong number of clients: %d" % n)

    print "short circuit: true or key=x"
    stmt = "select client_key from client \
             where 1=1 or client_key = %d" % cs[idx].key
    with c.execute(stmt) as cur:
         if not cur.ok():
            raise db.TestFailed("not ok: %d (%s)" % (cur.code(), cur.details()))
         n=0
         found=False
         for row in cur:
             n+=1
             if row.field(0) == cs[idx].key:
                found=True
         if not found:
             raise db.TestFailed("could not find my client")
         if n != l:
             raise db.TestFailed("wrong number of clients: %d" % n)

    print "short circuit: key=x or false"
    stmt = "select client_key from client \
             where client_key = %d or 1=0" % cs[idx].key
    with c.execute(stmt) as cur:
         if not cur.ok():
            raise db.TestFailed("not ok: %d (%s)" % (cur.code(), cur.details()))
         n=0
         for row in cur:
             n+=1
             if row.field(0) != cs[idx].key:
                raise db.TestFailed("wrong client: %d / %d" % (row.field(0), cs[idx].key))
         if n != 1:
             raise db.TestFailed("wrong number of clients: %d" % n)

    print "short circuit: key=x or true"
    stmt = "select client_key from client \
             where client_key = %d or 1=1" % cs[idx].key
    with c.execute(stmt) as cur:
         if not cur.ok():
            raise db.TestFailed("not ok: %d (%s)" % (cur.code(), cur.details()))
         n=0
         found=False
         for row in cur:
             n+=1
             if row.field(0) != cs[idx].key:
                found=True
         if not found:
             raise db.TestFailed("could not find my client")
         if n != l:
             raise db.TestFailed("wrong number of clients: %d" % n)

    print "short circuit: false and key=x"
    stmt = "select client_key from client \
             where 1=0 and client_key = %d" % cs[idx].key
    with c.execute(stmt) as cur:
         if cur.ok():
            raise db.TestFailed("found false..." % (cur.code(), cur.details()))

    print "short circuit: true and key=x"
    stmt = "select client_key from client \
             where 1=1 and client_key = %d" % cs[idx].key
    with c.execute(stmt) as cur:
         if not cur.ok():
            raise db.TestFailed("not ok: %d (%s)" % (cur.code(), cur.details()))
         n=0
         for row in cur:
             n+=1
             if row.field(0) != cs[idx].key:
                raise db.TestFailed("wrong client: %d / %d" % (row.field(0), cs[idx].key))
         if n != 1:
             raise db.TestFailed("wrong number of clients: %d" % n)

    print "short circuit: key=x and false"
    stmt = "select client_key from client \
             where client_key = %d and 1=0" % cs[idx].key
    with c.execute(stmt) as cur:
         if cur.ok():
            raise db.TestFailed("found false..." % (cur.code(), cur.details()))

    print "short circuit: key=x and true"
    stmt = "select client_key from client \
             where client_key = %d and 1=1" % cs[idx].key
    with c.execute(stmt) as cur:
         if not cur.ok():
            raise db.TestFailed("not ok: %d (%s)" % (cur.code(), cur.details()))
         n=0
         for row in cur:
             n+=1
             if row.field(0) != cs[idx].key:
                raise db.TestFailed("wrong client: %d / %d" % (row.field(0), cs[idx].key))
         if n != 1:
             raise db.TestFailed("wrong number of clients: %d" % n)

def casefun(c):

    print "RUNNING TEST 'casefun'"

    # case appears in where
    cnt=0
    for p in ps:
        if p.cat < 4:
           cnt += 1

    stmt = "select prod_key, prod_cat from product " \
           " where case " \
           "          when prod_cat = 1 then 0 " \
           "          when prod_cat = 2 then 1 " \
           "          when prod_cat = 3 then 2 " \
           "          when prod_cat = 4 then 3 " \
           "          when prod_cat = 5 then 4 " \
           "       end < 3"

    with c.execute(stmt) as cur:
         if not cur.ok():
            raise db.TestFailed("not ok: %d (%s)" % (cur.code(), cur.details()))
         n=0
         for row in cur:
             n+=1
    print "%d ?= %d" % (cnt, n)
    if n != cnt:
       raise db.TestFailed("not equal: %d != %d " % (n, cnt))

    # where is case
    cnt=0
    for p in ps:
        if p.cat < 4:
           cnt += 1

    stmt = "select prod_key, prod_cat from product " \
           " where case " \
           "          when prod_cat = 1 then true " \
           "          when prod_cat = 2 then true " \
           "          when prod_cat = 3 then true " \
           "          else false " \
           "       end"

    with c.execute(stmt) as cur:
         if not cur.ok():
            raise db.TestFailed("not ok: %d (%s)" % (cur.code(), cur.details()))
         n=0
         for row in cur:
             n+=1
    print "%d ?= %d" % (cnt, n)
    if n != cnt:
       raise db.TestFailed("not equal: %d != %d " % (n, cnt))

    # where is case with non-boolean values
    cnt=0
    for p in ps:
        if p.cat == 2 or p.cat == 3:
           cnt += 1

    stmt = "select prod_key, prod_cat from product " \
           " where case " \
           "          when prod_cat = 1 then false" \
           "          when prod_cat = 2 then 'two'" \
           "          when prod_cat = 3 then 3" \
           "          else false " \
           "       end"

    with c.execute(stmt) as cur:
         if not cur.ok():
            raise db.TestFailed("not ok: %d (%s)" % (cur.code(), cur.details()))
         n=0
         for row in cur:
             n+=1
    print "%d ?= %d" % (cnt, n)
    if n != cnt:
       raise db.TestFailed("not equal: %d != %d " % (n, cnt))

def countalledges(keys, n):
    x = 0
    for i in range(n):
        x += countedgeby(es, lambda e: e.destin, keys[i])
    return x

def mkin(keys,key,n):
    x = 0
    stmt = "("

    for k in keys:
       stmt += str(key(k))
       x+=1
       if x < n:
          stmt += ","
       else:
          break

    stmt += ")"
    return stmt

def infun(c):

    print "RUNNING TEST 'infun'"

    stmt = "select count(*) from product where prod_key in %s"
    for i in range(1, 100, 5):
        print "%d out of %d" % (i, len(ps))
        # print stmt % mkin(ps, lambda k: k.key, i)
        with c.execute(stmt % mkin(ps, lambda k: k.key, i)) as cur:
             for row in cur:
                 if row.field(0) != i:
                    raise db.TestFailed("count does not match for %d: %d" % (i, row.field(0)))

if __name__ == "__main__":
    with now.Connection("localhost", "55505", None, None) as c:
        (ps, cs, ss, es, vs) = db.loadDB(c, "db100")

        for i in range(0,IT):
            roundfloats(c)
            weekdays(c)
            shortcircuit(c)
            casefun(c)
            infun(c)

        print "PASSED" 

