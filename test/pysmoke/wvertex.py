#!/usr/bin/env python

# to be run after basicsmoke

import now
import db
import datetime
import time
import random

IT = 10

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
            raise db.TestFailed("could not find my edge")

if __name__ == "__main__":
    with now.Connection("localhost", "55505", None, None) as c:
        (ps, cs, es) = db.loadDB(c, "db100")

        for i in range(0,IT):
            roundfloats(c)
            weekdays(c)

