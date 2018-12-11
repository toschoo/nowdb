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

    idx = random.randint(1,len(es)-1)

    print "PIVOT: %d - %d - %d - %f" % (es[idx].origin, \
                                        es[idx].destin, \
                                        now.dt2now(es[idx].tp),
                                        es[idx].weight2)

    print "TOINT"
    w = int(round(es[idx].weight2*100))/100
    stmt = "select origin, destin, stamp, \
                   weight, weight2    \
              from sales \
             where toint(round(weight2*100))/100 = %d" % w
    found = False
    with c.execute(stmt) as cur:
         if not cur.ok():
            raise db.TestFailed("not ok: %d (%s)" % (cur.code(), cur.details()))
         for row in cur:
             print "FOUND: %d - %d - %d - %f" % (row.field(0), \
                                                 row.field(1), \
                                                 row.field(2), \
                                                 row.field(4))
             if row.field(0) == es[idx].origin and \
                row.field(1) == es[idx].destin and \
                row.field(2) == now.dt2now(es[idx].tp):
                found=True
         if not found:
            raise db.TestFailed("could not find my edge")

    print "CEIL/FLOOR"
    stmt = "select origin, destin, stamp, \
                   weight, weight2    \
              from sales \
             where ceil(weight2) >= %f \
               and floor(weight2) <= %f" % (es[idx].weight2, \
                                            es[idx].weight2)
    found = False
    with c.execute(stmt) as cur:
         if not cur.ok():
            raise db.TestFailed("not ok: %d (%s)" % (cur.code(), cur.details()))
         for row in cur:
             print "FOUND: %d - %d - %d - %f" % (row.field(0), \
                                                 row.field(1), \
                                                 row.field(2), \
                                                 row.field(4))
             if row.field(0) == es[idx].origin and \
                row.field(1) == es[idx].destin and \
                row.field(2) == now.dt2now(es[idx].tp):
                found=True
         if not found:
            raise db.TestFailed("could not find my edge")

    print "SQUARE"
    stmt = "select origin, destin, stamp, \
                   weight, weight2    \
              from sales \
             where (weight2^2)/weight2     >= %f \
               and (weight2^(1/2))*weight2 <= %f" % (es[idx].weight2-0.1, \
                                                     es[idx].weight2+0.1)
    found = False
    with c.execute(stmt) as cur:
         if not cur.ok():
            raise db.TestFailed("not ok: %d (%s)" % (cur.code(), cur.details()))
         for row in cur:
             print "FOUND: %d - %d - %d - %f" % (row.field(0), \
                                                 row.field(1), \
                                                 row.field(2), \
                                                 row.field(4))
             if row.field(0) == es[idx].origin and \
                row.field(1) == es[idx].destin and \
                row.field(2) == now.dt2now(es[idx].tp):
                found=True
         if not found:
            raise db.TestFailed("could not find my edge")

def weekdays(c):
    print "RUNNING TEST 'weekdays'"

    idx = random.randint(1,len(es)-1)

    print "PIVOT: %d - %d - %d - %d, %d, %d, %d, %d, %d" % (es[idx].origin, \
                                                            es[idx].destin, \
                                                            now.dt2now(es[idx].tp),
                                                            es[idx].tp.weekday(),
                                                            es[idx].tp.day,
                                                            es[idx].tp.hour,
                                                            es[idx].tp.minute,
                                                            es[idx].tp.second,
                                                            es[idx].tp.microsecond)

    w = (es[idx].tp.weekday()+1)%7

    print "WEEKDAY"
    stmt = "select origin, destin, stamp, wday(stamp) \
              from sales \
             where wday(stamp) = %d" % (es[idx].tp.weekday()+1)
    found = False
    cnt = 0
    with c.execute(stmt) as cur:
         if not cur.ok():
            raise db.TestFailed("not ok: %d (%s)" % (cur.code(), cur.details()))
         for row in cur:
             print "FOUND: %d - %d - %d - %d" % (row.field(0), \
                                                 row.field(1), \
                                                 row.field(2), \
                                                 row.field(3))
             cnt += 1
             if row.field(0) == es[idx].origin and \
                row.field(1) == es[idx].destin and \
                row.field(2) == now.dt2now(es[idx].tp):
                found=True
         if not found:
            raise db.TestFailed("could not find my edge")

    print "DAY OF MONTH"
    stmt = "select origin, destin, stamp, mday(stamp) \
              from sales \
             where mday(stamp) = %d" % (es[idx].tp.day)
    found = False
    cnt2 = 0
    with c.execute(stmt) as cur:
         if not cur.ok():
            raise db.TestFailed("not ok: %d (%s)" % (cur.code(), cur.details()))
         for row in cur:
             print "FOUND: %d - %d - %d - %d" % (row.field(0), \
                                                 row.field(1), \
                                                 row.field(2), \
                                                 row.field(3))
             cnt2 += 1
             if row.field(0) == es[idx].origin and \
                row.field(1) == es[idx].destin and \
                row.field(2) == now.dt2now(es[idx].tp):
                found=True
         if not found:
            raise db.TestFailed("could not find my edge")
         if cnt != cnt2:
            raise db.TestFailed("day of week(%d) != day of month(%d)" % (cnt, cnt2))

    print "HOUR OF DAY"
    stmt = "select origin, destin, stamp, hour(stamp) \
              from sales \
             where hour(stamp) = %d" % (es[idx].tp.hour)
    found = False
    with c.execute(stmt) as cur:
         if not cur.ok():
            raise db.TestFailed("not ok: %d (%s)" % (cur.code(), cur.details()))
         for row in cur:
             print "FOUND: %d - %d - %d - %d" % (row.field(0), \
                                                 row.field(1), \
                                                 row.field(2), \
                                                 row.field(3))
             if row.field(0) == es[idx].origin and \
                row.field(1) == es[idx].destin and \
                row.field(2) == now.dt2now(es[idx].tp):
                found=True
         if not found:
            raise db.TestFailed("could not find my edge")

    print "MINUTE OF HOUR"
    stmt = "select origin, destin, stamp, minute(stamp) \
              from sales \
             where minute(stamp) = %d" % (es[idx].tp.minute)
    found = False
    with c.execute(stmt) as cur:
         if not cur.ok():
            raise db.TestFailed("not ok: %d (%s)" % (cur.code(), cur.details()))
         for row in cur:
             print "FOUND: %d - %d - %d - %d" % (row.field(0), \
                                                 row.field(1), \
                                                 row.field(2), \
                                                 row.field(3))
             if row.field(0) == es[idx].origin and \
                row.field(1) == es[idx].destin and \
                row.field(2) == now.dt2now(es[idx].tp):
                found=True
         if not found:
            raise db.TestFailed("could not find my edge")

    print "SECOND OF MINUTE"
    stmt = "select origin, destin, stamp, second(stamp) \
              from sales \
             where second(stamp) = %d" % (es[idx].tp.second)
    found = False
    with c.execute(stmt) as cur:
         if not cur.ok():
            raise db.TestFailed("not ok: %d (%s)" % (cur.code(), cur.details()))
         for row in cur:
             print "FOUND: %d - %d - %d - %d" % (row.field(0), \
                                                 row.field(1), \
                                                 row.field(2), \
                                                 row.field(3))
             if row.field(0) == es[idx].origin and \
                row.field(1) == es[idx].destin and \
                row.field(2) == now.dt2now(es[idx].tp):
                found=True
         if not found:
            raise db.TestFailed("could not find my edge")

    print "MICROSECONDs"
    stmt = "select origin, destin, stamp, micro(stamp) \
              from sales \
             where micro(stamp) = %d" % (es[idx].tp.microsecond)
    found = False
    with c.execute(stmt) as cur:
         if not cur.ok():
            raise db.TestFailed("not ok: %d (%s)" % (cur.code(), cur.details()))
         for row in cur:
             print "FOUND: %d - %d - %d - %d" % (row.field(0), \
                                                 row.field(1), \
                                                 row.field(2), \
                                                 row.field(3))
             if row.field(0) == es[idx].origin and \
                row.field(1) == es[idx].destin and \
                row.field(2) == now.dt2now(es[idx].tp):
                found=True
         if not found:
            raise db.TestFailed("could not find my edge")

if __name__ == "__main__":
    with now.Connection("localhost", "55505", None, None) as c:
        (ps, cs, es) = db.loadDB(c, "db100")

        for i in range(0,IT):
            roundfloats(c)
            weekdays(c)

