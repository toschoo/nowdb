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
             where wday(stamp) = %d" % w
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

def shortcircuit(c):

    print "RUNNING TEST 'shortcircuit'"

    idx = random.randint(1,len(es)-1)
    l = len(es)

    x = 0
    stmt = "select count(*) from sales where edge='buys' and origin=%d" % es[idx].origin
    with c.execute(stmt) as cur:
         if not cur.ok():
            raise db.TestFailed("not ok: %d (%s)" % (cur.code(), cur.details()))
         for row in cur:
             x = row.field(0)

    print "PIVOT: %d, %d " % (es[idx].origin, x)

    print "short circuit: false or key=x"
    stmt = "select origin from sales \
             where 1=0 or origin = %d" % es[idx].origin
    with c.execute(stmt) as cur:
         if not cur.ok():
            raise db.TestFailed("not ok: %d (%s)" % (cur.code(), cur.details()))
         n=0
         for row in cur:
             n+=1
             if row.field(0) != es[idx].origin:
                raise db.TestFailed("wrong edge: %d / %d" % (row.field(0), es[idx].origin))
         if n != x:
             raise db.TestFailed("wrong number of edges: %d" % n)

    print "short circuit: true or key=x"
    stmt = "select origin from sales \
             where 1=1 or origin = %d" % es[idx].origin
    with c.execute(stmt) as cur:
         if not cur.ok():
            raise db.TestFailed("not ok: %d (%s)" % (cur.code(), cur.details()))
         n=0
         found=False
         for row in cur:
             n+=1
             if row.field(0) == es[idx].origin:
                found=True
         if not found:
             raise db.TestFailed("could not find my edge")
         if n != l:
             raise db.TestFailed("wrong number of edges: %d" % n)

    print "short circuit: key=x or false"
    stmt = "select origin from sales \
             where origin = %d or 1=0" % es[idx].origin
    with c.execute(stmt) as cur:
         if not cur.ok():
            raise db.TestFailed("not ok: %d (%s)" % (cur.code(), cur.details()))
         n=0
         for row in cur:
             n+=1
             if row.field(0) != es[idx].origin:
                raise db.TestFailed("wrong edge: %d / %d" % (row.field(0), es[idx].origin))
         if n != x:
             raise db.TestFailed("wrong number of edges: %d" % n)

    print "short circuit: key=x or true"
    stmt = "select origin from sales \
             where origin = %d or 1=1" % es[idx].origin
    with c.execute(stmt) as cur:
         if not cur.ok():
            raise db.TestFailed("not ok: %d (%s)" % (cur.code(), cur.details()))
         n=0
         found=False
         for row in cur:
             n+=1
             if row.field(0) != es[idx].origin:
                found=True
         if not found:
             raise db.TestFailed("could not find my edge")
         if n != l:
             raise db.TestFailed("wrong number of edges: %d" % n)

    print "short circuit: false and key=x"
    stmt = "select origin from sales \
             where 1=0 and origin = %d" % es[idx].origin
    with c.execute(stmt) as cur:
         if cur.ok():
            raise db.TestFailed("found false..." % (cur.code(), cur.details()))

    print "short circuit: true and key=x"
    stmt = "select origin from sales \
             where 1=1 and origin = %d" % es[idx].origin
    with c.execute(stmt) as cur:
         if not cur.ok():
            raise db.TestFailed("not ok: %d (%s)" % (cur.code(), cur.details()))
         n=0
         for row in cur:
             n+=1
             if row.field(0) != es[idx].origin:
                raise db.TestFailed("wrong edge: %d / %d" % (row.field(0), es[idx].origin))
         if n != x:
             raise db.TestFailed("wrong number of edges: %d" % n)

    print "short circuit: key=x and false"
    stmt = "select origin from sales \
             where origin = %d and 1=0" % es[idx].origin
    with c.execute(stmt) as cur:
         if cur.ok():
            raise db.TestFailed("found false..." % (cur.code(), cur.details()))

    print "short circuit: key=x and true"
    stmt = "select origin from sales \
             where origin = %d and 1=1" % es[idx].origin
    with c.execute(stmt) as cur:
         if not cur.ok():
            raise db.TestFailed("not ok: %d (%s)" % (cur.code(), cur.details()))
         n=0
         for row in cur:
             n+=1
             if row.field(0) != es[idx].origin:
                raise db.TestFailed("wrong edge: %d / %d" % (row.field(0), es[idx].origin))
         if n != x:
             raise db.TestFailed("wrong number of edges: %d" % n)

if __name__ == "__main__":
    with now.Connection("localhost", "55505", None, None) as c:
        (ps, cs, es) = db.loadDB(c, "db100")

        for i in range(0,IT):
            roundfloats(c)
            weekdays(c)
            shortcircuit(c)

        print "PASSED"

