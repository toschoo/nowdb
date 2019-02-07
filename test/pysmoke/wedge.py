#!/usr/bin/env python

# to be run after basicsmoke

import now
import db
import datetime
import time
import random

IT = 10

def countedge(es,edge):
    n=0
    for e in es:
        if e.edge == edge:
           n+=1
    return n

def countedgeby(es, key, pivot):
    n=0
    for e in es:
        if key(e) == pivot:
           n += 1
    return n

def roundfloats(c):
    print "RUNNING TEST 'roundfloats'"

    idx = random.randint(1,len(es)-1)
    if es[idx].edge != 'buys':
       idx-=1

    print "PIVOT: %d - %d - %d - %f" % (es[idx].origin, \
                                        es[idx].destin, \
                                        now.dt2now(es[idx].tp),
                                        es[idx].weight2)

    print "TOINT"
    w = int(round(es[idx].weight2*100))/100
    stmt = "select edge, origin, destin, stamp, \
                   weight, weight2    \
              from sales \
             where toint(round(weight2*100))/100 = %d" % w
    found = False
    with c.execute(stmt) as cur:
         if not cur.ok():
            raise db.TestFailed("not ok: %d (%s)" % (cur.code(), cur.details()))
         for row in cur:
             if row.field(0) != 'buys':
                continue

             print "FOUND: %d - %d - %d - %f" % (row.field(1), \
                                                 row.field(2), \
                                                 row.field(3), \
                                                 row.field(5))
             if row.field(1) == es[idx].origin and \
                row.field(2) == es[idx].destin and \
                row.field(3) == now.dt2now(es[idx].tp):
                found=True
         if not found:
            raise db.TestFailed("could not find my edge")

    print "CEIL/FLOOR"
    stmt = "select edge, origin, destin, stamp, \
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
             if row.field(0) != 'buys':
                continue
             print "FOUND: %d - %d - %d - %f" % (row.field(1), \
                                                 row.field(2), \
                                                 row.field(3), \
                                                 row.field(5))
             if row.field(1) == es[idx].origin and \
                row.field(2) == es[idx].destin and \
                row.field(3) == now.dt2now(es[idx].tp):
                found=True
         if not found:
            raise db.TestFailed("could not find my edge")

    print "SQUARE"
    stmt = "select edge, origin, destin, stamp, \
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
             if row.field(0) != 'buys':
                continue
             print "FOUND: %d - %d - %d - %f" % (row.field(1), \
                                                 row.field(2), \
                                                 row.field(3), \
                                                 row.field(5))
             if row.field(1) == es[idx].origin and \
                row.field(2) == es[idx].destin and \
                row.field(3) == now.dt2now(es[idx].tp):
                found=True
         if not found:
            raise db.TestFailed("could not find my edge")

def weekdays(c):
    print "RUNNING TEST 'weekdays'"

    idx = random.randint(1,len(es)-1)
    if es[idx].edge != 'buys':
       idx -= 1

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
    stmt = "select edge, origin, destin, stamp, wday(stamp) \
              from sales \
             where wday(stamp) = %d" % w
    found = False
    cnt = 0
    with c.execute(stmt) as cur:
         if not cur.ok():
            raise db.TestFailed("not ok: %d (%s)" % (cur.code(), cur.details()))
         for row in cur:
             if row.field(0) != 'buys':
                continue
             print "FOUND: %d - %d - %d - %d" % (row.field(1), \
                                                 row.field(2), \
                                                 row.field(3), \
                                                 row.field(4))
             cnt += 1
             if row.field(1) == es[idx].origin and \
                row.field(2) == es[idx].destin and \
                row.field(3) == now.dt2now(es[idx].tp):
                found=True
         if not found:
            raise db.TestFailed("could not find my edge")

    print "DAY OF MONTH"
    stmt = "select edge, origin, destin, stamp, mday(stamp) \
              from sales \
             where mday(stamp) = %d" % (es[idx].tp.day)
    found = False
    cnt2 = 0
    with c.execute(stmt) as cur:
         if not cur.ok():
            raise db.TestFailed("not ok: %d (%s)" % (cur.code(), cur.details()))
         for row in cur:
             if row.field(0) != 'buys':
                continue
             print "FOUND: %d - %d - %d - %d" % (row.field(1), \
                                                 row.field(2), \
                                                 row.field(3), \
                                                 row.field(4))
             cnt2 += 1
             if row.field(1) == es[idx].origin and \
                row.field(2) == es[idx].destin and \
                row.field(3) == now.dt2now(es[idx].tp):
                found=True
         if not found:
            raise db.TestFailed("could not find my edge")
         if cnt != cnt2:
            raise db.TestFailed("day of week(%d) != day of month(%d)" % (cnt, cnt2))

    print "HOUR OF DAY"
    stmt = "select edge, origin, destin, stamp, hour(stamp) \
              from sales \
             where hour(stamp) = %d" % (es[idx].tp.hour)
    found = False
    with c.execute(stmt) as cur:
         if not cur.ok():
            raise db.TestFailed("not ok: %d (%s)" % (cur.code(), cur.details()))
         for row in cur:
             if row.field(0) != 'buys':
                continue
             print "FOUND: %d - %d - %d - %d" % (row.field(1), \
                                                 row.field(2), \
                                                 row.field(3), \
                                                 row.field(4))
             if row.field(1) == es[idx].origin and \
                row.field(2) == es[idx].destin and \
                row.field(3) == now.dt2now(es[idx].tp):
                found=True
         if not found:
            raise db.TestFailed("could not find my edge")

    print "MINUTE OF HOUR"
    stmt = "select edge, origin, destin, stamp, minute(stamp) \
              from sales \
             where minute(stamp) = %d" % (es[idx].tp.minute)
    found = False
    with c.execute(stmt) as cur:
         if not cur.ok():
            raise db.TestFailed("not ok: %d (%s)" % (cur.code(), cur.details()))
         for row in cur:
             if row.field(0) != 'buys':
                continue
             
             print "FOUND: %d - %d - %d - %d" % (row.field(1), \
                                                 row.field(2), \
                                                 row.field(3), \
                                                 row.field(4))
             if row.field(1) == es[idx].origin and \
                row.field(2) == es[idx].destin and \
                row.field(3) == now.dt2now(es[idx].tp):
                found=True
         if not found:
            raise db.TestFailed("could not find my edge")

    print "SECOND OF MINUTE"
    stmt = "select edge, origin, destin, stamp, second(stamp) \
              from sales \
             where second(stamp) = %d" % (es[idx].tp.second)
    found = False
    with c.execute(stmt) as cur:
         if not cur.ok():
            raise db.TestFailed("not ok: %d (%s)" % (cur.code(), cur.details()))
         for row in cur:
             if row.field(0) != 'buys':
                continue

             print "FOUND: %d - %d - %d - %d" % (row.field(1), \
                                                 row.field(2), \
                                                 row.field(3), \
                                                 row.field(4))
             if row.field(1) == es[idx].origin and \
                row.field(2) == es[idx].destin and \
                row.field(3) == now.dt2now(es[idx].tp):
                found=True
         if not found:
            raise db.TestFailed("could not find my edge")

    print "MICROSECONDs"
    stmt = "select edge, origin, destin, stamp, micro(stamp) \
              from sales \
             where micro(stamp) = %d" % (es[idx].tp.microsecond)
    found = False
    with c.execute(stmt) as cur:
         if not cur.ok():
            raise db.TestFailed("not ok: %d (%s)" % (cur.code(), cur.details()))
         for row in cur:
             if row.field(0) != 'buys':
                continue
             
             print "FOUND: %d - %d - %d - %d" % (row.field(1), \
                                                 row.field(2), \
                                                 row.field(3), \
                                                 row.field(4))
             if row.field(1) == es[idx].origin and \
                row.field(2) == es[idx].destin and \
                row.field(3) == now.dt2now(es[idx].tp):
                found=True
         if not found:
            raise db.TestFailed("could not find my edge")

def shortcircuit(c):

    print "RUNNING TEST 'shortcircuit'"

    idx = random.randint(1,len(es)-1)
    l = countedge(es, 'buys')

    x = 0
    stmt = "select count(*) from sales where edge='buys' and origin=%d" % es[idx].origin
    with c.execute(stmt) as cur:
         if not cur.ok():
            raise db.TestFailed("not ok: %d (%s)" % (cur.code(), cur.details()))
         for row in cur:
             x = row.field(0)

    print "PIVOT: %d, %d " % (es[idx].origin, x)

    print "short circuit: false or key=x"
    stmt = "select edge, origin from sales \
             where 1=0 or origin = %d" % es[idx].origin
    with c.execute(stmt) as cur:
         if not cur.ok():
            raise db.TestFailed("not ok: %d (%s)" % (cur.code(), cur.details()))
         n=0
         for row in cur:
             if row.field(0) != 'buys':
                continue
             n+=1
             if row.field(1) != es[idx].origin:
                raise db.TestFailed("wrong edge: %d / %d" % (row.field(1), es[idx].origin))
         if n != x:
             raise db.TestFailed("wrong number of edges: %d" % n)

    print "short circuit: true or key=x"
    stmt = "select edge, origin from sales \
             where 1=1 or origin = %d" % es[idx].origin
    with c.execute(stmt) as cur:
         if not cur.ok():
            raise db.TestFailed("not ok: %d (%s)" % (cur.code(), cur.details()))
         n=0
         found=False
         for row in cur:
             if row.field(0) != 'buys':
                continue

             n+=1
             if row.field(1) == es[idx].origin:
                found=True
         if not found:
             raise db.TestFailed("could not find my edge")
         if n != l:
             raise db.TestFailed("wrong number of edges: %d" % n)

    print "short circuit: key=x or false"
    stmt = "select edge, origin from sales \
             where origin = %d or 1=0" % es[idx].origin
    with c.execute(stmt) as cur:
         if not cur.ok():
            raise db.TestFailed("not ok: %d (%s)" % (cur.code(), cur.details()))
         n=0
         for row in cur:
             if row.field(0) != 'buys':
                continue
             n+=1
             if row.field(1) != es[idx].origin:
                raise db.TestFailed("wrong edge: %d / %d" % (row.field(1), es[idx].origin))
         if n != x:
             raise db.TestFailed("wrong number of edges: %d" % n)

    print "short circuit: key=x or true"
    stmt = "select edge, origin from sales \
             where origin = %d or 1=1" % es[idx].origin
    with c.execute(stmt) as cur:
         if not cur.ok():
            raise db.TestFailed("not ok: %d (%s)" % (cur.code(), cur.details()))
         n=0
         found=False
         for row in cur:
             if row.field(0) != 'buys':
                continue
             n+=1
             if row.field(1) == es[idx].origin:
                found=True
         if not found:
             raise db.TestFailed("could not find my edge")
         if n != l:
             raise db.TestFailed("wrong number of edges: %d" % n)

    print "short circuit: false and key=x"
    stmt = "select edge, origin from sales \
             where 1=0 and origin = %d" % es[idx].origin
    with c.execute(stmt) as cur:
         if cur.ok():
            raise db.TestFailed("found false..." % (cur.code(), cur.details()))

    print "short circuit: true and key=x"
    stmt = "select edge, origin from sales \
             where 1=1 and origin = %d" % es[idx].origin
    with c.execute(stmt) as cur:
         if not cur.ok():
            raise db.TestFailed("not ok: %d (%s)" % (cur.code(), cur.details()))
         n=0
         for row in cur:
             if row.field(0) != 'buys':
                continue
             n+=1
             if row.field(1) != es[idx].origin:
                raise db.TestFailed("wrong edge: %d / %d" % (row.field(1), es[idx].origin))
         if n != x:
             raise db.TestFailed("wrong number of edges: %d" % n)

    print "short circuit: key=x and false"
    stmt = "select edge, origin from sales \
             where origin = %d and 1=0" % es[idx].origin
    with c.execute(stmt) as cur:
         if cur.ok():
            raise db.TestFailed("found false..." % (cur.code(), cur.details()))

    print "short circuit: key=x and true"
    stmt = "select edge, origin from sales \
             where origin = %d and 1=1" % es[idx].origin
    with c.execute(stmt) as cur:
         if not cur.ok():
            raise db.TestFailed("not ok: %d (%s)" % (cur.code(), cur.details()))
         n=0
         for row in cur:
             if row.field(0) != 'buys':
                continue
             n+=1
             if row.field(1) != es[idx].origin:
                raise db.TestFailed("wrong edge: %d / %d" % (row.field(1), es[idx].origin))
         if n != x:
             raise db.TestFailed("wrong number of edges: %d" % n)

def casefun(c):

    print "RUNNING TEST 'casefun'"

    # case appears in where
    cnt=0
    for e in es:
        if e.destin < 1050:
           cnt += 1

    stmt = "select destin from sales" \
           " where edge = 'buys' " \
           "   and case " \
           "          when destin-(1000) < 10 then 1.0/10.0" \
           "          when destin-(1000) < 20 then 2.0/10.0" \
           "          when destin-(1000) < 30 then 3.0/10.0" \
           "          when destin-(1000) < 40 then 4.0/10.0" \
           "          when destin-(1000) < 50 then 5.0/10.0" \
           "          else 1.0 " \
           "       end < 1.0"

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
    for e in es:
        if e.origin < 4000010 or \
           (e.origin >= 4000020 and e.origin < 4000030) or \
           (e.origin >= 4000040 and e.origin < 4000050):
           cnt += 1

    stmt = "select origin from sales " \
           " where case " \
           "          when origin < 4000010 then true " \
           "          when origin < 4000020 then false" \
           "          when origin < 4000030 then true " \
           "          when origin < 4000040 then false" \
           "          when origin < 4000050 then true " \
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
    for e in es:
        if e.origin >= 4000010 and e.origin < 4000030:
           cnt += 1

    stmt = "select origin from sales" \
           " where case " \
           "          when origin < 4000010 then false" \
           "          when origin < 4000020 then 'two'" \
           "          when origin < 4000030 then 3" \
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

def mkin(keys,n):
    x = 0
    stmt = "("

    for k in keys:
       stmt += str(k)
       x+=1
       if x < n:
          stmt += ","
       else:
          break

    stmt += ")"
    return stmt

def infun(c):

    print "RUNNING TEST 'infun'"

    keys = []

    with c.execute("select prod_key from product where prod_cat is not null") as cur:
         if not cur.ok():
            raise db.TestFailed("not ok: %d (%s)" % (cur.code(), cur.details()))
         for row in cur:
             keys.append(row.field(0))

    stmt = "select count(*) from sales where edge = 'buys' and destin in %s"
    for i in range(1, 100, 5):
        print "%d out of %d" % (i, len(keys))
        # print "select count(*) from sales where edge = 'buys' and destin in %s" % mkin(keys,i)
        with c.execute(stmt % mkin(keys, i)) as cur:
             for row in cur:
                 if row.field(0) != countalledges(keys, i):
                    raise db.TestFailed("count does not match for %d: %d vs. %d" % \
                                        (i, row.field(0), countalledges(keys, i)))

if __name__ == "__main__":
    with now.Connection("localhost", "55505", None, None) as c:
        (ps, cs, ss, es) = db.loadDB(c, "db100")

        for i in range(0,IT):
            roundfloats(c)
            weekdays(c)
            shortcircuit(c)
            casefun(c)
            infun(c)

        print "PASSED"

