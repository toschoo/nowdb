import nowdb
from datetime import *

def sayhello():
    print("hello")
    return nowdb.success()

def timetest(t):
    res = nowdb.makeRow()
    with nowdb.execute("select count(*) \
                          from buys \
                         where stamp <= %d" % t) as cur:
        for row in cur:
            res.add2Row(nowdb.UINT, row.field(0))
            res.closeRow()
        return res
    return nowdb.success()

def mycount(tab):
    sql = "select count(*) from %s" % tab
    print(sql)
    with nowdb.execute(sql) as cur:
         res = nowdb.makeRow()
         for row in cur:
             res.add2Row(nowdb.UINT, cur.field(0))
             res.closeRow()
         return res

def myadd(t,a,b):
    res = nowdb.makeRow()
    res.add2Row(t, a + b)
    res.closeRow()
    return res

def myfloatadd(a,b):
    print("myadd: %f + %f" % (a,b))
    return myadd(nowdb.FLOAT, a, b)

def myuintadd(a,b):
    print("myadd: %d + %d" % (a,b))
    return myadd(nowdb.UINT, a, b)

def myintadd(a,b):
    print("myadd: %d + %d" % (a,b))
    return myadd(nowdb.INT, a, b)

def mylogic(o,a,b):
    if o == "and":
       c = a and b
    elif o == "or":
       c = a or b
    elif o == "xor":
       c = (a and not b) or (not a and b)
    else:
       c = True
       
    res = nowdb.makeRow()
    res.add2Row(nowdb.BOOL, c)
    res.closeRow()
    return res

def getEasterDate(y):
    a = y % 19
    b = y % 4
    c = y % 7
    k = y / 100
    p = (8*k + 13) / 25
    q = k / 4
    m = (15 + k - p - q) % 30
    n = (4 + k - q) % 7
    d = (19 * a + m) % 30
    e = (2*b + 4*c + 6*d + n) % 7
    return (22 + d + e)

def easter(y):
    m = 3
    d = getEasterDate(y)
    if d > 31:
       d-=31
       m+=1
    t = nowdb.dt2now(datetime(y,m,d))
    res = nowdb.makeRow()
    res.add2Row(nowdb.TIME,t)
    res.closeRow()
    return res

def getunique(t):
    sql = "select max(id) from unique"
    with nowdb.execute(sql) as cur:
        for row in cur:
           x = row.field(0) + 1
           sql = "insert into unique (id, desc) \
                              values (%d, '%s')" % (x, t)
           with nowdb.execute(sql) as r:
                if not r.ok():
                   raise DBError(r.code(), r.details())
           return x

def getnow():
    with nowdb.execute("select now()") as row:
         return row.field(0)

def groupbuy(p):
    sql = "select coal(sum(quantity),0), coal(avg(quantity),0), coal(max(quantity),0), \
                  coal(sum(price),0),    coal(avg(price),0),    coal(max(price),0) \
             from buys where destin = %d" % p

    uid = getunique('groupbuy')
    dt  = getnow()

    with nowdb.execute(sql) as cur:
         if not cur.ok():
            raise nowdb.makeError(cur.code(), cur.details())
         for row in cur:
             f1 = row.field(0)
             f2 = row.field(1)
             f3 = row.field(2)
             f4 = row.field(3)
             f5 = row.field(4)
             f6 = row.field(5)
             ins = "insert into result (origin, destin, stamp, \
                                        f1, f2, f3, f4, f5, f6) \
                                values (%d, %d, %d, \
                                        %f, %f, %f, %f, %f, %f)" % \
                                       (uid, p, dt, \
                                        f1, f2, f3, f4, f5, f6)
             with nowdb.execute(ins) as r:
                  if not r.ok():
                     raise nowdb.DBError(r.code(), r.details())
   
    sql = "select destin, f1, f2, f3, f4, f5, f6 \
             from result where origin = %d" % uid
    return nowdb.execute(sql)
