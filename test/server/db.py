import nowdb as nd
from datetime import *

def sayhello():
    print("hello")
    return nd.success()

def timetest(t):
    res = nd.makeRow()
    with nd.execute("select count(*) \
                          from buys \
                         where stamp <= %d" % t) as cur:
        for row in cur:
            res.add2Row(nd.UINT, row.field(0))
            res.closeRow()
        return res
    return nd.success()

def mycount(tab):
    sql = "select count(*) from %s" % tab
    print(sql)
    cur = nd.execute(sql)
    res = nd.makeRow()
    for row in cur:
        res.add2Row(nd.UINT, cur.field(0))
        res.closeRow()
    return res

def myadd(t,a,b):
    res = nd.makeRow()
    res.add2Row(t, a + b)
    res.closeRow()
    return res

def myfloatadd(a,b):
    print("myadd: %f + %f" % (a,b))
    return myadd(nd.FLOAT, a, b)

def myuintadd(a,b):
    print("myadd: %d + %d" % (a,b))
    return myadd(nd.UINT, a, b)

def myintadd(a,b):
    print("myadd: %d + %d" % (a,b))
    return myadd(nd.INT, a, b)

def mylogic(o,a,b):
    if o == "and":
       c = a and b
    elif o == "or":
       c = a or b
    elif o == "xor":
       c = (a and not b) or (not a and b)
    else:
       c = True
       
    res = nd.makeRow()
    res.add2Row(nd.BOOL, c)
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
    t = nd.dt2now(datetime(y,m,d))
    res = nd.makeRow()
    res.add2Row(nd.TIME,t)
    res.closeRow()
    return res

def getunique(t):
    sql = "select max(id) from unique"
    with nd.execute(sql) as cur:
        for row in cur:
           x = row.field(0) + 1
           sql = "insert into unique (id, desc) \
                              values (%d, '%s')" % (x, t)
           with nd.execute(sql) as r:
                if not r.ok():
                   raise DBError(r.code(), r.details())
           return x

def getnow():
    with nd.execute("select now()") as row:
         return row.field(0)

def groupbuy(p):
    sql = "select coal(sum(quantity),0), coal(avg(quantity),0), coal(max(quantity),0), \
                  coal(sum(price),0),    coal(avg(price),0),    coal(max(price),0) \
             from buys where destin = %d" % p

    uid = getunique('groupbuy')
    dt  = getnow()

    with nd.execute(sql) as cur:
         if not cur.ok():
            raise nd.makeError(cur.code(), cur.details())
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
             with nd.execute(ins) as r:
                  if not r.ok():
                     raise nd.DBError(r.code(), r.details())
   
    sql = "select destin, f1, f2, f3, f4, f5, f6 \
             from result where origin = %d" % uid
    return nd.execute(sql)

_fibn1 = 0
_fibn2 = 1

def fibreset():
    global _fibn1
    global _fibn2
    _fibn1 = 0
    _fibn2 = 1

def fib():
    global _fibn1
    global _fibn2
    f = _fibn1 + _fibn2
    _fibn1 = _fibn2
    _fibn2 = f
    return nd.makeReturnValue(nd.UINT, _fibn1)

def emptytable():
  nd.execute("create type myemptytype( \
                   id uint primary key) \
                   if not exists")
  with nd.execute("select * from myemptytype") as cur:
      for row in cur:
          print("this row does not exist")

