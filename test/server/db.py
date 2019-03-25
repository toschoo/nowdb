import nowdb
from datetime import *

def sayhello():
    print("hello")
    return nowdb.success().toDB()

def mycount(tab):
    sql = "select count(*) from %s" % tab
    print(sql)
    with nowdb.execute(sql) as cur:
         res = nowdb.makeRow()
         try:
             for row in cur:
                 res.add2Row(nowdb.UINT, cur.field(0))
                 res.closeRow()
             return res.toDB()
         except Exception as x:
             res.release()
             return nowdb.makeError(nowdb.USRERR, str(x)).toDB()

def getunique(t):
    sql = "select max(id) from unique"
    with nowdb.execute(sql) as cur:
        for row in cur:
           x = row.field(0) + 1
           with nowdb.execute("insert into unique (id, desc) \
                                           values (%d, '%s')"  % (x, t)) as r:
                if not r.ok():
                   raise DBError(r.code(), r.details())
           return x

def getnow():
    with nowdb.execute("select now() from unique") as cur:
        for row in cur:
            return row.field(0)

def groupbuy(p):
    sql = "select coal(sum(quantity),0), coal(avg(quantity),0), coal(max(quantity),0), \
                  coal(sum(price),0),    coal(avg(price),0),    coal(max(price),0) \
             from buys where destin = %d" % p
    try:
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
        return nowdb.execute(sql).toDB()
    except Exception as x:
        return nowdb.makeError(nowdb.USRERR,str(x)).toDB()
