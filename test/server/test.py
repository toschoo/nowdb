import nowdb
from datetime import *

def sayhello():
    print("hello")

def mycount(tab):
    sql = "select count(*) from %s" % tab
    print(sql)
    with nowdb.execute(sql) as cur:
         res = nowdb.makeRow()
         try:
             for row in cur:
                 res.add2Row(nowdb.UINT, cur.field(0))
                 res.closeRew()
             return res.toDB()
         except Exception as x:
             res.release()
             return nowdb.makeError(nowdb.USRERR, str(x))
             
