#!/usr/bin/env python

import db
import now
import random as rnd
import math
import sys
import time

def createlocks(c):
    c.execute("create lock testlock1 if not exists").release()

def locktest(c):
    print("LOCKTEST")

    if rnd.randint(1,3) == 1:
       mode = "reading"
    else:
       mode = "writing"

    x = rnd.randint(0,3)

    stmt = "lock testlock1 for %s" % mode

    if x > 0:
       if x == 1:
         stmt += " set timeout = 0"
       else:
         stmt += " set timeout = %d " % (x * 1000)

    print(stmt)
    with c.execute(stmt) as r:
       if not r.ok():
          if r.code() == now.TIMEOUT:
             print("TIMEOUT (%s | %d)" % (mode, x))
             return
          raise db.TestFailed("error on lock: %d (%s)" % (r.code(), r.details()))

    print("LOCKED")
    time.sleep(1)

    c.rexecute_("unlock testlock1")

def selflock(c):
    print("SELFLOCK")

    c.rexecute_("lock testlock1")
    try:
      r = c.rexecute("lock testlock1")
      r.release()
    except now.DBError as x:
      print("DBError: %s" % str(x))

    c.rexecute_("unlock testlock1")

if __name__ == "__main__":

    print("running locktest")

    rnd.seed()

    with now.connect("localhost", "55505", None, None) as c:
      r = c.execute("use db100")
      if not r.ok():
         raise db.TestFailed("cannot use: %d (%s)", (r.code(), r.details()))

      createlocks(c)

      for i in range(25):
          locktest(c)
          selflock(c)

    print("PASSED")
