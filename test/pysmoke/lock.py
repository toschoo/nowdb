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
    if rnd.randint(1,3) == 1:
       mode = "reading"
    else:
       mode = "writing"

    x = rnd.randint(0,3) * 1000

    stmt = "lock testlock1 for %s" % mode

    if x > 0:
       stmt += " set timeout = %d " % x

    print(stmt)
    r = c.execute(stmt)
    if not r.ok():
       if r.code() == 36: # timeout
          print("TIMEOUT (%s | %d)" % (mode, x))
          return
       raise db.TestFailed("error on lock: %d (%s)" % (r.code(), r.details()))

    print("LOCKED")
    time.sleep(1)

    r = c.execute("unlock testlock1")
    if not r.ok():
       raise db.TestFailed("error on unlock: %d (%s)" % (r.code(), r.details()))

if __name__ == "__main__":

    print("running locktest")

    with now.connect("localhost", "55505", None, None) as c:
      r = c.execute("use db100")
      if not r.ok():
         raise db.TestFailed("cannot use: %d (%s)", (r.code(), r.details()))

      createlocks(c)

      for i in range(10):
          locktest(c)

    print("PASSED")
