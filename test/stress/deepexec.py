#!/usr/bin/env python
from now import *
import threading

class QThread(threading.Thread):
  def __init__(self):
    threading.Thread.__init__(self)

  def run(self):
    try:
      with Connection("127.0.0.1", "55505", None, None) as con:
        with con.execute("use retail") as r:
          if not r.ok():
             print "ERROR: %d on use: %s" % (r.code(), r.details())
             return

        for i in range(10):
          with con.execute("exec fib(5)") as r:
            if not r.ok():
              print "ERROR %d on fib: %s" % (r.code(), r.details())
              return

          if i%5 == 0:
            with con.execute("exec fibreset()") as r:
              if not r.ok():
                print "ERROR %d on fibreset: %s" % (r.code(), r.details())
                return

    except ClientError as x:
      print "[%s] cannot connect: %s" % (self.name, x)

THREADS = 100

thds = []

for i in range(THREADS):
    thds.append(QThread())

for t in thds:
    t.start()

# time.sleep(1)

for t in thds:
    t.join()
  
