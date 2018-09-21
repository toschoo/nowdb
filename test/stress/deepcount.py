#!/usr/bin/env python

from now import *
import threading
import time
import signal

class QThread(threading.Thread):
  def __init__(self):
    threading.Thread.__init__(self)

  def run(self):
    with Connection("127.0.0.1", "55505", None, None) as con:
      with con.execute("use retail") as r:
        if not r.ok():
           print "ERROR: %d on use: %s" % (r.code(), r.details())
           return

      for i in range(10):
        # print "[%s] range %d" % (self.name, i)
        with con.execute("select count(*) from tx") as r:
          if not r.ok():
             print "ERROR %d on fib: %s" % (r.code(), r.details())
             return

THREADS = 100

thds = []

for i in range(THREADS):
    thds.append(QThread())

for t in thds:
    t.start()

m = THREADS
while m > 0:
  time.sleep(1)
  print "having %d" % m
  for t in thds:
    if t.isAlive():
      # t.join(0.01)
      pass
    else:
      m -= 1
