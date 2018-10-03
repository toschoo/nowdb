#!/usr/bin/env python

from now import *
import threading
import sys

class QThread(threading.Thread):
  def __init__(self, loops):
    threading.Thread.__init__(self)
    self.loops = loops

  def run(self):
    try:
      with Connection("127.0.0.1", "55505", None, None) as con:
        with con.execute("use retail") as r:
          if not r.ok():
             print "ERROR: %d on use: %s" % (r.code(), r.details())
             return

        for i in range(self.loops):
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

    except Exception as x:
      print "[%s] unexpected exception: %s" % (self.name, x)

# ------------------------------------------------------------------------
# MAIN
# ------------------------------------------------------------------------

print "%s called with %d arguments" % (sys.argv[0], len(sys.argv)-1)

n = len(sys.argv)

IT = 1
THREADS = 10
LOOP = 10

ok = 0

try:
  if n > 1:
    IT = abs(int(sys.argv[1]))
    ok += 1

  if n > 2:
    THREADS = abs(int(sys.argv[2]))
    ok += 1

  if n > 3:
    LOOP = abs(int(sys.argv[3]))
    ok += 1

except:
  if ok == 0:
    x = sys.argv[1]
  if ok == 1:
    x = sys.argv[2]
  if ok == 2:
    x = sys.argv[3]

  print "%s is not an integer" % x
  exit(1)

  
print "iterations: %d, threads: %d, loops per thread: %d"  % \
      (IT, THREADS, LOOP)

for j in range(IT):

  if IT > 1:
    print "iteration %d of %d" % (j+1,IT)

  thds = []

  for i in range(THREADS):
      thds.append(QThread(LOOP))

  for t in thds:
      t.start()

  for t in thds:
      t.join()
