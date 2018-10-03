#!/usr/bin/env python

from now import *
import threading
import sys


def execfib():
    try:
      with Connection("127.0.0.1", "55505", None, None) as con:
        with con.execute("use retail") as r:
          if not r.ok():
             print "ERROR: %d on use: %s" % (r.code(), r.details())
             return

        with con.execute("exec fib(5)") as r:
          if not r.ok():
            print "ERROR %d on fib: %s" % (r.code(), r.details())
            return

    except ClientError as x:
      print "[%s] cannot connect: %s" % (self.name, x)

# ------------------------------------------------------------------------
# MAIN
# ------------------------------------------------------------------------

print "%s called with %d arguments" % (sys.argv[0], len(sys.argv)-1)

n = len(sys.argv)

IT = 1

try:
  if n > 1:
    IT = abs(int(sys.argv[1]))

except:
  print "%s is not an integer" % sys.argv[1]
  exit(1)

  
print "iterations: %d"  % IT

for j in range(IT):

  if IT > 1:
    print "iteration %d of %d" % (j+1,IT)

  execfib()

