#!/usr/bin/python

from now import *
from random import *
from datetime import *
from calendar import *
from dateutil.tz import *
import sys

utc = tzutc()
lc = tzlocal()
tzn = tzoffset("MYZONE", 5*3600)

seed()

if len(sys.argv) > 1:
  mx = int(sys.argv[1])
else:
  mx = 10

for i in range(mx):

  x = randint(0,5)

  if x == 0:
    n = datetime.now(utc)
  if x < 2:
    k = randint(1,12)
    n = datetime.now(tzoffset("MYZONE", k*3600))
  # if x < 3:
  #   n = datetime.now()
  else:
    n = datetime.now(lc)

  # print "now: %s" % n.strftime("%Y-%m-%dT%H:%M.%S")

  x = randint(0,6)

  f = 10**x

  m = randint(1,1000000)
  m *= f

  x = randint(0,1)
  if x == 0:
    dt = n + timedelta(microseconds=m)
  else:
    dt = n - timedelta(microseconds=m)

  nw = dt2now(dt)
  dt2 = now2dt(nw)

  if dt != dt2:
    print "Test %06d) FAILED. timestamps differ: %s != %s" % (dt.strftime("%Y-%m-%dT%H:%M:%S.%f"), dt2.strftime("%Y-%m-%dT%H:%M:%S.%f"))
    exit(1)

  print "Test %06d) %s: OK" % ((i+1), dt.strftime("%Y-%m-%dT%H:%M:%S.%f"))

  
    

  
  
