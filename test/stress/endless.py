#!/usr/bin/env python
from now import *

# while True
for i in range(100):
   try:
      with Connection("127.0.0.1", "55505", None, None) as con:
        with con.execute("use retail") as r:
          if not r.ok():
             print "ERROR: %d on use: %s" % (r.code(), r.details())
             exit(1)

        for i in range(100):
          with con.execute("exec fib(5)") as r:
            if not r.ok():
               print "ERROR %d on fib: %s" % (r.code(), r.details())
               exit(1)

          if i%5 == 0:
            with con.execute("exec fibreset()") as r:
              if not r.ok():
                 print "ERROR %d on fibreset: %s" % (r.code(), r.details())
                 exit(1)

   except ClientError as x:
      print "[%s] cannot connect: %s" % (self.name, x)
