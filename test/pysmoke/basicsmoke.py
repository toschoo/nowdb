#!/usr/bin/env python

import now
import db

if __name__ == '__main__':
    with now.Connection("localhost", "55505", None, None) as c:

        db.createDB(c, "db100")

        (products, clients, edges) = db.addRandomData(c, 100, 50, 1000)

        with c.execute("select origin, destin, timestamp, weight, weight2 \
                          from sales") as cur:
            if not cur.ok():
              print "ERROR: %s" % cur.details()
              exit(1)
            for r in cur:
               o = r.field(0)
               d = r.field(1)
               t = now.now2dt(r.field(2)).strftime(now.TIMEFORMAT)
               w = r.field(3)
               w2 = r.field(4)

               print "%d/%d @%s: %d / %.4f" % (o, d, t, w, w2)

        with c.execute("select count(*) from sales") as cur:
           if not cur.ok():
              print "ERROR: %s" % cur.details()
              exit(1)
           for r in cur:
              cnt = r.field(0)
              print "we have %d edges" % cnt
              if cnt != 1000:
                 print "we need to have 1000"
                 exit(1)
	
        with c.execute("select client_key from vertex as client") as cur:
           if not cur.ok():
              print "ERROR: %s" % cur.details()
              exit(1)
           cnt = 0
           for row in cur:
              print "%s" % row.field(0)
              cnt += 1
           print "we have %d clients " % cnt
           if cnt != 50:
              print "we need to have 100"
              exit(1)
     
