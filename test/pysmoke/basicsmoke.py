#!/usr/bin/env python

import now
import db

if __name__ == '__main__':
    with now.Connection("localhost", "55505", None, None) as c:

	nump = 100
	numc = 50
	nume = 1000

        db.createDB(c, "db100")

        (products, clients, edges) = db.addRandomData(c, nump, numc, nume)

        cnt = 0
        sm = 0
        avg = 0.0
        for e in edges:
           sm += e.weight
           cnt += 1

        avg = float(sm)/float(cnt)

        with c.execute("select count(*), sum(weight), avg(weight) \
                          from sales") as cur:
            if not cur.ok():
              print "ERROR: %s" % cur.details()
              exit(1)
            for r in cur:
               cnt2 = r.field(0)
               sm2 = r.field(1)
               avg2 = r.field(2)

        if cnt != cnt2:
           print "count differs: %d -- %d" % (cnt, cnt2)
           exit(1)

        if sm  != sm2:
           print "sum differs: %d -- %d" % (sm, sm2)
           exit(1)

        if round(avg,4) != round(avg2,4):
           print "avg differs: %.4f -- %.4f" % (avg, avg2)
           exit(1)

        """ nice demonstration
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
        """

        with c.execute("select count(*) from sales") as cur:
           if not cur.ok():
              print "ERROR: %s" % cur.details()
              exit(1)
           for r in cur:
              cnt = r.field(0)
              print "we have %d edges" % cnt
              if cnt != nume:
                 print "we need to have %d" % nume
                 exit(1)
	
        with c.execute("select prod_key, prod_desc from vertex as product") as cur:
           if not cur.ok():
              print "ERROR: %s" % cur.details()
              exit(1)
           cnt = 0
           for row in cur:
              cnt += 1

           print "we have %d products" % cnt
           if cnt != nump:
              print "we need to have %d" % nump
              exit(1)

        with c.execute("select client_key, client_name from vertex as client") as cur:
           if not cur.ok():
              print "ERROR: %s" % cur.details()
              exit(1)
           cnt = 0
           for row in cur:
              # print "%s" % row.field(0)
              cnt += 1
           print "we have %d clients " % cnt
           if cnt != numc:
              print "we need to have %d" % numc
              exit(1)

        (p2,c2,e2) = db.loadDB(c,"db100")
        if len(p2) != len(products):
            print "loaded products differ from original: %d != %d" % (len(p2), len(products))
        if len(c2) != len(clients):
            print "loaded clients differ from original: %d != %d" % (len(c2), len(clients))
        if len(e2) != len(edges):
            print "loaded edges differ from original: %d != %d" % (len(e2), len(edges))
        print "LOADED: %d %d %d" % (len(p2), len(c2), len(e2))
