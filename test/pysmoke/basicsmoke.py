#!/usr/bin/env python

import now
import db

if __name__ == '__main__':
    with now.Connection("localhost", "55505", None, None) as c:

	nump = 100
	numc = 50
	nums = 25
	nume = 1000

        db.createDB(c, "db100")

        (products, clients, stores, edges) = db.addRandomData(c, nump, numc, nums, nume)

        cnt = 0
        cnt2 = 0
        sm = 0
        sm2 = 0
        avg = 0.0
        avg2 = 0.0
        for e in edges:
           sm += e.quantity
           cnt += 1

        avg = float(sm)/float(cnt)

        """
        with c.execute("select count(*), sum(quantity), avg(quantity) \
                          from buys") as cur:
        """
        with c.execute("select 1, origin, destin \
                          from buys") as cur:
            if not cur.ok():
              print "ERROR %d: %s" % (cur.code(), cur.details())
              exit(1)
            for r in cur:
               if r.count() != 3:
                  print "ERROR: wrong number of fields: %d != %ds" % (r.count(), 3)
                  exit(1)

               print "%d %d" % (r.field(1), r.field(2))

               cnt2 += r.field(0)
               #sm2 += r.field(1)
               #avg2 = r.field(2)

        if cnt != cnt2:
           print "count differs: %d -- %s" % (cnt, cnt2)
           exit(1)

        if sm  != sm2:
           print "sum differs: %d -- %d" % (sm, sm2)
           exit(1)

        if round(avg,4) != round(avg2,4):
           print "avg differs: %.4f -- %.4f" % (avg, avg2)
           exit(1)

        with c.execute("select count(*) from buys") as cur:
           if not cur.ok():
              print "ERROR: %s" % cur.details()
              exit(1)
           for r in cur:
              cnt = r.field(0)
           print "we have %d edges" % cnt
           if cnt != 2*nume:
              print "we need to have %d" % nume
              exit(1)
	
        with c.execute("select prod_key, prod_desc from product") as cur:
           if not cur.ok():
              print "ERROR %d: %s" % (cur.code(), cur.details())
              exit(1)
           cnt = 0
           for row in cur:
              cnt += 1

           print "we have %d products" % cnt
           if cnt != nump:
              print "we need to have %d" % nump
              exit(1)

        with c.execute("select client_key, client_name from client") as cur:
           if not cur.ok():
              print "ERROR %d: %s" % (cur.code(), cur.details())
              exit(1)
           cnt = 0
           for row in cur:
              # print "%s" % row.field(0)
              cnt += 1
           print "we have %d clients " % cnt
           if cnt != numc:
              print "we need to have %d" % numc
              exit(1)

        with c.execute("select store_name, city, address, size from store") as cur:
           if not cur.ok():
              print "ERROR %d: %s" % (cur.code(), cur.details())
              exit(1)
           cnt = 0
           for row in cur:
              # print "%s" % row.field(0)
              cnt += 1
           print "we have %d stores " % cnt
           if cnt != nums:
              print "we need to have %d" % nums
              exit(1)

        (p2,c2,s2,e2) = db.loadDB(c,"db100")
        if len(p2) != len(products):
            print "loaded products differ from original: %d != %d" % (len(p2), len(products))
        if len(c2) != len(clients):
            print "loaded clients differ from original: %d != %d" % (len(c2), len(clients))
        if len(s2) != len(stores):
            print "loaded stores differ from original: %d != %d" % (len(s2), len(stores))
        if len(e2) != len(edges):
            print "loaded edges differ from original: %d != %d" % (len(e2), len(edges))
        print "LOADED: %d %d %d %d" % (len(p2), len(c2), len(s2), len(e2))
