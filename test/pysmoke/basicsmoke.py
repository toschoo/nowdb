#!/usr/bin/env python

import now
import random
import db

if __name__ == '__main__':

    random.seed()

    with now.Connection("localhost", "55505", None, None) as c:

        nump = 1000
        numc = 500
        nums = 25
        nume = 30000
        numv = 100

        db.createDB(c, "db100")

        (products, clients, stores, buys, visits) = db.addRandomData(c, nump, numc, nums, nume, numv)

        cnt = 0
        cnt2 = 0
        sm = 0
        sm2 = 0
        avg = 0.0
        avg2 = 0.0
        for b in buys:
           sm += b.quantity
           cnt += 1

        avg = float(sm)/float(cnt)

        with c.execute("select count(*), sum(quantity), avg(quantity) \
                          from buys") as cur:
            if not cur.ok():
              print("ERROR %d: %s" % (cur.code(), cur.details()))
              exit(1)
            for r in cur:
               if r.count() != 3:
                  print("ERROR: wrong number of fields: %d != %ds" % (r.count(), 3))
                  exit(1)

               # print "%d|%d|%.2f" % (r.field(0), r.field(1), r.field(2))

               cnt2 += r.field(0)
               sm2 += r.field(1)
               avg2 = r.field(2)

        if cnt != cnt2:
           print("count differs: %d -- %s" % (cnt, cnt2))
           exit(1)

        if sm  != sm2:
           print("sum differs: %d -- %d" % (sm, sm2))
           exit(1)

        if round(avg,4) != round(avg2,4):
           print("avg differs: %.4f -- %.4f" % (avg, avg2))
           exit(1)

        with c.execute("select count(*) from buys") as cur:
           if not cur.ok():
              print("ERROR: %s" % cur.details())
              exit(1)
           for r in cur:
              cnt = r.field(0)
           print("we have %d buy-edges" % cnt)
           if cnt != nume:
              print("we need to have %d" % nume)
              exit(1)

        cnt = 0
        for row in c.execute("select prod_key, prod_desc from product"):
            cnt += 1

        print("we have %d products" % cnt)
        if cnt != nump:
           print("we need to have %d" % nump)
           exit(1)

        cnt = 0
        for row in c.execute("select client_key, client_name from client"):
            cnt += 1

        print("we have %d clients " % cnt)
        if cnt != numc:
           print("we need to have %d" % numc)
           exit(1)

        with c.execute("select store_name, city, address, size from store") as cur:
           if not cur.ok():
              print("ERROR %d: %s" % (cur.code(), cur.details()))
              exit(1)
           cnt = 0
           for row in cur:
              # print "%s" % row.field(0)
              cnt += 1
           print("we have %d stores " % cnt)
           if cnt != nums:
              print("we need to have %d" % nums)
              exit(1)

        (p2,c2,s2,b2,v2) = db.loadDB(c,"db100")
        if len(p2) != len(products):
            print("loaded products differ from original: %d != %d" % (len(p2), len(products)))
        if len(c2) != len(clients):
            print("loaded clients differ from original: %d != %d" % (len(c2), len(clients)))
        if len(s2) != len(stores):
            print("loaded stores differ from original: %d != %d" % (len(s2), len(stores)))
        if len(b2) != len(buys):
            print("loaded buys differ from original: %d != %d" % (len(b2), len(buys)))
        if len(v2) != len(visits):
            print("loaded vists differ from original: %d != %d" % (len(v2), len(vists)))
        print("LOADED: %d %d %d %d %d" % (len(p2), len(c2), len(s2), len(b2), len(v2)))
