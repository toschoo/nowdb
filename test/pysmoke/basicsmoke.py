#!/usr/bin/env python

import now
import db

if __name__ == '__main__':
    with now.Connection("localhost", "55505", None, None) as c:
        db.createDB(c, "db100")

        products = db.createProducts(100)
        clients = db.createClients(50)
        
        edges = db.createEdges(1000, clients, products)

        # db.storeProducts(c, products)
        # db.storeClients(c, clients)

        db.products2csv("rsc/products.csv", products)
        db.clients2csv("rsc/clients.csv", clients)

        with c.execute("load 'rsc/products.csv' into vertex use header as product") as r:
            if not r.ok():
               print "cannot load products: %s" % r.details()
               exit(1)

        with c.execute("load 'rsc/clients.csv' into vertex use header as client") as r:
            if not r.ok():
               print "cannot load clients: %s" % r.details()
               exit(1)

        db.storeEdges(c, edges)

        with c.execute("select origin, destin, timestamp, weight, weight2 \
                          from sales") as cur:
            for r in cur:
               o = r.field(0)
               d = r.field(1)
               t = now.now2dt(r.field(2)).strftime(now.TIMEFORMAT)
               w = r.field(3)
               w2 = r.field(4)

               print "%d/%d @%s: %d / %.4f" % (o, d, t, w, w2)
