#!/usr/bin/env python

# to be run after basicsmoke

import now
import db
import datetime
import random

# it must be possible to get the key of a vertex
# from any of its attributes
def vertexSelectNoPK(c):

    print "RUNNING TEST 'vertexSelectNoPK'"

    stmt = "select prod_desc, prod_price from vertex as product \
             where prod_key = %s" % str(ps[0].key)
    with c.execute(stmt) as cur:
        if not cur.ok():
            print "%d: %s" % (cur.code(), cur.details())
            raise db.TestFailed("cannot execute select on product with pk")
        for row in cur:
            print "%s: %.2f" % (row.field(0), row.field(1))

    stmt = "select prod_key from vertex as product \
             where prod_desc = '%s'" % ps[0].desc
    with c.execute(stmt) as cur:
        if not cur.ok():
            print "%d: %s" % (cur.code(), cur.details())
            raise db.TestFailed("cannot execute select on product without pk")
        for row in cur:
            print "%d" % row.field(0)
            if row.field(0) != ps[0].key:
                raise db.TestFailed("wroing key selected for product: %d != %d" %
                                   (row.field(), ps[0].key))

    stmt = "select client_key from vertex as client \
             where client_name = '%s'" % cs[0].name
    with c.execute(stmt) as cur:
        if not cur.ok():
            print "%d: %s" % (cur.code(), cur.details())
            raise db.TestFailed("cannot execute select on client without pk")
        for row in cur:
            print "%d" % row.field(0)
            if row.field(0) != cs[0].key:
                raise db.TestFailed("wroing key selected for client: %d != %d" %
                                   (row.field(), cs[0].key))

    stmt = "select prod_desc from vertex as product"
    with c.execute(stmt) as cur:
        n=0
        if not cur.ok():
            print "%d: %s" % (cur.code(), cur.details())
            raise db.TestFailed("cannot execute select on product")
        for row in cur:
            n+=1

        if n != len(ps):
           raise db.TestFailed("expecting %d products, but have %d" % (len(ps), n))

    stmt = "select client_name from vertex as client"
    with c.execute(stmt) as cur:
        n=0
        if not cur.ok():
            print "%d: %s" % (cur.code(), cur.details())
            raise db.TestFailed("cannot execute select on client")
        for row in cur:
            n+=1

        if n != len(cs):
           raise db.TestFailed("expecting %d clients, but have %d" % (len(cs), n))

# it shall be possible to use 0 as key value
def keyzero(c):

    print "RUNNING TEST 'keyzero'"

    stmt = "create type testzero ( \
               test_key uint primary key, \
               test_desc text \
            )"
    with c.execute(stmt) as r:
        if not r.ok():
           print "%d: %s" % (r.code(), r.details())
           raise db.TestFailed("cannot create zero type")

    stmt = "insert into testzero(test_key, test_desc) (0, 'zero')"
    with c.execute(stmt) as r:
        if not r.ok():
            print "%d: %s" % (r.code(), r.details())
            raise db.TestFailed("cannot insert zero")

    stmt = "select test_key, test_desc \
              from vertex as testzero \
             where test_key = 0"
    with c.execute(stmt) as cur:
        if not cur.ok():
           print "%d: %s" % (cur.code(), cur.details())
           raise db.TestFailed("cannot select zero")
        for row in cur:
           print "%d: %s" % (row.field(0), row.field(1))
           if row.field(0) != 0 or row.field(1) != "zero":
              raise db.TestFailed("zero values differ!")

    stmt = "drop type testzero"
    with c.execute(stmt) as r:
        if not r.ok():
           print "%d: %s" % (r.code(), r.details())
           raise db.TestFailed("cannot drop zero")

# create edge must contain origin and destin
# origin and edgin must be vertex types
def createInvalidEdge(c):

    print "RUNNING TEST 'createInvalidEdge'"

    with c.execute("create edge testedge (origin client, weight float)") as r:
         if r.ok():
            raise db.TestFailed("I can create an edge without destin :-(")
         else:
            print "%d: %s" % (r.code(), r.details())

    with c.execute("create edge testedge (destin product, weight float)") as r:
         if r.ok():
            raise db.TestFailed("I can create an edge without origin :-(")
         else:
            print "%d: %s" % (r.code(), r.details())

    with c.execute("create edge testedge (origin uint, destin product, weight float)") as r:
         if r.ok():
            raise db.TestFailed("I can create an edge with origin that is not a vertex :-(")
         else:
            print "%d: %s" % (r.code(), r.details())

    with c.execute("create edge testedge (origin client, destin uint, weight float)") as r:
         if r.ok():
            raise db.TestFailed("I can create an edge with destin that is not a vertex :-(")
         else:
            print "%d: %s" % (r.code(), r.details())

# insert into edge must contain edge and timestamp
def invalidEdgeInserts(c):

    print "RUNNING TEST 'invalidEdgeInserts'"

    with c.execute("insert into sales (origin, destin, timestamp, weight) \
                                      (9999999, 9999, '2019-01-01', 0.99)") as r:
         if r.ok():
            raise db.TestFailed("I can insert an edge without edge :-(")
         else:
            print "%d: %s" % (r.code(), r.details())

    with c.execute("insert into sales (edge, origin, destin, weight) \
                                      ('buys', 9999999, 9999, 0.99)") as r:
         if r.ok():
            raise db.TestFailed("I can insert an edge without timestamp :-(")
         else:
            print "%d: %s" % (r.code(), r.details())

#### MAIN ####################################################################
if __name__ == '__main__':
    with now.Connection("localhost", "55505", None, None) as c:
        (ps, cs, es) = db.loadDB(c, "db100")

        print "%d, %s" % (ps[0].key, ps[0].desc)

        vertexSelectNoPK(c)
        keyzero(c)
        createInvalidEdge(c)
        invalidEdgeInserts(c)
