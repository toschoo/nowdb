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

    stmt = "select prod_desc, prod_price from product \
             where prod_key = %d" % ps[0].key
    with c.execute(stmt) as cur:
        if not cur.ok():
            print "%d: %s" % (cur.code(), cur.details())
            raise db.TestFailed("cannot execute select on product with pk")
        for row in cur:
            print "%s: %.2f" % (row.field(0), row.field(1))

    stmt = "select prod_key from product \
             where prod_desc = '%s'" % ps[0].desc
    with c.execute(stmt) as cur:
        if not cur.ok():
            print "%d: %s" % (cur.code(), cur.details())
            raise db.TestFailed("cannot execute select on product without pk")
        found = False
        for row in cur:
            print "%d" % row.field(0)
            if row.field(0) == ps[0].key:
                found=True
        if not found:
            raise db.TestFailed("product not found: %d, %s" %
                                   (ps[0].key,ps[0].desc))

    stmt = "select client_key from client \
             where client_name = '%s'" % cs[0].name
    with c.execute(stmt) as cur:
        if not cur.ok():
            print "%d: %s" % (cur.code(), cur.details())
            raise db.TestFailed("cannot execute select on client without pk")
        found=False
        for row in cur:
            print "%d" % row.field(0)
            if row.field(0) == cs[0].key:
                found=True
        if not found:      
           raise db.TestFailed("product not found. %d, %s" %
                                   (cs[0].key, cs[0].name))

    stmt = "select prod_desc from product"
    with c.execute(stmt) as cur:
        n=0
        if not cur.ok():
            print "%d: %s" % (cur.code(), cur.details())
            raise db.TestFailed("cannot execute select on product")
        for row in cur:
            n+=1

        if n != len(ps):
           raise db.TestFailed("expecting %d products, but have %d" % (len(ps), n))

    stmt = "select client_name from client"
    with c.execute(stmt) as cur:
        n=0
        if not cur.ok():
            print "%d: %s" % (cur.code(), cur.details())
            raise db.TestFailed("cannot execute select on client")
        for row in cur:
            n+=1

        if n != len(cs):
           raise db.TestFailed("expecting %d clients, but have %d" % (len(cs), n))

# it shall be possible to use text as primary key
def whereWithStringKey(c):

    print "RUNNING TEST 'whereWithStringKey'"

    idx = random.randint(1,len(vs)-1)

    stmt = "select store_name, size from store \
             where store_name = '%s'" % vs[idx].destin
    with c.execute(stmt) as cur:
       if not cur.ok():
          raise db.TestFailed("not ok: %d (%s)" % (cur.code(), cur.details()))
       n = 0
       for row in cur:
           if row.field(0) != vs[idx].destin:
              raise db.TestFailed("unexpected store: %s" % row.field(0))
           n+=1
       if n < 1:
              raise db.TestFailed("nothing found")
       if n != 1:
              raise db.TestFailed("one row expected; found: %d" % n)

    stmt = "select origin, destin from visits \
             where destin = '%s'" % vs[idx].destin
    with c.execute(stmt) as cur:
       if not cur.ok():
          raise db.TestFailed("not ok: %d (%s)" % (cur.code(), cur.details()))
       n = 0
       for row in cur:
           if row.field(1) != vs[idx].destin:
              raise db.TestFailed("unexpected edge: %s" % row.field(1))
           n+=1
       if n < 1:
              raise db.TestFailed("nothing found")
          
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
              from testzero \
             where test_key = 0"
    with c.execute(stmt) as cur:
        if not cur.ok():
           print "%d: %s" % (cur.code(), cur.details())
           raise db.TestFailed("cannot select zero")
        for row in cur:
           print "%d: %s" % (row.field(0), row.field(1))
           if row.field(0) != 0 or row.field(1) != "zero":
              raise db.TestFailed("zero values differ!")

    ### DELETE ZERO!

    stmt = "drop type testzero"
    with c.execute(stmt) as r:
        if not r.ok():
           print "%d: %s" % (r.code(), r.details())
           raise db.TestFailed("cannot drop zero")

# create edge must contain origin and destin
# origin and edgin must be vertex types
def createInvalidEdge(c):

    print "RUNNING TEST 'createInvalidEdge'"

    with c.execute("create stamped edge testedge (origin client, price float)") as r:
         if r.ok():
            raise db.TestFailed("I can create an edge without destin :-(")
         else:
            print "%d: %s" % (r.code(), r.details())

    with c.execute("create stamped edge testedge (destin product, price float)") as r:
         if r.ok():
            raise db.TestFailed("I can create an edge without origin :-(")
         else:
            print "%d: %s" % (r.code(), r.details())

    with c.execute("create stamped edge testedge (origin uint, destin product, price float)") as r:
         if r.ok():
            raise db.TestFailed("I can create an edge with origin that is not a vertex :-(")
         else:
            print "%d: %s" % (r.code(), r.details())

    with c.execute("create stamped edge testedge (origin client, destin uint, price float)") as r:
         if r.ok():
            raise db.TestFailed("I can create an edge with destin that is not a vertex :-(")
         else:
            print "%d: %s" % (r.code(), r.details())

    with c.execute("create edge unstamped (origin client, destin product)") as r:
         if not r.ok():
            raise db.TestFailed("ERROR creating unstamed edge: %d: %s" % (r.code(), r.details()))

# insert into edge must contain edge and timestamp
def invalidEdgeInserts(c):

    print "RUNNING TEST 'invalidEdgeInserts'"

    with c.execute("insert into buys (origin, destin, price) \
                                     (9999999, 9999, 0.99)") as r:
         if r.ok():
            raise db.TestFailed("I can insert an edge without timestamp :-(")
         else:
            print "%d: %s" % (r.code(), r.details())

    with c.execute("insert into buys (destin, stamp, price) \
                                     (9999, '2018-10-10', 0.99)") as r:
         if r.ok():
            raise db.TestFailed("I can insert an edge without origin :-(")
         else:
            print "%d: %s" % (r.code(), r.details())

    with c.execute("insert into buys (origin, stamp, price) \
                                     (9999999, '2018-10-10', 0.99)") as r:
         if r.ok():
            raise db.TestFailed("I can insert an edge without destin :-(")
         else:
            print "%d: %s" % (r.code(), r.details())

    with c.execute("insert into unstamped (origin, destin) \
                                          (9999999, 9999)") as r:
         if not r.ok():
            raise db.TestFailed("I cannot insert an unstamped edge %d: %s" % (r.code(), r.details()))

    with c.execute("insert into unstamped (origin, destin, stamp) \
                                          (9999999, 9999, '2018-10-10')") as r:
         if r.ok():
            raise db.TestFailed("I can insert an unstamped edge with stamp")
         else:
            print "%d: %s" % (r.code(), r.details())

    with c.execute("insert into unstamped (origin, stamp) \
                                          (9999999, '2018-10-10')") as r:
         if r.ok():
            raise db.TestFailed("I can insert an unstamped edge without destin")
         else:
            print "%d: %s" % (r.code(), r.details())

    with c.execute("insert into unstamped (destin, stamp) \
                                          (9999999, '2018-10-10')") as r:
         if r.ok():
            raise db.TestFailed("I can insert an unstamped edge without origin")
         else:
            print "%d: %s" % (r.code(), r.details())

    with c.execute("insert into unstamped (origin) (9999999)") as r:
         if r.ok():
            raise db.TestFailed("I can insert an unstamped edge without origin")
         else:
            print "%d: %s" % (r.code(), r.details())

    with c.execute("insert into unstamped (destin) (9999999)") as r:
         if r.ok():
            raise db.TestFailed("I can insert an unstamped edge without origin")
         else:
            print "%d: %s" % (r.code(), r.details())

# it shall not be possible to create an edge and a type of the same name
# it shall not be possible to create a context and a type of the same name
# it shall not be possible to create a context and an edge of the same name
def doublenaming(c):

    print "RUNNING TEST 'doublenaming'"

    # type and edge
    with c.execute("create stamped edge fooedge (origin client, destin product, price float)") as r:
         if not r.ok():
            raise db.TestFailed("I cannot create an edge named 'fooedge': %s" % r.details())

    with c.execute("create type fooedge (foo_key uint primary key, foo_name text)") as r:
         if r.ok():
            raise db.TestFailed("I can create type 'fooedge' (which is an edge) :-(")

    with c.execute("create type bartype (bar_key uint primary key, bar_name text)") as r:
         if not r.ok():
            raise db.TestFailed("I cannot create a type named 'bartype': %s" % r.details())

    with c.execute("create stamped edge bartype (origin client, destin product, price float)") as r:
         if r.ok():
            raise db.TestFailed("I can create an edge named 'bartype' which is a type :-(")

    with c.execute("drop edge fooedge") as r:
         if not r.ok():
            raise db.TestFailed("I cannot drop edge fooedge: %s" % r.details())

    with c.execute("drop type bartype") as r:
         if not r.ok():
            raise db.TestFailed("I cannot drop type bartype: %s" % r.details())

    """ this must be storage instead of context!
    # type and context
    with c.execute("create table fooctx") as r:
         if not r.ok():
            raise db.TestFailed("I cannot create a table named 'fooctx': %s" % r.details())

    with c.execute("create type fooctx (foo_key uint primary key, foo_name text)") as r:
         if r.ok():
            raise db.TestFailed("I can create type 'fooctx' (which is a table) :-(")

    with c.execute("create type bartype (bar_key uint primary key, bar_name text)") as r:
         if not r.ok():
            raise db.TestFailed("I cannot create a type named 'bartype': %s" % r.details())

    with c.execute("create table bartype") as r:
         if r.ok():
            raise db.TestFailed("I can create an edge named 'bartype' which is a type :-(")

    with c.execute("drop table fooctx") as r:
         if not r.ok():
            raise db.TestFailed("I cannot drop table fooctx: %s" % r.details())

    with c.execute("drop type bartype") as r:
         if not r.ok():
            raise db.TestFailed("I cannot drop type bartype: %s" % r.details())

    # edge and context
    with c.execute("create table fooctx") as r:
         if not r.ok():
            raise db.TestFailed("I cannot create a table named 'fooctx': %s" % r.details())

    with c.execute("create edge fooctx (origin client, destin product, price float)") as r:
         if r.ok():
            raise db.TestFailed("I can create edge 'fooctx' (which is a table) :-(")

    with c.execute("create edge baredge (origin client, destin product, price float)") as r:
         if not r.ok():
            raise db.TestFailed("I cannot create an edge named 'baredge': %s" % r.details())

    with c.execute("create table baredge") as r:
         if r.ok():
            raise db.TestFailed("I can create an table named 'baredge' which is an edge :-(")

    with c.execute("drop table fooctx") as r:
         if not r.ok():
            raise db.TestFailed("I cannot drop table fooctx: %s" % r.details())

    with c.execute("drop edge baredge") as r:
         if not r.ok():
            raise db.TestFailed("I cannot drop edge baredge: %s" % r.details())
    """

#### MAIN ####################################################################
if __name__ == '__main__':
    with now.Connection("localhost", "55505", None, None) as c:
        (ps, cs, ss, es, vs) = db.loadDB(c, "db100")

        print "%d, %s" % (ps[0].key, ps[0].desc)

        vertexSelectNoPK(c)
        whereWithStringKey(c)
        keyzero(c)
        createInvalidEdge(c)
        invalidEdgeInserts(c)
        #doublenaming(c)

        print "PASSED"
