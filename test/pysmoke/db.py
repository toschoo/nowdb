import now
import random
import datetime

PRODUCTRANGE = 1000
CLIENTRANGE = 9000000

class FailedCreation(Exception):
    def __init__(self, m):
         self.msg = m

    def __str__(self):
        return self.msg

class FailedLoading(Exception):
    def __init__(self, m):
         self.msg = m

    def __str__(self):
        return self.msg

class TestFailed(Exception):
    def __init__(self, m):
         self.msg = m

    def __str__(self):
        return self.msg

def createDB(c, db):
    stmt = "drop schema %s if exists" % db
    with c.execute(stmt) as r:
        if not r.ok():
            raise FailedCreation("cannot drop database: %d (%s)" % (r.code(), r.details()))

    stmt = "create schema %s" % db
    with c.execute(stmt) as r:
        if not r.ok():
            raise FailedCreation("cannot create database")

    stmt = "use %s" % db
    with c.execute(stmt) as r:
        if not r.ok():
            raise FailedCreation("cannot use %s" % db)

    with c.execute("create type product ( \
                      prod_key uint primary key, \
                      prod_desc    text, \
                      prod_cat     uint, \
                      prod_packing uint, \
                      prod_price float)") as r:
        if not r.ok():
            raise FailedCreation("cannot create type product")

    # this one is for insert tests
    with c.execute("create type product2 ( \
                      prod_key uint primary key, \
                      prod_desc    text, \
                      prod_cat     uint, \
                      prod_packing uint, \
                      prod_price float)") as r:
        if not r.ok():
            raise FailedCreation("cannot create type product")
   
    with c.execute("create type client ( \
                      client_key uint primary key, \
                      client_name text, \
                      birthdate time)") as r:
        if not r.ok():
            raise FailedCreation("cannot create type client")
   
    with c.execute("create type store ( \
                      store_name text primary key, \
                      city text, \
                      address text, \
                      size float)") as r:
        if not r.ok():
            raise FailedCreation("cannot create type client")
   
    with c.execute("create stamped edge buys ( \
                      origin client, \
                      destin product, \
                      quantity uint, \
                      price float)") as r:
        if not r.ok():
            raise FailedCreation("cannot create edge buys: %s (%d)" % \
                                            (r.details(), r.code()))
   
    with c.execute("create stamped edge visits ( \
                      origin client, \
                      destin store, \
                      quantity uint, \
                      price float)") as r:
        if not r.ok():
            raise FailedCreation("cannot create edge vists")

def addRandomData(c, ps, cs, ss, bs, vs):
    products = createProducts(ps)
    clients = createClients(cs)
    stores = createStores(ss)
    (buys, visits) = createEdges(bs, vs, clients, products, stores)

    storeProducts(c, products)
    storeClients(c, clients)
    storeStores(c, stores)
    storeEdges(c, buys)
    storeEdges(c, visits)

    return (products, clients, stores, buys, visits)

def db2csv(p,c,e):
    products2csv("rsc/products.csv", p)
    clients2csv("rsc/clients.csv", c)
    edges2csv("rsc/buys.csv", e)
    edges2csv("rsc/visits.csv", e)
    # missing:
    # stores and visits

def loadDB(c, db):
    stmt = "use %s" % db
    with c.execute(stmt) as r:
        if not r.ok():
            raise FailedLoading("cannot use %s" % db)
    (bs, vs) = loadEdges(c)
    return (loadProducts(c), loadClients(c), loadStores(c), bs, vs)

def loadProducts(c):
    ps = []
    with c.execute("select prod_key, prod_desc, prod_cat, \
                           prod_packing, prod_price \
                      from product") as cur:
        for row in cur:
            ps.append(Product(row.field(0), row.field(1), row.field(2), row.field(3), row.field(4)))
    return ps

def loadClients(c):
    cs = []
    with c.execute("select client_key, client_name, birthdate from client") as cur:
        for row in cur:
            cs.append(Client(row.field(0), row.field(1), now.now2dt(row.field(2))))
    return cs

def loadStores(c):
    vs = []
    with c.execute("select store_name, city, address, size from store") as cur:
        for row in cur:
            vs.append(Store(row.field(0), row.field(1), row.field(2), row.field(3)))
    return vs

def loadEdges(c):
    bs = []
    with c.execute("select origin, destin, timestamp, quantity, price from buys") as cur:
        for row in cur:
           bs.append(BuysEdge(row.field(0),
                              row.field(1),
                              now.now2dt(row.field(2)),
                              row.field(3),
                              row.field(4)))
    vs = []
    with c.execute("select origin, destin, timestamp, quantity, price from visits") as cur:
        for row in cur:
           vs.append(VisitsEdge(row.field(0),
                                row.field(1),
                                now.now2dt(row.field(2)),
                                row.field(3),
                                row.field(4)))
    return (bs, vs)

def loadFromCsv():
    with c.execute("load 'rsc/products.csv' into vertex use header as product") as r:
        if not r.ok():
            raise FailedCreation("cannot load products: %s" % r.details())

    with c.execute("load 'rsc/clients.csv' into vertex use header as client") as r:
        if not r.ok():
             raise FailedCreateion("cannot load clients: %s" % r.details())

    with c.execute("load 'rsc/edges.csv' into sales as edge") as r:
        if not r.ok():
             raise FailedCreateion("cannot load edges: %s" % r.details())

    # missing:
    # stores

def storeProducts(conn, ps):
    for p in ps:
        p.store(conn)

def products2csv(csv, ps):
   with open(csv, 'w') as f:
        f.write("prod_key;prod_desc;prod_cat;prod_packing;prod_price\n");
        for p in ps:
            p.tocsv(f)

def storeClients(conn, cs):
    for c in cs:
        c.store(conn)

def clients2csv(csv, cs):
   with open(csv, 'w') as f:
        f.write("client_key;client_name;birthdate\n");
        for c in cs:
            c.tocsv(f)

def storeStores(conn, ss):
    for s in ss:
        s.store(conn)

    # missing: 2csv

def storeEdges(conn, es):
    for e in es:
        e.store(conn)

def edges2csv(csv, es):
   with open(csv, 'w') as f:
        for e in es:
            e.tocsv(f)

class Product:
    def __init__(self, key, desc, cat, pack, price):
        self.key = key
        self.desc = desc
        self.cat  = cat
        self.packing = pack
        self.price = price 

    def store(self, c):
        stmt = "insert into product ("
        stmt += "prod_key, prod_desc, prod_cat, \
                 prod_packing, prod_price) ("
        stmt += str(self.key) + ", "
        stmt += "'" + self.desc + "', "
        stmt += str(self.cat) + ", "
        stmt += str(self.packing)  + ", "
        stmt += str(self.price) + ")"
        with c.execute(stmt) as r:
            if not r.ok():
               raise FailedCreation("cannot insert product -- %s" 
                                                   % r.details())

    def tocsv(self, csv):
        line = ""
        line += str(self.key) + ";"
        line += self.desc + ";"
        line += str(self.cat)  + ";"
        line += str(self.packing) + ";"
        line += str(self.price) + "\n"
        csv.write(line)

class Client:
    def __init__(self, key, name, birthdate):
        self.key = key
        self.name = name
        self.birthdate = birthdate

    def store(self, c):
        stmt = "insert into client ("
        stmt += "client_key, client_name, birthdate) ("
        stmt += str(self.key) + ", "
        stmt += "'" + self.name + "', "
        stmt += "'" + self.birthdate.strftime(now.TIMEFORMAT) + "')"
        with c.execute(stmt) as r:
            if not r.ok():
               raise FailedCreation("cannot insert client -- %s" 
                                                  % r.details())

    def tocsv(self, csv):
        line = ""
        line += str(self.key) + ";"
        line += self.name + ";"
        line += self.birthdate.strftime(now.TIMEFORMAT) + "\n"
        csv.write(line)

class Store:
    def __init__(self, name, city, address, size):
        self.name = name 
        self.city = city  
        self.address = address
        self.size = size

    def store(self, c):
        stmt = "insert into store ("
        stmt += "store_name, city, address, size) ("
        stmt += "'" + self.name + "', "
        stmt += "'" + self.city + "', "
        stmt += "'" + self.address + "', "
        stmt += str(self.size) + ")"
	# print stmt
        with c.execute(stmt) as r:
            if not r.ok():
               raise FailedCreation("cannot insert client -- %s" 
                                                  % r.details())

    def tocsv(self, csv):
        line = ""
        line += str(self.name) + ";"
        line += self.city + ";"
        line += self.address + ";"
        line += self.size + "\n"
        csv.write(line)

class BuysEdge:
    def __init__(self, o, d, t, q, p):
       self.origin  = o
       self.destin  = d
       self.tp      = t
       self.quantity = q
       self.price = p

    def store(self, c):
       stmt = "insert into buys "
       stmt += "(origin, destin, timestamp, quantity, price) ("
       stmt += str(self.origin.key) + ", "
       stmt += str(self.destin.key) + ", "
       stmt += "'" + self.tp.strftime(now.TIMEFORMAT) + "', "
       stmt += str(self.quantity) + ", "
       stmt += str(self.price) + ")"
       # print "executing %s" % stmt
       with c.execute(stmt) as r:
            if not r.ok():
               raise FailedCreation("cannot insert buys -- %s" % r.details())

    def tocsv(self, csv):
        line = ""
        line += str(self.origin) + ";"
        line += str(self.destin) + ";"
        line += ";"
        line += self.tp.strftime(now.TIMEFORMAT) + ";"
        line += str(self.quantity) + ";"
        line += str(self.price) + "\n"
        csv.write(line)

class VisitsEdge:
    def __init__(self, o, d, t, q, p):
       self.origin  = o
       self.destin  = d
       self.tp      = t
       self.quantity = q
       self.price = p

    def store(self, c):
       stmt = "insert into visits "
       stmt += "(origin, destin, timestamp, quantity, price) ("
       stmt += str(self.origin.key) + ", "
       stmt += "'" + self.destin.name + "', "
       stmt += "'" + self.tp.strftime(now.TIMEFORMAT) + "', "
       stmt += str(self.quantity) + ", "
       stmt += str(self.price) + ")"
       # print "executing %s" % stmt
       with c.execute(stmt) as r:
            if not r.ok():
               raise FailedCreation("cannot insert visits -- %s" % r.details())

    def tocsv(self, csv):
        line = ""
        line += str(self.origin) + ";"
        line += str(self.destin) + ";"
        line += ";"
        line += self.tp.strftime(now.TIMEFORMAT) + ";"
        line += str(self.quantity) + ";"
        line += str(self.price) + "\n"
        csv.write(line)

def createProducts(no):
    p = []
    for i in range(no):
        x = random.randint(1,100)
        if x < 55:
           cat = 2 if random.randint(0,2) == 0 else 4
        else:
           cat = (x%5) + 1

        p.append(Product(1000 + i, \
                 randomString(random.randint(1,16)), \
                 cat, 
                 random.randint(1,3), \
                 float(random.randint(1,25))/float(random.randint(1,10))))
    return p

def createClients(no):
    c = []
    t = datetime.datetime.utcnow()
    for i in range(no):
        c.append(Client(4000000 + i, randomString(random.randint(1,16)), \
                        t - datetime.timedelta(days=365*random.randint(12,100), \
                              hours=0, minutes=0, seconds=0, microseconds=0)))
    return c

def createStores(no):
    s = []
    for i in range(no):
        s.append(Store(str(i+100), \
                       randomString(random.randint(1,5)),  \
                       randomString(random.randint(1,12)), \
                       float(random.randint(500,10000)/float(random.randint(1,5)))))
    return s

def createEdges(bs, vs, c, p, s):
    b = []
    t = datetime.datetime.utcnow()
    for i in range(bs):
        cdx = random.randint(0,len(c)-1)
        pdx = random.randint(0,len(p)-1)
        tp = t + datetime.timedelta(
            microseconds=random.randint(0,96*3600000000))
        w = random.randint(1,100)
        w2 = w * p[pdx].price
        b.append(BuysEdge(c[cdx], p[pdx], tp, w, w2))

    v = []
    t = datetime.datetime.utcnow()
    for i in range(vs):
        cdx = random.randint(0,len(c)-1)
        sdx = random.randint(0,len(s)-1)
        tp = t + datetime.timedelta(
            microseconds=random.randint(0,96*3600000000))
        w = random.randint(1,100)
        w2 = w * p[pdx].price
        v.append(VisitsEdge(c[cdx], s[sdx], tp, w, w2))

    return (b,v)

def randomString(l):
    s = ""
    for i in range(l):
       c = random.randint(65,90)
       s += str(chr(c))
    return s
