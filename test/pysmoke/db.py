import now
import random
import datetime

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
            raise FailedCreation("cannot drop database")

    stmt = "create schema %s" % db
    with c.execute(stmt) as r:
        if not r.ok():
            raise FailedCreation("cannot create database")

    stmt = "use %s" % db
    with c.execute(stmt) as r:
        if not r.ok():
            raise FailedCreation("cannot use %s" % db)

    with c.execute("create table sales") as r:
        if not r.ok():
            raise FailedCreation("cannot create table sales")

    with c.execute("create index idx_sales_ed on sales (edge,destin)") as r:
        if not r.ok():
            raise FailedCreation("cannot create index idx_sales_ed")

    with c.execute("create index idx_sales_eo on sales (edge,origin)") as r:
        if not r.ok():
            raise FailedCreation("cannot create index idx_sales_eo")

    with c.execute("create index vidx_ropo on vertex (role, property)") as r:
        if not r.ok():
            raise FailedCreation("cannot create index vidx_ropo")
   
    with c.execute("create type product ( \
                      prod_key uint primary key, \
                      prod_desc text, \
                      prod_price float)") as r:
        if not r.ok():
            raise FailedCreation("cannot create type product")
   
    with c.execute("create type client ( \
                      client_key uint primary key, \
                      client_name text)") as r:
        if not r.ok():
            raise FailedCreation("cannot create type client")
   
    with c.execute("create edge buys ( \
                      origin client, \
                      destination product, \
                      weight uint, \
                      weight2 float)") as r:
        if not r.ok():
            raise FailedCreation("cannot create edge buys")

def addRandomData(c, ps, cs, es):
    products = createProducts(ps)
    clients = createClients(cs)
    edges = createEdges(es, clients, products)

    storeProducts(c, products)
    storeClients(c, clients)
    storeEdges(c, edges)

    return (products, clients, edges)

def db2csv(p,c,e):
    products2csv("rsc/products.csv", p)
    clients2csv("rsc/clients.csv", c)
    edges2csv("rsc/edges.csv", e)

def loadDB(c, db):
    stmt = "use %s" % db
    with c.execute(stmt) as r:
        if not r.ok():
            raise FailedLoading("cannot use %s" % db)
    return (loadProducts(c), loadClients(c), loadEdges(c))

def loadProducts(c):
    ps = []
    with c.execute("select prod_key, prod_desc, prod_price from vertex as product") as cur:
        for row in cur:
            ps.append(Product(row.field(0), row.field(1), row.field(2)))
    return ps

def loadClients(c):
    cs = []
    with c.execute("select client_key, client_name from vertex as client") as cur:
        for row in cur:
            cs.append(Client(row.field(0), row.field(1)))
    return cs

def loadEdges(c):
    es = []
    with c.execute("select origin, destin, timestamp, weight, weight2 from sales") as cur:
        for row in cur:
           es.append(BuyEdge(row.field(0),
                             row.field(1),
                             now.now2dt(row.field(2)),
                             row.field(3),
                             row.field(4)))
    return es

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

def storeProducts(conn, ps):
    for p in ps:
        p.store(conn)

def products2csv(csv, ps):
   with open(csv, 'w') as f:
        f.write("prod_key;prod_desc;prod_price\n");
        for p in ps:
            p.tocsv(f)

def storeClients(conn, cs):
    for c in cs:
        c.store(conn)

def clients2csv(csv, cs):
   with open(csv, 'w') as f:
        f.write("client_key;client_name\n");
        for c in cs:
            c.tocsv(f)

def storeEdges(conn, es):
    for e in es:
        e.store(conn)

def edges2csv(csv, es):
   with open(csv, 'w') as f:
        for e in es:
            e.tocsv(f)

class Product:
    def __init__(self, key, desc, price):
        self.key = key
        self.desc = desc
        self.price = price 

    def store(self, c):
        stmt = "insert into product ("
        stmt += "prod_key, prod_desc, prod_price) ("
        stmt += str(self.key) + ", "
        stmt += "'" + self.desc + "', "
        stmt += str(self.price) + ")"
        with c.execute(stmt) as r:
            if not r.ok():
               raise FailedCreation("cannot insert product -- %s" 
                                                   % r.details())

    def tocsv(self, csv):
        line = ""
        line += str(self.key) + ";"
        line += self.desc + ";"
        line += str(self.price) + "\n"
        csv.write(line)

class Client:
    def __init__(self, key, name):
        self.key = key
        self.name = name

    def store(self, c):
        stmt = "insert into client ("
        stmt += "client_key, client_name) ("
        stmt += str(self.key) + ", "
        stmt += "'" + self.name + "')"
        with c.execute(stmt) as r:
            if not r.ok():
               raise FailedCreation("cannot insert client -- %s" 
                                                  % r.details())

    def tocsv(self, csv):
        line = ""
        line += str(self.key) + ";"
        line += self.name + "\n"
        csv.write(line)

class BuyEdge:
    def __init__(self, o, d, t, w, w2):
       self.edge = 'buys'
       self.origin  = o
       self.destin  = d
       self.tp      = t
       self.weight  = w
       self.weight2 = w2

    def store(self, c):
       stmt = "insert into sales "
       stmt += "(edge, origin, destin, timestamp, weight, weight2) ("
       stmt += "'buys',"
       stmt += str(self.origin.key) + ", "
       stmt += str(self.destin.key) + ", "
       stmt += "'" + self.tp.strftime(now.TIMEFORMAT) + "', "
       stmt += str(self.weight) + ", "
       stmt += str(self.weight2) + ")"
       # print "executing %s" % stmt
       with c.execute(stmt) as r:
            if not r.ok():
               raise FailedCreation("cannot insert sales -- %s" % r.details())

    def tocsv(self, csv):
        line = ""
        line += self.edge + ";"
        line += str(self.origin) + ";"
        line += str(self.destin) + ";"
        line += ";"
        line += self.tp.strftime(now.TIMEFORMAT) + ";"
        line += str(self.weight) + ";"
        line += str(self.weight2) + "\n"
        csv.write(line)

def createProducts(no):
    p = []
    for i in range(no):
        p.append(Product(1000 + i, randomString(random.randint(1,16)), \
              float(random.randint(1,25))/float(random.randint(1,10))))
    return p

def createClients(no):
    c = []
    for i in range(no):
        c.append(Client(4000000 + i, randomString(random.randint(1,16))))
    return c

def createEdges(no, c, p):
    e = []
    t = datetime.datetime.utcnow()
    for i in range(no):
        cdx = random.randint(0,len(c)-1)
        pdx = random.randint(0,len(p)-1)
        tp = t + datetime.timedelta(
            microseconds=random.randint(0,3600000000))
        w = random.randint(1,100)
        w2 = w * p[pdx].price
        e.append(BuyEdge(c[cdx], p[pdx], tp, w, w2))
    return e

def randomString(l):
    s = ""
    for i in range(l):
       c = random.randint(65,90)
       s += str(unichr(c))
    return s
