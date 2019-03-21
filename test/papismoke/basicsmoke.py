#!/usr/bin/env python

import nowapi as na
import now
import db

nump = 100
numc = 50
nums = 25
nume = 1000
numv = 100

def createdb():
    with now.Connection("localhost", "55505", None, None) as c:
        db.createDB(c, "db150")
        return db.addRandomData(c, nump, numc, nums, nume, numv)

def countdb(c, t):
    with c.cursor() as cur:
         cur.setRowFormat(na.listrow)
         cur.execute('select count(*) from %s' % t)
         for row in cur:
             return row[0]

if __name__ == '__main__':
        (products, clients, stores, buys, visits) = createdb()

        pl = len(products)
        cl = len(clients)
        sl = len(stores)
        bl = len(buys)
        vl = len(visits)

	if pl != nump or \
           cl != numc or \
           sl != nums or \
           bl != nume or \
           vl != numv:
           raise db.TestFailed('what we got is not what we demanded...')
        
        print("having %d | %d | %d | %d | %d" % (len(products), len(clients), \
                                                 len(stores), len(buys), len(visits)))

        with na.connect('localhost', '55505', None, None, 'db150') as c:
             n = countdb(c, 'product')
             if n != pl:
                raise db.TestFailed('products (%d) differs from %d' % (n, len(products)))
             print('products OK')
             n = countdb(c, 'client')
             if n != cl:
                raise db.TestFailed('clients  (%d) differs from %d' % (n, len(products)))
             print('clients  OK')
             n = countdb(c, 'store')
             if n != sl:
                raise db.TestFailed('stores   (%d) differs from %d' % (n, len(products)))
             print('stores   OK')
             n = countdb(c, 'buys')
             if n != bl:
                raise db.TestFailed('buys     (%d) differs from %d' % (n, len(products)))
             print('buys     OK')
             n = countdb(c, 'visits')
             if n != vl:
                raise db.TestFailed('visits   (%d) differs from %d' % (n, len(products)))
             print('visits   OK')

        print("PASSED")       


