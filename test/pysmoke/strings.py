#!/usr/bin/env python

# to be run after basicsmoke

import now
import db
import datetime
import time
import random

def simplestring(c):

    print("RUNNING TEST 'simplestring'")

    stmt = "select 'pizza', 5.99"
    pizza = 'pizza'
    price = 5.99

    runtest(c, stmt, pizza, price)

def apostrophe(c):

    print("RUNNING TEST 'apostrophe'")

    stmt = "select 'pizza \\'speciale\\'', 5.99"
    pizza = 'pizza \'speciale\''
    price = 5.99

    runtest(c, stmt, pizza, price)

def linefeed(c):

    print("RUNNING TEST 'linefeed'")

    stmt = "select 'pizza \\'speciale\\'\\nFamily Size!', 5.99"
    pizza = 'pizza \'speciale\'\nFamily Size!'
    price = 5.99

    runtest(c, stmt, pizza, price)

def careturn(c):

    print("RUNNING TEST 'careturn'")

    stmt = "select 'pizza \\'speciale\\'\\rFamily Size!', 5.99"
    pizza = 'pizza \'speciale\'\rFamily Size!'
    price = 5.99

    runtest(c, stmt, pizza, price)

def carandfeed(c):

    print("RUNNING TEST 'carandfeed'")

    stmt = "select 'pizza \\'speciale\\'\\r\\nFamily Size!', 5.99"
    pizza = 'pizza \'speciale\'\r\nFamily Size!'
    price = 5.99

    print("testing: '%s'" % pizza)
    runtest(c, stmt, pizza, price)

def tab(c):

    print("RUNNING TEST 'tab'")

    stmt = "select 'pizza \\'speciale\\'\\r\\nFamily Size!\\tYou\\'ll love it!', 5.99"
    pizza = 'pizza \'speciale\'\r\nFamily Size!\tYou\'ll love it!'
    price = 5.99

    runtest(c, stmt, pizza, price)

def esc(c):

    print("RUNNING TEST 'esc'")

    stmt = "select 'pizza \\'speciale\\'\\r\\nFamily Size!\\tYou\\'ll love it! \\\ Or Money back!', 5.99"
    pizza = 'pizza \'speciale\'\r\nFamily Size!\tYou\'ll love it! \ Or Money back!'
    price = 5.99

    print("testing: '%s'" % pizza)
    runtest(c, stmt, pizza, price)

def empty(c):

    print("RUNNING TEST 'empty'")

    stmt = "select '', 5.99"
    pizza = ''
    price = 5.99

    print("testing: '%s'" % pizza)
    runtest(c, stmt, pizza, price)

def emptyinempty(c):

    print("RUNNING TEST 'emptyinempty'")

    stmt = "select '\\'\\'', 5.99"
    pizza = '\'\''
    price = 5.99

    print("testing: '%s'" % pizza)
    runtest(c, stmt, pizza, price)

def runtest(c, stmt, pizza, price):
    print("executing '%s'" % stmt)
    r = c.oneRow(stmt)
    if r[0] != pizza:
       raise db.TestFailed("not a pizza: %s | %s" % (r[0], pizza))
    if r[1] != price:
       raise db.TestFailed("wrong price: %d | %d" % (r[1], price))

if __name__ == "__main__":

    random.seed()

    with now.Connection("localhost", "55505", None, None) as c:
        (ps, cs, ss, es, vs) = db.loadDB(c, "db100")

        simplestring(c)
        apostrophe(c)
        linefeed(c)
        careturn(c)
        carandfeed(c)
        tab(c)
        esc(c)
        empty(c)
        emptyinempty(c)

        print("PASSED")
