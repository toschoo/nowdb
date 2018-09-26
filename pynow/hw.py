import random
import nowdb

def hello():
  print "hello nowdb!"
  return nowdb.success()

def sumof2(a,b):
  print "%d" % (a+b)
  return (1,None)

def sumof3(a,b,c):
  print "%d" % (a+b+c)
  return (1,None)

def truediv(a,b):
  print "%f" % (a/b)
  return (1,None)

def lululooping(a,b):
  for i in range(10):
    c = a + b
    print "%d + %d = %d" % (a,b,c)
    b = a
    a = c
  return (1,None)

f1 = 0
f2 = 1
random.seed()
myid = int(round(100000 * random.random()))

def fib(n):
  global f1
  global f2
  global myid

  for i in range(n):
    f = f1 + f2
    print "[%d] %d + %d = %d" % (myid,f1,f2,f)
    f1 = f2
    f2 = f 
  return (1,None)

def fibreset():
  global f1
  global f2

  f1 = 0
  f2 = 1

  return (1,None)

def simple(stmt):
  return nowdb.execute(stmt).toDB()

def happy():
  return nowdb.success().toDB()

def mycount(edge, origin):

  try:

    print "mycount(%s, %d)" % (edge, origin)

    stmt = "select count(*) from tx "
    stmt += "where edge = '" + edge + "'"
    stmt += "  and origin =" + str(origin)

    with nowdb.execute(stmt) as c:

      if not c.ok():
        return nowdb.makeError(c.code(), c.details()).toDB()

      cnt1 = 0
      for row in c:
        cnt1 = row.field(0)

    stmt = "select destin from tx "
    stmt += "where edge = '" + edge + "'"
    stmt += "  and origin =" + str(origin)

    with nowdb.execute(stmt) as c:

      if not c.ok():
        return nowdb.makeError(c.code(), c.details()).toDB()

      cnt = 0
      for row in c:
        dst = row.field(0)
        print "desintation: %d" % dst
        cnt += 1
      

    print "select count(*): %d" % cnt1
    print "manual count   : %d" % cnt

    if cnt1 != cnt:
      msg = "counts differ %d != %d" % (cnt1, cnt)
      print "%s" % msg
      return nowdb.makeError(1, msg)

  except StopIteration:
    print "EOF"

  except nowdb.WrongType as w:
    print "Wrong Type: %s" % w

  except nowdb.DBError as d:
    print "DB Error: %s" % d

  except Exception as e:
    print "unexpected exception: %s" % e

  return nowdb.success().toDB()
