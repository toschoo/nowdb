from now import *

with Connection("127.0.0.1", "55505", None, None) as c:
  print "connection created"

  with c.execute("use retail") as r:
    if not r.ok():
        print "ERROR %d on use: %s" % (r.code(), r.details())
        exit(1)

  with c.execute("select edge, count(*), sum(weight) from tx \
                   where edge='buys_product' and origin=0") as r:
    if not r.ok():
      print "ERROR %d on select: %s" % (r.code(), r.details())
      exit(1)

    with r.row() as row:
      s = row.field(0)
      m = row.field(1)
      w = row.field(2)

      print "%s: %.4f %d" % (s, w, m)

  with c.execute("select edge, destin, weight from tx \
                   where edge='buys_product' and origin=0") as r:
    if not r.ok():
        print "ERROR %d on select: %s" % (r.code(), r.details())
        exit(1)

    t = 0.0
    cnt = 0
    while r.ok():
      print "r: %d" % r.rType()
      with r.row() as row:
        while True:
          s = row.field(0)
          d = row.field(1)
          w = row.field(2)
          t += w
          cnt += 1

          print "%s: %d %.4f" % (s, d, w)

          if not row.next():
             break

        r.fetch()

    if not r.eof():
      print "ERROR %d on select: %s" % (r.code(), r.details())
      exit(1)

  print "sum: %.4f\n" % t
    
# connection closed at this point
print "explain -1: %s" % explain(-1)
print "explain  8: %s" % explain(8)
print "explain -2: %s" % explain(-2)
print "explain -3: %s" % explain(-3)
print "explain -4: %s" % explain(-4)
