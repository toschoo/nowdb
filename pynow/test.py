from now import *

with Connection("127.0.0.1", "55505", None, None) as c:
  print "connection created"

  with c.execute("use retail") as r:
    if not r.ok():
        print "ERROR %d on use: %s" % (r.code(), r.details())
        exit(1)

  with c.execute("select edge, count(*), sum(weight) from tx \
                   where edge='buys_product' \
                     and origin=419870620036") as r:
    if not r.ok():
      print "ERROR %d on select: %s" % (r.code(), r.details())
      exit(1)

    with r.row() as row:
      m = row.field(1)
      w = row.field(2)

    print "sum: %.4f / count: %d" % (w, m)

  t = 0.0
  cnt = 0

  with c.execute("select edge, destin, timestamp, weight, true from tx \
                   where edge='buys_product' \
                     and origin=419870620036") as cur:

    for row in cur:
  
      s = row.field(0)
      d = row.field(1)
      dt = row.field(2)
      w = row.field(3)
      y = row.field(4)
      
      ds = now2dt(dt).strftime("%Y-%m-%dT%H:%M:%S")

      t += w
      cnt += 1

      print "%s: %d (%s) %.4f %s" % (s, d, ds, w, y)

  print "sum: %.4f / count: %d" % (t,cnt)


