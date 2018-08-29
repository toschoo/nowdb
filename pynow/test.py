from now import *

c = connect("127.0.0.1", 55505, None, None)
if c == None:
    print "cannot connect"
    exit(1)
  

r = c.execute("use retail")
if not r.ok():
    print "ERROR %d on use: %s" % (r.code(), r.details())
    r.release()
    exit(1)

r.release()

r = c.execute("select edge, count(*), sum(weight) from tx where edge='buys_product' and origin=0")
if not r.ok():
    print "ERROR %d on select: %s" % (r.code(), r.details())
    r.release()
    exit(1)

row = r.row()

s = row.field(0)
m = row.field(1)
w = row.field(2)

print "%s: %.4f %d" % (s, w, m)

row.release()

r = c.execute("select edge, destin, weight from tx where edge='buys_product' and origin=0")
if not r.ok():
    print "ERROR %d on select: %s" % (r.code(), r.details())
    r.release()
    exit(1)

t = 0.0
cnt = 0
row = r.row()
while r.ok():
    s = row.field(0)
    d = row.field(1)
    w = row.field(2)
    t += w
    cnt += 1

    print "%s: %d %.4f" % (s, d, w)

    if not row.next():
        if not r.fetch():
            print "cannot fetch"
            break
        row.release()
        row = r.row()

if not r.eof():
    print "ERROR %d on select: %s" % (r.code(), r.details())
    r.release()
    exit(1)

print "sum: %.4f\n" % t
    
r.release()
c.close()
