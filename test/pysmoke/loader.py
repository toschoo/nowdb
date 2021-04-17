#!/usr/bin/env python

import now
import db
import random as rnd

base  = 'rsc'
SNAME = base + '/' + 'sns.csv'
DNAME = base + '/' + 'dvc.csv'
ENAME = base + '/' + 'edge300.csv'
TNAME = base + '/' + 'tedge300.csv'

sns = []
dvc = []
edg = []
tdg = []

class Sensor:
      def __init__(self, key, name, info=None):
          self.key = key
          self.name = name
          self.info = info

class Device:
      def __init__(self, key, name, desc=None):
          self.key = key
          self.name = name
          self.desc = desc

class Edge:
      def __init__(self, d, s):
          self.device = d
          self.sensor = s

class Measure:
      def __init__(self, s, st, t, r):
          self.sensor = s
          self.stamp = st
          self.temp = t
          self.revo = r

def createSensors(mx):
    for k in range(mx):
        name = randomString(10)
        info = None
        x = rnd.randint(0,2)
        if x == 0:
           info = randomString(20)
        sns.append(Sensor(k+1000,name,info)) 
    with open(SNAME, 'w') as f:
         f.write('key;name;info\n')
         for s in sns:
             n = sanitize(s.name)
             if s.info is None:
                f.write('%d;%s;\n' % (s.key, n))
             else:
                i = sanitize(s.info)
                f.write('%d;%s;%s\n' % (s.key, n, i))
           
def createDevices(mx):
    for k in range(mx):
        name = randomString(10)
        desc = None
        x = rnd.randint(0,2)
        if x == 0:
           desc = randomString(15)
        dvc.append(Device(k+10000,name,desc)) 
    with open(DNAME, 'w') as f:
         f.write('key;name;desc\n')
         for d in dvc:
             n = sanitize(d.name)
             if d.desc is None:
                f.write('%d;%s;\n' % (d.key, n))
             else:
                dsc = sanitize(d.desc)
                f.write('%d;%s;%s\n' % (d.key, n, dsc))

def createDvcSns():
    for s in sns:
        d = dvc[rnd.randint(0,len(dvc)-1)]
        edg.append(Edge(d,s))
    with open(ENAME, 'w') as f:
        f.write('origin;destin\n')
        for e in edg:
            f.write('%d;%d\n' % (e.device.key,e.sensor.key))

def createMeasures(mx):
    for k in range(mx):
        s = sns[rnd.randint(0,len(sns)-1)]
        x = rnd.randint(0,3)
        temp = None
        revo = None
        if x == 0:
           temp = float(rnd.randint(0,100))
           revo = float(rnd.randint(0,100))
        elif x == 1:
           temp = float(rnd.randint(0,100))
        elif x == 2:
           revo = float(rnd.randint(0,100))
        tdg.append(Measure(s,k,temp,revo))
    with open(TNAME, 'w') as f:
         f.write('origin;destin;stamp;temp;revo\n')
         for t in tdg:
             f.write('%d;1;1970-01-01T00:00:00.%09d;%s;%s\n' % \
                    (t.sensor.key, t.stamp,
                     '' if t.temp is None else str(t.temp),
                     '' if t.revo is None else str(t.revo)))

def randomString(mx):
    s = ''
    alpha = 'ABCDEFGHIJKLMNOPQRSTUVWXYZ\n\r\\'
    m = rnd.randint(3,mx+1)
    for i in range(m):
        s += rnd.choice(alpha)
    return s

def sanitize(s):
    return s.replace('\\','\\\\').replace('\n','\\n').replace('\r','\\r')

def createTypes(c):
    for i in range(3):
        if i == 0:
           t = 'sensor'
           f = 'info'
        elif i == 1:
           t = 'device'
           f = 'desc'
        elif i == 2:
           t = 'nirvana'
           f = 'desc'

        with c.execute("drop type %s if exists" % t) as r:
             if not r.ok():
                raise db.TestFailed('cannot drop %s (%s)' % (t, r.details()))
        with c.execute("create type %s (\
                           key uint primary key, \
                           name text, \
                           %s   text)" % (t, f)) as r:
             if not r.ok():
                raise db.TestFailed('cannot create %s' % t)

    with c.execute("insert into nirvana (key, name, desc) \
                                values  (1, '', '')") as r:
         if not r.ok():
                raise db.TestFailed('cannot enter nirvana')

def createEdges(c):
    c.rexecute_("drop storage mystore if exists")
    with c.execute("create small storage mystore") as r:
         if not r.ok():
            raise db.TestFailed('cannot create mystore')
    with c.execute("drop edge dvcsns if exists") as r:
         if not r.ok():
            raise db.TestFailed('cannot drop dvcsns')
    with c.execute("create edge dvcsns ( \
                           origin device origin, \
                           destin sensor destin)") as r:
         if not r.ok():
            raise db.TestFailed('cannot create dvcsns %s' % r.details())
    with c.execute("drop edge measure if exists") as r:
         if not r.ok():
            raise db.TestFailed('cannot drop measure')
    with c.execute("create edge measure ( \
                           origin device  origin, \
                           destin nirvana destin, \
                           stamp  time    stamp, \
                           temp   float, \
                           revo   float) \
                      storage=mystore") as r:
         if not r.ok():
            raise db.TestFailed('cannot create measure')

def load(c):
    with c.execute("load 'rsc/dvc.csv' into device use header set errors='rsc/dvc.err'") as r:
         if not r.ok():
            raise db.TestFailed('cannot load dvc: %s' % r.details())
    with c.execute("load 'rsc/sns.csv' into sensor use header set errors='rsc/sns.err'") as r:
         if not r.ok():
            raise db.TestFailed('cannot load sns: %s' % r.details())
    with c.execute("load 'rsc/edge300.csv' into dvcsns use header set errors='rsc/edge300.err'") as r:
         if not r.ok():
            raise db.TestFailed('cannot load dvcsns %s' % r.details())
    with c.execute("load 'rsc/tedge300.csv' into measure use header set errors='rsc/tedge300.err'") as r:
         if not r.ok():
            raise db.TestFailed('cannot load measure %s' % r.details())

def testNoneIsNull(c):
    print("RUNNING 'testNoneIsNull'")
    print("DEVICES")
    for i in range(len(dvc)):
        k = rnd.randint(0,len(dvc)-1)
        with c.execute("select key, desc from device where key = %d" % dvc[k].key) as cur:
             cnt = 0
             for row in cur:
                 print("%d: '%s'" % (row.field(0), row.field(1)))
                 if row.field(0) != dvc[k].key:
                    raise db.TestFailed('wrong key: %d' % k)
                 if (row.field(1) is None and \
                     dvc[k].desc is not None) or \
                    (row.field(1) is not None and \
                     dvc[k].desc is None):
                    raise db.TestFailed('NULL mismatch: %d' % k)
                 cnt += 1
             if cnt != 1:
                raise db.TestFailed("too many or not enough rows: %d" % cnt)

    print("SENSORS")
    for i in range(len(sns)):
        k = rnd.randint(0,len(sns)-1)
        with c.execute("select key, info from sensor where key = %d" % sns[k].key) as cur:
             cnt = 0
             for row in cur:
                 print("%d: '%s'" % (row.field(0), row.field(1)))
                 if row.field(0) != sns[k].key:
                    raise db.TestFailed('wrong key: %d' % k)
                 if (row.field(1) is None and \
                     sns[k].info is not None) or \
                    (row.field(1) is not None and \
                     sns[k].info is None):
                    raise db.TestFailed('NULL mismatch: %d' % k)
                 cnt += 1
             if cnt != 1:
                raise db.TestFailed("too many or not enough rows: %d" % cnt)

    for i in range(len(tdg)//50):
        k = rnd.randint(0,len(tdg)-1)
        with c.execute("select origin, temp, revo \
                          from measure \
                         where origin = %d \
                           and stamp  = %d" % (tdg[k].sensor.key,tdg[k].stamp)) as cur:
             cnt = 0
             for row in cur:
                 # print("%d: %s | %s" % (row.field(0), row.field(1), row.field(2)))
                 if row.field(0) != tdg[k].sensor.key:
                    raise db.TestFailed('wrong key: %d' % tdg[k].sensor.key)
                 if (row.field(1) is None and \
                     tdg[k].temp is not None) or \
                    (row.field(1) is not None and \
                     tdg[k].temp is None):
                    raise db.TestFailed('NULL mismatch on temp: %d' % k)
                 if (row.field(2) is None and \
                     tdg[k].revo is not None) or \
                    (row.field(2) is not None and \
                     tdg[k].revo is None):
                    raise db.TestFailed('NULL mismatch on revo: %d' % k)
                 cnt += 1
             if cnt != 1:
                raise db.TestFailed("too many or not enough rows: %d" % cnt)

def testCount(c,mx):

    print("RUNNING 'testCount'")

    with c.execute("select count(*) from sensor") as cur:
         for row in cur:
             if row.field(0) != len(sns):
                raise db.TestFailed("wrong count for sensor: %d | %d" \
                                             % (row.field(0), mx))

    with c.execute("select count(*) from device") as cur:
         for row in cur:
             if row.field(0) != len(dvc):
                raise db.TestFailed("wrong count for device: %d | %d" \
                                           % (row.field(0), mx))

    with c.execute("select count(*) from dvcsns") as cur:
         for row in cur:
             if row.field(0) != len(edg):
                raise db.TestFailed("wrong count for dvcsns: %d | %d" \
                                            % (row.field(0), mx))

    with c.execute("select count(*) from measure") as cur:
         for row in cur:
             if row.field(0) != len(tdg):
                raise db.TestFailed("wrong count for tedge: %d | %d" \
                                           % (row.field(0), mx))

def showSensors(c):
    print("RUNNING 'showSensors'")
    with c.execute("select * from sensor") as cur:
         for row in cur:
             print("%d: %s" % (row.field(0), row.field(1)))

# test strings
def testSensorNames(c):
    print("RUNNING 'testSensorNames'")
    for s in sns:
        n = c.oneValue("select name from sensor where key = %d" % s.key)
        print("comparing '%s' and '%s'" % (n, s.name))
        if n != s.name:
           raise db.TestFailed("wrong name in sensor %d: %s | %s" \
                                               % (s.key, n, s.name))

def testDeviceNames(c):
    print("RUNNING 'testDeviceNames'")
    for d in dvc:
        n = c.oneValue("select name from device where key = %d" % d.key)
        print("comparing '%s' and '%s'" % (n, d.name))
        if n != d.name:
           raise db.TestFailed("wrong name in sensor %d: %s | %s" \
                                               % (d.key, n, d.name))
           
if __name__ == '__main__':

    rnd.seed()

    ms = 50000

    print("starting loader")
    
    with now.Connection("localhost", "55505", None, None) as c:
        print("connected...")
        with c.execute("use db100") as r:
             if not r.ok():
                raise db.TestFailed('cannot use db100')
        
        print("using db100...")

        createTypes(c)
        createEdges(c)
        createSensors(100)
        createDevices(10)
        createDvcSns()
        createMeasures(ms)

        load(c)

        # showSensors(c)

        testNoneIsNull(c)
        testCount(c, ms)
        testSensorNames(c)
        testDeviceNames(c)

        print("PASSED")
