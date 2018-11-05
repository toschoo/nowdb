#!/usr/bin/env python

import sys
import os

BASE = "./todo"

def helptxt(p):
    print "%s <cmd> <ops>" % p
    print "cmds: "
    print "    show: "

def pending(b):
    return b + "/pending"

def done(b):
    return b + "/done"

def progress(b):
    return b + "/progress"

def path(p, f):
    return p + "/" + f

def label(s):
    return s[2:len(s)-4]

def delabel(s):
    return "id" + s + ".txt"

def getDir(b,s):
    if s == "pending":
       return pending(b)
    if s == "progress":
       return progress(b)
    if s == "done":
       return done(b)

def isorter(i):
    return int(label(i))

def updid(base):
    idf = path(base, "id")
    with open(idf,"r") as f:
       tmp=f.readline()
       i = int(tmp[:len(tmp)-1])
       print "%d" % i
       i+=1
    with open(idf,"w") as f:
       f.write(str(i)+"\n")
    return i

def prompath(path):
    if path == "idea":
       return "pending"
    if path == "pending":
       return "progress"
    if path == "progress":
       return "done"
    return None

def show(base, what="pending"):
    print what
    p = getDir(base, what)
    d = sorted(os.listdir(p),key=isorter)
    for i in d:
      with open(path(p,i), "r") as f:
         l = f.readline()
         print "%05s: %s" % (label(i),l[:len(l)-1])

def item(base, what, itm):
    p = path(path(base, what), delabel(itm))
    with open(p,"r") as f:
       for l in f:
          print l[:len(l)-1]

def create(base, topic):
    i = updid(base)
    fn = "id" + str(i) + ".txt"
    p = path(pending(base), fn)
    with open(p, "w") as f:
       f.write(topic + "\n")
    cmd = "vim %s" % p
    print cmd
    os.system(cmd)

def edit(base, what, itm):
    p = path(path(base, what), delabel(itm))
    cmd = "vim %s" % p
    os.system(cmd)

def prom(base, what, itm):
    src = path(path(base, what), delabel(itm))
    trg = path(base,prompath(what))
    if trg is None:
       print "cannot promote that item"
       return
    cmd = "mv %s %s" % (src, trg)
    os.system(cmd)

if __name__ == "__main__":
    if len(sys.argv) < 2:
       print "I need a command"
       helptxt(sys.argv[0])
       exit(1)

    if sys.argv[1] == "show"     or \
       sys.argv[1] == "idea"     or \
       sys.argv[1] == "pending"  or \
       sys.argv[1] == "progress" or \
       sys.argv[1] == "done"     or \
       sys.argv[1] == "parked":
         if sys.argv[1] == "show":
            what = "pending"
            if len(sys.argv) > 2:
               what = sys.argv[2]
         else:
            what = sys.argv[1]

         show(BASE,what)

    if sys.argv[1] == "item":
       if len(sys.argv) < 4:
          print "Tell me where the item is and which item you want to see"
          exit(1)
       what = sys.argv[2]
       itm = sys.argv[3]
       item(BASE, what, itm)

    if sys.argv[1] == "create":
       if len(sys.argv) < 3:
          print "Tell me the topic"
          exit(1)
       topic = sys.argv[2]
       create(BASE, topic)

    if sys.argv[1] == "edit":
       if len(sys.argv) < 4:
          print "Tell me where the item is and which item you want to edit"
          exit(1)
       what = sys.argv[2]
       itm = sys.argv[3]
       edit(BASE, what, itm)

    if sys.argv[1] == "promote":
       if len(sys.argv) < 4:
          print "Tell me where the item is and which item you want to promote"
          exit(1)
       what = sys.argv[2]
       itm = sys.argv[3]
       prom(BASE, what, itm)
