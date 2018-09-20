import random

def hello():
  print "hello nowdb!"
  return (1,None)

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
    
