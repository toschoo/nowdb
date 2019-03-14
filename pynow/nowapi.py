# -------------------------------------------------------------------------
#  Python DB API for NoWDB
#
#  -----------------------------------
#  (c) Tobias Schoofs, 2019
#  -----------------------------------
#  
#  This file is part of the NOWDB CLIENT Library.
# 
#  The NOWDB CLIENT Library is free software; you can redistribute it
#  and/or modify it under the terms of the GNU Lesser General Public
#  License as published by the Free Software Foundation; either
#  version 2.1 of the License, or (at your option) any later version.
# 
#  The NOWDB CLIENT Library
#  is distributed in the hope that it will be useful,
#  but WITHOUT ANY WARRANTY; without even the implied warranty of
#  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
#  Lesser General Public License for more details.
# 
#  You should have received a copy of the GNU Lesser General Public
#  License along with the NOWDB CLIENT Library; if not, see
#  <http://www.gnu.org/licenses/>.
# -------------------------------------------------------------------------
import now
from exceptions import StandardError

# globals
apilevel = "2.0"
threadsafety = 2
paramstyle = "format"

# exceptions
class Warning(StandardError):
    def __init__(self, info):
        self.info = info

    def __str__(self):
        return self.info

class Error(StandardError):
    def __init__(self, info):
        self.info = info

    def __str__(self):
        return self.info

class InterfaceError(Error):
    def __init__(self, info):
        super(Error, self).__init__(info)
    def __str__(self):
        return super(Error, self).__str__()

class DatabaseError(Error):
    def __init__(self, info):
        super(Error, self).__init__(info)
    def __str__(self):
        return super(Error, self).__str__()

class DataError(DatabaseError):
    def __init__(self, info):
        super(DatabaseError, self).__init__(info)
    def __str__(self):
        return super(DatabaseError, self).__str__()

class OperationalError(DatabaseError):
    def __init__(self, info):
        super(DatabaseError, self).__init__(info)
    def __str__(self):
        return super(DatabaseError, self).__str__()

class IntegrityError(DatabaseError):
    def __init__(self, info):
        super(DatabaseError, self).__init__(info)
    def __str__(self):
        return super(DatabaseError, self).__str__()

class InternalError(Error):
    def __init__(self, info):
        super(DatabaseError, self).__init__(info)
    def __str__(self):
        return super(DatabaseError, self).__str__()

class ProgrammingError(Error):
    def __init__(self, info):
        super(DatabaseError, self).__init__(info)
    def __str__(self):
        return super(DatabaseError, self).__str__()

class NotSupportedError(DatabaseError):
    def __init__(self, info):
        super(DatabaseError, self).__init__(info)
    def __str__(self):
        return super(DatabaseError, self).__str__()

class Date:
    def __init__(y,m,d):
       pass

class Time:
    def __init__(h,i,s):
       pass

class Timestamp:
    def __init__(y,m,d,h,i,s):
       pass

class DateFromTicks:
    def __init__(t):
       pass

class TimeFromTicks:
    def __init__(t):
       pass

class TimestampFromTicks:
    def __init__(t):
       pass

class Binary:
    def __init__(s):
       pass

# connection
class Connection:
   def __init__(self):
       self._c = None
       pass

   def close(self):
       if not self._c is None:
          self._c.close()
          self._c = None

   def commit(self):
       pass

   def rollback(self):
       pass

   def cursor(self):
       return Cursor(self)

   def __enter__(self):
     return self

   def __exit__(self, a, b, c):
     self.close()

def connect(host, port, db, u, p):
    try:
       c = now.connect(host, port, u, p)
    except Exception as x:
       raise InterfaceError(str(x))
    if c is None:
       raise InterfaceError("no connection")
    try:
        r = c.execute("use %s" % db)
    except Exception as x:
       raise DatabaseError(str(x))
    if not r.ok():
       c.close()
       c = None
       raise InternalError("cannot use %s" %db)
    con = Connection()
    con._c = c
    return con

def whitespace(s):
    return s.strip(' \n\t')

def isWhitespace(s):
    return (s in [' ', '\n', '\t'])

def extractfields(s):
    fs = s.split(',')
    r = []
    for f in fs:
        r.append(whitespace(f))
    return r

def removefrom(s):
    i = s.find('from')
    x = s[:i]
    n = whitespace(s[i+5:])
    l = len(n)
    for i in range(l):
        if isWhitespace(n[i]):
           l=i
           break
    return (x,n[:l])

# cursor
class Cursor:
    def __init__(self, c):
        self._cur = None
        self._con = c
        self._ff = True
        self.arraysize = 1
        self.description = None
        self.rowcount = -1

    def callproc(self):
        pass

    def close(self):
        if self._cur is not None:
           self._cur.release()
           self._cur = None
           self._ff = True
           self.description = None
           self.rowcount = -1

    def getfields(self, s, n):
        try:
            r = self._con._c.execute("describe %s" % n)
        except Exception as x:
            raise DatabaseError(str(x))
        for row in r:
            self.description.append((row.field(0),0))
   
    def selparse(self, op):
        tmp = whitespace(op)
        if tmp.startswith('select'):
           (tmp2, n) = removefrom(tmp[6:])
           if whitespace(tmp2).startswith('*'):
              return self.getfields(tmp2,n)
           return extractfields(tmp2)
        return None

    def mkdesc(self, op):
        self.description = []
        fs = self.selparse(op)
        if fs is None:
           return
        for f in fs:
            self.description.append((f,0))
            
    def execute(self, op, parameters=None):
        if self._con is None:
           raise InterfaceError("no connection")
        if self._cur is not None:
           self.close()

        # parameters!

        self.mkdesc(op)
        r = None
        try:
           r = self._con._c.execute(op)
        except Exception as x:
           raise DatabaseError(str(x))
        if r is None:
           raise InternalError("no result")
        if not r.ok():
           if r.code() == now.EOF:
              r.release()
           else:
             e = r.details()
             r.release()
             raise DatabaseError(e)
        self._cur = r
        self._ff  = True

    def executemany(self, ops, seq_of_parameters=None):
        pass

    def getrow(self):
        if self._cur.rw is None:
           return None
        r = self._cur.rw
        if not r.ok():
           e = r.details()
           r.release()
           raise InerfaceError(e)
        s = {}
        for i in range(r.count()):
            f = r.field(i)
            s[self.description[i][0]] = f
            # self.description[i].type_code = r.field(i)
        return s

    def fetchone(self):
        if self._cur is None:
           raise InterfaceError("not executed")
        if self._ff:
           self._cur.__iter__()
           self._ff = False
           self.rowcount = 0
        try:
           self._cur.__next__()
        except StopIteration:
           return None
        except Exception as x:
           if self._cur.eof():
              return None
           raise InterfaceError(str(x))
        self.rowcount += 1
        return self.getrow()

    def fetchmany(self, size=1):
        r = []
        for i in range(size):
            row = self.fetchone()
            if row is None:
               break
            r.append(row)
        if len(r) == 0:
           return None
        return r

    def fetchall(self):
        r = []
        while True:
          row = self.fetchone()
          if row is None:
             break
          r.append(row)
        if len(r) == 0:
           return None
        return r

    def __enter__(self):
       return self

    def __exit__(self, a, b, c):
       self.close()

    def __iter__(self):
        return self

    def __next__(self):
        r = self.fetchone()
        if r is None:
           raise StopIteration
        return r

    def next(self):
        return self.__next__()
