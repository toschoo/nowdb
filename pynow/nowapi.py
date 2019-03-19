'''
   Python DB API for NoWDB

   -----------------------------------
   (c) Tobias Schoofs, 2019
   -----------------------------------
   
   This file is part of the NOWDB CLIENT Library.

   It provides a client API compliant to the
   PEP 249 Python Database API Specification 2.0.

   It provides in particular
   - a connection object and constructor
   - a cursor to execute sql queries against a NoWDB database server
  
   The NOWDB CLIENT Library is free software; you can redistribute it
   and/or modify it under the terms of the GNU Lesser General Public
   License as published by the Free Software Foundation; either
   version 2.1 of the License, or (at your option) any later version.
  
   The NOWDB CLIENT Library
   is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
   Lesser General Public License for more details.
  
   You should have received a copy of the GNU Lesser General Public
   License along with the NOWDB CLIENT Library; if not, see
   <http://www.gnu.org/licenses/>.
'''
import now
from exceptions import StandardError
from datetime import datetime

# globals
apilevel = "2.0"
threadsafety = 2
paramstyle = "format"

dictrow = 1
tuplerow = 2
listrow = 3

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

class InternalError(DatabaseError):
    def __init__(self, info):
        super(DatabaseError, self).__init__(info)
    def __str__(self):
        return super(DatabaseError, self).__str__()

class ProgrammingError(DatabaseError):
    def __init__(self, info):
        super(DatabaseError, self).__init__(info)
    def __str__(self):
        return super(DatabaseError, self).__str__()

class NotSupportedError(DatabaseError):
    def __init__(self, info):
        super(DatabaseError, self).__init__(info)
    def __str__(self):
        return super(DatabaseError, self).__str__()

# Type constructors
def Date(y,m,d):
    '''
    constructs a timestamp.
    '''
    return datetime(y,m,d,tzinfo=now.utc)

def Timestamp(y,m,d,h,i,s):
    '''
    constructs a timestamp.
    '''
    return datetime(y,m,d,h,i,s,now.utc)

def Time(h,i,s):
    raise NotSupportedError("Time is not supported; use Timestamp instead")

def DateFromTicks(t):
    raise NotSupportedError("fromTicks is not supported")

def TimeFromTicks(t):
    raise NotSupportedError("fromTicks is not supported")

def TimestampFromTicks(t):
    raise NotSupportedError("fromTicks is not supported")

def Binary(s):
    raise NotSupportedError("Binary is not supported")

class Connection:
   '''
   The Connection class provides access to a NoWDB server.
   Connection is a resource manager and can be used
   in a 'with' statement, e.g.:
      with nowapi.connect('localhost', '50677', 'user', 'mypwd', 'mydb') as c:
           ...
   close() is called on leaving the scope of the with statment.
   
   A connection can be shared between threads.
   '''
   def __init__(self):
       self._c = None
       pass

   def close(self):
       '''
       Closes the connection to the database.
       '''
       if not self._c is None:
          self._c.close()
          self._c = None

   def commit(self):
       pass

   def rollback(self):
       pass

   def cursor(self):
       '''
       Ceates a cursor.
       '''
       return Cursor(self)

   def execute(self, stmt, parameters=None, rowformat=dictrow):
       '''
       This is a convenience interface (not foreseen in the DB API)
       that creates a cursor, executes the statement on it and
       returns it. It allows to write more concise code of the form:
          for row in con.execute('select * from mytab'):
              ...
       Parameters:
       - stmt      : an SQL string
       - parameters: parameters for that string
       - rowformat : the rowformat for the cursor
       '''
       cur = self.cursor()
       cur.setRowFormat(rowformat)
       cur.execute(stmt, parameters)
       return cur

   def __enter__(self):
     return self

   def __exit__(self, a, b, c):
     self.close()

def connect(host, port, u, p, db=None):
    '''
    Connection constructor; it expects
    - a host (e.g. an IP address, a hostname or 'localhost')
    - a port number
    - a database to use
    - a username
    - a password.
    
    The database may be None.
    In that case, a database may be dynamically selected
    using the SQL command 'use'.
    '''
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

def convert(t, v):
    if t != now.TIME and t != now.DATE:
       return v
    else:
       return now.now2dt(v)

def addpars(op, ps):
    if ps is None:
       return op
    l = []
    for p in ps:
        if type(p) == datetime:
           l.append(now.dt2now(p))
        else:
           l.append(p)
    return (op % tuple(i for i in l))

# cursor
class Cursor:
    '''
    Cursor objects provide a means to execute statements
    against a database server.
    They are created from a connection object using
    the 'cursor()' method.

    If the Cursor is executed with a select statement,
    results can be fetched from the database.
    There three fetch methods:
    - fetchone, which obtains one row from the resultset
    - fetchmany, which obtains n rows from the resultset
    - fetchall, which obtains all rows from the resultset

    Note that there is no performance benefit in using
    fetchmany instead of fetchone.
    The client/server protocol already optimises
    the fetching policy internally. fetchone, therefore,
    performs a call to the server only when there are no more
    rows locally available.

    Cursors allocate local resources
    and resources in the database server.
    To avoid local memory leaks and to go gentle on
    server resources, the cursor should be closed,
    when the resultset is not needed any more.

    Cursor is a resource manager that can be used in a
    'with' statment, e.g.:
         with cursor.execute("select * from mytable") as cur:
              ...
    The cursor is automatically closed on leaving the scope
    of the with statement.

    Cursor is an iterator that can be used
    with an 'in' statement, e.g.:
         for row in cursor:
             ...
    '''
    def __init__(self, c):
        self._cur = None
        self._con = c
        self._ff = True
        self.arraysize = 1
        self.description = None
        self.rowcount = -1
        self.rowformat = dictrow

    def callproc(self):
        pass

    def close(self):
        '''
        closes the cursor. Note that a cursor on execution
        allocates local and server resources.
        It should therefore be closed as soon as the resultset
        is not needed anymore.
        '''
        if self._cur is not None:
           # print("CLOSE")
           self._cur.release()
           self._cur = None
           self._ff = True
           self.description = None
           self.rowcount = -1

    def setRowFormat(self, rowtype):
        '''
        sets the rowformat. Valid options are
        - dictrow: rows are presented as dictionary fieldname -> value
        - tuplerow: rows are presented as tuples (fieldname, value)
        Default: dictrow.
        The dictrow is more convenient, since it allows to refer to values as
            row['fieldname']
        In some cases, when constants or functions are used,
        fieldnames are awkward, e.g.
            row['round(sum(price)/count(*))']
        In such cases it is more appropriate to refer to the field as
            row[1]
        '''
        if rowtype not in [dictrow,tuplerow,listrow]:
           raise InterfaceError("unknown row format: %s" % rowtype)
        self.rowformat = rowtype

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
        '''
        executes an sql statement.
        '''
        if self._con is None:
           raise InterfaceError("no connection")
        if self._cur is not None:
           self.close()

        opp = addpars(op, parameters)

        self.mkdesc(opp)
        r = None

        try:
           r = self._con._c.execute(opp)
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
        raise NotSupportedError("executemany is not supported")

    def getrow(self):
        if self._cur is None:
           return None
        if self._cur.rw is None:
           return None
        r = self._cur.rw
        if not r.ok():
           e = r.details()
           r.release()
           raise InerfaceError(e)

        if self.rowformat == dictrow:
           s = {}
        else:
           s = []
        
        for i in range(r.count()):
            (t,f) = r.typedField(i)
            if self.rowformat == dictrow:
               s[self.description[i][0]] = convert(t,f)
            else:
               s.append(convert(t,f))

        if self.rowformat != tuplerow:
           return s
        else:
           return tuple(s)

    def fetchone(self):
        '''
        fetches one row from the resultset.
        Note that a fetchone does not necessarily
        implies an interaction with the server.
        Rows are retrieved from the server in
        batches and kept locally; only when the
        local batch of rows is exhaused, fetchone
        sends a fetch request to the server to
        obtain the next batch.
        '''
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
        '''
        fetches n rows from the resultset (n>=1, default: 1).
        Note that fetchmany does not necessarily has performance
        advantages over fetchone (see docstring on fetchone).
        '''
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
        '''
        fetches all rows from the resultset.
        '''
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
           self.close()
           raise StopIteration
        return r

    def next(self):
        return self.__next__()

    def __del__(self):
        self.close()
