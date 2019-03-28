'''
   Basic Python NOWDB Stored Procedure Interface
 
   -----------------------------------
   (c) Tobias Schoofs, 2018
   -----------------------------------
   
   This file is part of the NOWDB Stored Procedure Library.

   It provides in particular
   - an execute function
   - a polymorphic result type
   - an iterable cursor and row result type
  
   The NOWDB Stored Procedure Library
   is free software; you can redistribute it
   and/or modify it under the terms of the GNU Lesser General Public
   License as published by the Free Software Foundation; either
   version 2.1 of the License, or (at your option) any later version.
  
   The NOWDB Stored Procedure Library
   is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
   Lesser General Public License for more details.
  
   You should have received a copy of the GNU Lesser General Public
   License along with the NOWDB CLIENT Library; if not, see
   <http://www.gnu.org/licenses/>.
'''

from ctypes import *
from datetime import *
from calendar import *
from dateutil.tz import *

# ---- load libraries -----------------------------------------------------
libc = cdll.LoadLibrary("libc.so.6")
now = cdll.LoadLibrary("libnowdb.so")

# ---- result types -------------------------------------------------------
NOTHING = 0
STATUS = 0x21
REPORT = 0x22
ROW = 0x23
CURSOR = 0x24

# ---- value types --------------------------------------------------------
TEXT = 1
DATE = 2
TIME = 3
FLOAT = 4
INT = 5
UINT = 6
BOOL = 9

EOF = 8

USRERR = 73

utc = tzutc()

TIMEFORMAT = "%Y-%m-%dT%H:%M:%S.%f"
DATEFORMAT = "%Y-%m-%d"

_db = None

_free = libc.free
_free.argtypes = [c_void_p]

_strlen = libc.strlen
_strlen.argtypes = [c_char_p]
_strlen.restype = c_size_t

_exec = now.nowdb_dbexec_statement
_exec.restype = c_long
_exec.argtypes = [c_void_p, c_char_p, c_void_p]

_wrap = now.nowdb_dbresult_wrap
_wrap.restype = c_void_p
_wrap.argtypes = [c_void_p]

_success = now.nowdb_dbresult_success
_success.restype = c_void_p
_success.argtypes = []

_mkErr = now.nowdb_dbresult_makeError
_mkErr.restype = c_void_p
_mkErr.argtypes = [c_long, c_char_p]

_mkRow = now.nowdb_dbresult_makeRow
_mkRow.restype = c_void_p
_mkRow.argtypes = []

_rDestroy = now.nowdb_dbresult_destroy
_rDestroy.argtypes = [c_void_p, c_long]

_rType = now.nowdb_dbresult_type
_rType.restype = c_long
_rType.argtypes = [c_void_p]

_rStatus = now.nowdb_dbresult_status
_rStatus.restype = c_long
_rStatus.argtypes = [c_void_p]

_rCode = now.nowdb_dbresult_errcode
_rCode.restype = c_long
_rCode.argtypes= [c_void_p]

_rDetails = now.nowdb_dbresult_details
_rDetails.restype = c_void_p
_rDetails.argtypes = [c_void_p]

_rEof = now.nowdb_dbresult_eof
_rEof.restype = c_long
_rEof.argtypes = [c_void_p]

_rRow = now.nowdb_dbcur_row
_rRow.restype = c_void_p
_rRow.argtypes = [c_void_p]

_rFetch = now.nowdb_dbcur_fetch
_rFetch.restype = c_long
_rFetch.argtypes = [c_void_p]

_rOpen  = now.nowdb_dbcur_open
_rOpen.restype = c_long
_rOpen.argtypes = [c_void_p]

_rClose = now.nowdb_dbcur_close
_rClose.argtypes = [c_void_p]

_rRewind = now.nowdb_dbrow_rewind
_rRewind.argtypes = [c_void_p]

_rNext = now.nowdb_dbrow_next
_rNext.restype = c_long
_rNext.argtypes = [c_void_p]

_rOff = now.nowdb_dbrow_off
_rOff.restype = c_long
_rOff.argtypes = [c_void_p]

_rCount = now.nowdb_dbrow_count
_rCount.restype = c_long
_rCount.argtypes = [c_void_p]

_rField = now.nowdb_dbrow_field
_rField.restype = c_void_p
_rField.argtypes = [c_void_p, c_long, POINTER(c_long)]

_rAdd = now.nowdb_dbresult_add2Row
_rAdd.restype = c_long
_rAdd.argtypes = [c_void_p, c_byte, c_void_p]

_rCap = now.nowdb_dbresult_rowCapacity
_rCap.restype = c_long
_rCap.argtypes = [c_void_p]

_rCloseRow = now.nowdb_dbresult_closeRow
_rCloseRow.restype = c_long
_rCloseRow.argtypes = [c_void_p]

# --- set db object to the current db
def _setDB(db):
  global _db
  _db = db

def dt2now(dt):
    '''
    Convert an aware Python datetime object
    reflecting UTC time to nowdb time.
    '''
    x = timegm(dt.utctimetuple())
    x *= 1000000
    x += dt.microsecond
    x *= 1000
    return x

# ---- convert nowdb time to datetime
def now2dt(p):
    '''
    Convert nowdb time to an aware Python datetime object
    reflecting UTC time.
    '''
    t = p/1000 # to microseconds
    s = t / 1000000 # to seconds
    m = t - s * 1000000 # isolate microseconds
    dt = datetime.fromtimestamp(s, utc)
    dt += timedelta(microseconds=m)
    return dt

def execute(stmt):
    '''
    execute the sql statement.
    The function returns a polymorphic 'result'
    (see below).
    '''
    global _db
    r = c_void_p()
    x = _exec(_db, c_char_p(stmt), byref(r))
    return Result(r.value)

def wrapResult(r):
  return Result(_wrap(r))

def makeError(rc, msg):
  '''
  make an error result using the string 'msg'.
  '''
  return Result(_mkErr(c_long(rc), c_char_p(msg)))

def success():
  '''
  make a status result 'OK'.
  '''
  return Result(_success())

def makeRow():
  '''
  make a empty row result.
  '''
  return Result(_mkRow())

def convert(t, value):
  '''
  Convert a Python value to a nowdb value
  of type 't'.
  '''
  if t == TEXT: 
    return c_char_p(value)
  elif t == DATE:
    return c_longlong(value)
  elif t == TIME:
    return c_longlong(value)
  elif t == FLOAT:
    return c_double(value)
  elif t == INT:
    return c_longlong(value)
  elif t == UINT:
    return c_ulonglong(value)
  elif t == BOOL:
    return c_longlong(value)
  raise WrongType("unknown value")

class Result:
  '''
  Result is a polymorphic datatype representing the result
  of a return statement. It can in particular be
  - a status (ok or not ok)
  - a report (rows affected)
  - a cursor (an iterator) or
  - a row.

  Result implements the resource manager and can
  be used with the 'with' statement. On leaving the scope
  of the statement, 'release' is called on the result, e.g.:

    with nowdb.execute('select * from mytable') as res:
         # do something

  Results correspond to resources in the server process    
  and shall therefore be released when they are not needed
  anymore. Otherwise, the resources remain pending until the result
  is collected by the Python GC (which calls the result's
  __del__ method).

  If the result is a cursor, it is also an iterator,
  which can be iterated by means of the 'in' construction, e.g.:

    with con.execute('select * from mytable') as cur:
         for row in cur:
             # do something with the row

  '''
  def __init__(self, r):
    self._r = r
    self._rw = None
    self._needNext = False

  def __enter__(self):
    return self

  def __exit__(self, xt, xv, tb):
    self.release()

  # cursor is an iterator
  def __iter__(self):
    if self.rType() == STATUS:
       if self.code() == 0:
          return self
       if self.code() == EOF:
          return self
       raise DBError(self.code(), self.details())

    if self.rType() != CURSOR:
       raise WrongType("result is not a cursor")

    if _rOpen(self._r) != 0:
       if not self.ok():
          return self

    if _rFetch(self._r) != 0:
       if not self.ok():
          return self

    self._rw = self.row()
    self._needNext = False
    return self

  def __next__(self):
      x = self.rType()
      if x != CURSOR and x != ROW:
        raise StopIteration

      if not self.ok():
        raise StopIteration

      if self._needNext:
        if not self._rw.nextRow():
           self._rw.release()
           self._rw = None
           if not self.fetch() or not self.ok():
              x = self.code()
              d = self.details()

              _rClose(self._r)
              self._r = None

              if x == EOF:
                 raise StopIteration

              raise DBError(x, d)

           self._rw = self.row()

      else:
        self._needNext = True

      # print "returning %d" % _rOff(self._rw._r)
      return self._rw

  # the python 2 syntax is bad
  def next(self):
     return self.__next__()

  def release(self):
    '''
    releases the corresponding resources in the server process. 
    When used with a 'with' statement,
    release() is called internally; otherwise it is called
    by the Python GC through the __del__ method.
    '''
    if self._rw is not None:
       self._rw.release()
       self._rw = None
    if self._r is None:
       return
    if self.rType() == CURSOR:
       _rClose(self._r)
    else:
       _rDestroy(self._r, 1)
    self._r = None 

  def __del__(self):
    self.release()

  def rType(self):
    return _rType(self._r)

  # --- extract C object and prepare for GC
  def _toDB(self):
    r = self._r
    self._r = None
    self._rw = None # is this safe?
    return r

  def ok(self):
    '''
    the result reflects an erroneous status.
    '''
    return (_rStatus(self._r) != 0)

  def eof(self):
    '''
    the cursor reached eof.
    '''
    return (_rEof(self._r) != 0)

  def code(self):
    '''
    returns the error code (if any).
    '''
    return _rCode(self._r)

  def details(self):
    '''
    returns a text message with details on the error (if any).
    '''
    p = _rDetails(self._r)
    if p is not None:
      x = cast(p, c_char_p)
      if x is None:
          _free(p)
          return "cannot obtain error message"
      s = x.value
      _free(p)
      return s
    return "cannot obtain error message"

  def fetch(self):
    '''
    fetches the next (bunch of) rows from the cursor.
    '''
    return (_rFetch(self._r) == 0)

  def add2Row(self, t, value):
    '''
    add the Python variable 'value' to the row as nowdb type 't'.
    If there is not enough room in the row to store the value,
    an exception is raised.
    '''
    if self._r is None:
      return False
    if not self.fitsRow(t, value):
       raise DBError(-107, "row is full") 
    v = convert(t, value)
    if _rAdd(self._r, t, byref(v)) != 0:
       raise DBError(-107, "cannot add to row") 

  def fitsRow(self, t, value):
    '''
    checks if 'value' of type 't' still fits into the row.
    '''
    if self._r is None:
      return False
    c = _rCap(self._r)
    if c <= 0:
       return False
    if t == BOOL or t == NOTHING:
       if c < 3:
          return False
    elif t == TEXT:
       if c <= libc.strlen(c_char_p(value)) + 3:
          return False
    elif c < 10: 
       return False
    return True

  def closeRow(self):
    '''
    adds the 'end-of-row' marker.
    '''
    if self._r is None:
      return False
    return (_rCloseRow(self._r)== 0)

  def row(self):
    '''
    returns the row(s) from a cursor.
    Cursors may hold a bunch of rows.
    This method copies the rows stored in the cursor
    and returns it to the user.
    Note that a row is a result and must be released.
    '''
    if self.rType() != CURSOR:
       raise WrongType("result is not a cursor")
    r = _rRow(self._r)
    if r == None:
      raise DBError(self.code(), self.details())
    return Result(r)

  def nextRow(self):
    '''
    advances to the next row.
    If there was at least one more row,
    the method returns True, otherwise False.
    '''
    return (_rNext(self._r) == 0)

  def count(self):
    '''
    counts the number of fields in the row.
    '''
    x = self.rType()
    if x != ROW:
       raise WrongType("result not a row")

    if self._r is None:
       return 0

    return int(_rCount(self._r))

  # field with type information
  def typedField(self, idx):
    '''
    returns the tuple (type, value) of the idxth field
    of the current row (starting to count from 0).
    Example:
      with con.execute('select * from mytable') as cur:
           for row cur:
               (t,v) = row.field(0)
               print("value %s is of type %d" % (t,v))
        
    '''
    x = self.rType()
    if x != CURSOR and x != ROW:
       raise WrongType("result not a cursor nor a row")

    t = c_long()
    v = _rField(self.r, c_long(idx), byref(t))

    if t.value == TEXT:
       s = cast(v, c_char_p)
       return (t.value, s.value)

    elif t.value == TIME or t.value == DATE or t.value == INT:
       i = cast(v,POINTER(c_longlong))
       return (t.value, i[0])

    elif t.value == UINT:
       u = cast(v,POINTER(c_ulonglong))
       return (t.value, u[0])

    elif t.value == FLOAT:
       f = cast(v,POINTER(c_double))
       return (t.value, f[0])

    elif t.value == BOOL:
       f = cast(v,POINTER(c_byte))
       if f[0] == 0:
          return (t.value, False)
       return (t.value, True)

    else:
       return (NOTHING, None)

  def field(self, idx):
        '''
        Same as typedField, but without type information.
        '''
        x = self.rType()
        if x != CURSOR and x != ROW:
            raise WrongType("result not a cursor nor a row")

        t = c_long()
        v = _rField(self._r, c_long(idx), byref(t))

        if t.value == TEXT:
            s = cast(v, c_char_p)
            return s.value

        elif t.value == TIME or t.value == DATE or t.value == INT:
            i = cast(v,POINTER(c_longlong))
            return i[0]

        elif t.value == UINT:
            u = cast(v,POINTER(c_ulonglong))
            return u[0]

        elif t.value == FLOAT:
            f = cast(v,POINTER(c_double))
            return f[0]

        elif t.value == BOOL:
            f = cast(v,POINTER(c_byte))
            if f[0] == 0:
                return False
            return True

        else:
            return None

# the entry point
def _caller(f,t):
    try:
       if t is None:
          r = f()
       else:
          r = f(*t)
       if r is None:
            return success()._toDB()
       else:
            return r._toDB()
    except Exception as x:
       return makeError(str(x))._toDB() 

class DBError(Exception):
  '''
  Exception indicating that something went wrong in the server.
  '''
  def __init__(self, c, d):
    self._code = c
    self._details = d

  def __str__(self):
    return str(self._code) + ": " + self._details

class WrongType(Exception):
  '''
  Exception indicating that an invalid method was called
  on a result, e.g. fetch on a result that is not a cursor.
  '''
  def __init__(self, s):
    self._str = s

  def __str__(self):
    return self._str
