# -------------------------------------------------------------------------
#  Basic Python NOWDB Stored Procedure Interface
#
#  -----------------------------------
#  (c) Tobias Schoofs, 2018
#  -----------------------------------
#  
#  This file is part of the NOWDB Stored Procedure Library.
# 
#  The NOWDB Stored Procedure Library
#  is free software; you can redistribute it
#  and/or modify it under the terms of the GNU Lesser General Public
#  License as published by the Free Software Foundation; either
#  version 2.1 of the License, or (at your option) any later version.
# 
#  The NOWDB Stored Procedure Library
#  is distributed in the hope that it will be useful,
#  but WITHOUT ANY WARRANTY; without even the implied warranty of
#  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
#  Lesser General Public License for more details.
# 
#  You should have received a copy of the GNU Lesser General Public
#  License along with the NOWDB CLIENT Library; if not, see
#  <http://www.gnu.org/licenses/>.
# -------------------------------------------------------------------------

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

utc = tzutc()

_db = None

_free = libc.free
_free.argtypes = [c_void_p]

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
_rClose.restype = c_long
_rClose.argtypes = [c_void_p]

_rRewind = now.nowdb_dbrow_rewind
_rRewind.argtypes = [c_void_p]

_rNext = now.nowdb_dbrow_next
_rNext.restype = c_long
_rNext.argtypes = [c_void_p]

_rField = now.nowdb_dbrow_field
_rField.restype = c_void_p
_rField.argtypes = [c_void_p, c_long, POINTER(c_long)]

def _setDB(db):
  global _db
  _db = db

def execute(stmt):
    global _db
    r = c_void_p()
    x = _exec(_db, c_char_p(stmt), byref(r))
    return Result(r.value)

def wrapResult(r):
  return Result(_wrap(r))

def makeError(rc, msg):
  return Result(_mkErr(c_long(rc), c_char_p(msg)))

def success():
  return Result(_success())

class Result:
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
    if self.rType() != CURSOR:
       raise WrongType("result is not a cursor")

    if _rOpen(self._r) != 0:
       print "cannot open: %d: %s" % (self.code(), self.details())
       raise StopIteration

    if _rFetch(self._r) != 0:
       print "cannot fetch: %d: %s" % (self.code(), self.details())
       raise StopIteration

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

              if x == EOF:
                 _rClose(self._r)
                 raise StopIteration

              raise DBError(x, d)

           self._needNext = False
           self._rw = self.row()

      else:
        self._needNext = True

      return self._rw

  # the python 2 syntax is bad
  def next(self):
     return self.__next__()

  def release(self):
    _rDestroy(self._r, 1)
    self._r = 0

  def rType(self):
    return _rType(self._r)

  def toDB(self):
    return self._r

  def ok(self):
    return (_rStatus(self._r) != 0)

  def eof(self):
    return (_rEof(self._r) != 0)

  def code(self):
    return _rCode(self._r)

  def details(self):
    p = _rDetails(self._r)
    if p is not None:
      s = cast(p, c_char_p)
      _free(p)
      return s
    return ""

  def fetch(self):
    return (_rFetch(self._r) == 0)

  def row(self):
    if self.rType() != CURSOR:
       raise WrongType("result is not a cursor")
    r = _rRow(self._r)
    if r == None:
      raise DBError(self.code(), self.details())
    return Result(r)

  def nextRow(self):
    return (_rNext(self._r) == 0)

  # field from row
  def field(self, idx):
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
            print "type is %d" % t.value
            return None

class DBError(Exception):
  def __init__(self, c, d):
    self._code = c
    self._details = d

  def __str__(self):
    return str(self._code) + ": " + self._details

class WrongType(Exception):
  def __init__(self, s):
    self._str = s

  def __str__(self):
    return self._str
