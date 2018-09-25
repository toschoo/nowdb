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

_rStatus = now.nowdb_dbresult_status
_rStatus.restype = c_long
_rStatus.argtypes = [c_void_p]

_rCode = now.nowdb_dbresult_errcode
_rCode.restype = c_long
_rCode.argtypes= [c_void_p]

_rDetails = now.nowdb_dbresult_details
_rDetails.restype = c_char_p
_rDetails.argtypes = [c_void_p]

_rEof = now.nowdb_dbresult_eof
_rEof.restype = c_long
_rEof.argtypes = [c_void_p]

def _setDB(db):
  global _db
  print "set DB to %d" % db
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

  def __enter__(self):
    return self

  def __exit__(self, xt, xv, tb):
    self.release()

  def release(self):
    _rDestroy(self._r, 1)
    self._r = 0

  def toDB(self):
    return self._r

  def ok(self):
    return (_rStatus(self._r) == 0)

  def eof(self):
    return (_rEof(self._r) != 0)

  def code(self):
    return _rCode(self._r)

  def details(self):
    return _rDetails(self._r)

class WrongType(Exception):
  def __init__(self, s):
    self._str = s

  def __str__(self):
    return self._str
