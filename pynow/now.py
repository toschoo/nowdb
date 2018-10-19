# -------------------------------------------------------------------------
#  Basic Python NOWDB Client Interface
#
#  -----------------------------------
#  (c) Tobias Schoofs, 2018
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

from ctypes import *
from datetime import *
from calendar import *
from dateutil.tz import *

# ---- load libraries -----------------------------------------------------
libc = cdll.LoadLibrary("libc.so.6")
now = cdll.LoadLibrary("libnowdbclient.so")

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

TIMEFORMAT = "%Y-%m-%dT%H:%M:%S"
DATEFORMAT = "%Y-%m-%d"

# ---- C functions --------------------------------------------------------
_explainError = now.nowdb_err_explain
_explainError.restype = c_char_p
_explainError.argtypes = [c_long]

_connect = now.nowdb_connect
_connect.restype = c_long
_connect.argtypes = [c_void_p, c_char_p, c_char_p, c_char_p, c_char_p, c_long]

_closeCon = now.nowdb_connection_close
_closeCon.restype = c_long
_closeCon.argtypes = [c_void_p]

_exec = now.nowdb_exec_statement
_exec.restype = c_long
_exec.argtypes = [c_void_p, c_char_p, c_void_p]

_rDestroy = now.nowdb_result_destroy
_rDestroy.restype = None
_rDestroy.argtypes = [c_void_p]

_rType = now.nowdb_result_type
_rType.restype = c_long
_rType.argtypes = [c_void_p]

_rStatus = now.nowdb_result_status
_rStatus.restype = c_long
_rStatus.argtypes = [c_void_p]

_rErrCode = now.nowdb_result_errcode
_rErrCode.restype = c_long
_rErrCode.argtypes = [c_void_p]

_rDetails = now.nowdb_result_details
_rDetails.restype = c_char_p
_rDetails.argtypes = [c_void_p]

_rCurId = now.nowdb_cursor_id
_rCurId.restype = c_ulonglong
_rCurId.argtypes = [c_void_p]

_rRow = now.nowdb_cursor_row
_rRow.restype = c_void_p
_rRow.argtypes = [c_void_p]

_rCopy = now.nowdb_row_copy
_rCopy.restype = c_void_p
_rCopy.argtypes = [c_void_p]

_rFetch = now.nowdb_cursor_fetch
_rFetch.restype = c_long
_rFetch.argtypes = [c_void_p]

_rClose = now.nowdb_cursor_close
_rClose.restype = c_long
_rClose.argtypes = [c_void_p]

_rField = now.nowdb_row_field
_rField.restype = c_void_p
_rField.argtypes = [c_void_p, c_long, POINTER(c_long)]

_rNext = now.nowdb_row_next
_rNext.restype = c_long
_rNext.argtypes = [c_void_p]

_rEof = now.nowdb_cursor_eof
_rEof.restype = c_long
_rEof.argtypes = [c_void_p]

# ---- explain error
def explain(err):
    return _explainError(c_long(err))

# ---- convert datetime to nowdb time
def dt2now(dt):
    x = timegm(dt.utctimetuple())
    x *= 1000000
    x += dt.microsecond
    x *= 1000
    return x

# ---- convert nowdb time to datetime
def now2dt(p):
    t = p/1000 # to microseconds
    s = t / 1000000 # to seconds
    m = t - s * 1000000 # isolate microseconds
    dt = datetime.fromtimestamp(s, utc)
    dt += timedelta(microseconds=m)
    return dt

# ---- create a connection
def connect(addr, port, usr, pwd):
    return Connection(addr, port, usr, pwd)

# ---- a connection
class Connection:
    def __init__(self, addr, port, usr, pwd):
        con = c_void_p()
        x = _connect(byref(con), c_char_p(addr), c_char_p(port), c_char_p(usr), c_char_p(pwd), c_long(0))
        if x == 0:
            self.con = con
            self.addr = addr
            self.port = port
            self.usr  = usr
            self.pwd  = pwd
        else:
            raise ClientError(x)

    def __enter__(self):
        return self

    def __exit__(self, a, b, c):
      if self.con != None:
          # x = _closeCon(self.con)
          self.close()
          
    def close(self):
        x = _closeCon(self.con)
        if x == 0:
            self.con = None
        else:
            print "cannot close connection!"

    def execute(self, stmt):
        r = c_void_p()
        x = _exec(self.con, c_char_p(stmt), byref(r))
        if x == 0:
            return Result(r)
        else:
            raise ClientError(x)

# ---- result
class Result:
    def __init__(self, r):
        # print "result up"
        self.r = r
        self.cur = None
        self.needNext = False
        self.rw = None
        if _rType(self.r) == CURSOR:
            self.cur = _rCurId(self.r)

    def release(self):
        # print "result down"
        if self.rw != None:
            self.rw.release()

        x = 1
        if self.cur != None:
            x = _rClose(self.r)
        if x != 0:
            _rDestroy(self.r)
        self.r = None
        self.cur = None

    # cursor is a resource manager
    def __enter__(self):
       return self

    def __exit__(self, a, b, c):
        self.release()

    # cursor is an iterator
    def __iter__(self):
        if self.rType() == STATUS:
            if self.code() == EOF:
               return self
            raise DBError(self.code(), self.details())
          
        if self.cur == None:
            raise WrongType("result is not a cursor")

        self.rw = self.row()
        return self
    
    def __next__(self):
        x = self.rType()
        if x != CURSOR and x != ROW:
            raise StopIteration

        if not self.ok():
            raise StopIteration

        if self.needNext:
            if not self.rw.nextRow():
                self.rw.release()
                self.rw = None
                self.fetch()
                if not self.ok():
                    x = self.code()
                    d = self.details()

                    if x == EOF:
                       raise StopIteration

                    raise DBError(x, d)

                self.rw = self.row()
        else:
            self.needNext = True

        return self.rw

    # the python 2 syntax is bad
    def next(self):
        return self.__next__()

    # result type
    def rType(self):
        return _rType(self.r)

    # status is ok
    def ok(self):
        x = _rStatus(self.r)
        return (x == 0)

    # error code
    def code(self):
        return _rErrCode(self.r)

    # error details
    def details(self):
        return _rDetails(self.r)

    # get row from cursor
    def row(self):
        if self.rType() != CURSOR:
           raise WrongType("result is not a cursor")
        r = _rCopy(_rRow(self.r))
        if r == None:
           raise ClientError(-1)
        return Result(r)

    # next row from row
    def nextRow(self):
        t = self.rType()
        if t != CURSOR and t != ROW:
            raise WrongType("result not a cursor nor a row")
        x = _rNext(self.r)
        if x == 0:
          return True
        if x == EOF:
          return False
        raise ClientError(x)

    # field from row
    def field(self, idx):
        x = self.rType()
        if x != CURSOR and x != ROW:
            raise WrongType("result not a cursor nor a row")

        t = c_long()
        v = _rField(self.r, c_long(idx), byref(t))

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

    # fetch a bunch of rows from cursor
    def fetch(self):
        if self.rType() != CURSOR and self.rType() != ROW:
            raise WrongType("result is not a cursor")

        x = _rFetch(self.r)
        if x != 0:
            raise ClientError(x)

    # end-of-file
    def eof(self):
        x = _rEof(self.r)
        return (x != 0)

# something went wrong in the client
class ClientError(Exception):
    def __init__(self, code):
        self.code = code

    def __str__(self):
       return explain(self.code)

# something went wrong in the db
# is raised only in a for loop
class DBError(Exception):
    def __init__(self, code, details):
        self.code = code
        self.details = details

    def __str__(self):
       return str(self.code) + ": " + self.details

# type mismatch: we are at the edge
# of python dynamic typing to C static typing
class WrongType(Exception):
    def __init__(self, s):
        self.str = s

    def __str__(self):
        return self.str