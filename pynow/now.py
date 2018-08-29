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

# ---- C functions --------------------------------------------------------
_explainError = now.nowdb_err_explain
_explainError.restype = c_char_p
_explainError.argtypes = [c_long]

_connect = now.nowdb_connect
_connect.restype = c_long
_connect.argtypes = [c_void_p, c_char_p, c_short, c_char_p, c_char_p, c_long]

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

_rRow = now.nowdb_cursor_row
_rRow.restype = c_void_p
_rRow.argtypes = [c_void_p]

_rCopy = now.nowdb_row_copy
_rCopy.restype = c_void_p
_rCopy.argtypes = [c_void_p]

_rFetch = now.nowdb_cursor_fetch
_rFetch.restype = c_long
_rFetch.argtypes = [c_void_p]

_rField = now.nowdb_row_field
_rField.restype = c_void_p
_rField.argtypes = [c_void_p, c_long, POINTER(c_long)]

_rNext = now.nowdb_row_next
_rNext.restype = c_long
_rNext.argtypes = [c_void_p]

_rEof = now.nowdb_cursor_eof
_rEof.restype = c_long
_rEof.argtypes = [c_void_p]

# ---- create a connection
def connect(addr, port, usr, pwd):
    con = c_void_p()
    x = _connect(byref(con), c_char_p(addr), c_short(port), c_char_p(usr), c_char_p(pwd), c_long(0))
    if x == 0:
        return (0,Connection(con))
    else:
        return (x,None)

# ---- a connection
class Connection:
    def __init__(self, con):
        self.con = con

    def close(self):
        return _closeCon(self.con)

    def execute(self, stmt):
        r = c_void_p()
        x = _exec(self.con, c_char_p(stmt), byref(r))
        return Result(r)

# ---- result
class Result:
    def __init__(self, r):
        self.r = r

    def release(self):
        _rDestroy(self.r)
        self.r = None

    def rType(self):
        return _rType(self.r)

    def ok(self):
        x = _rStatus(self.r)
        return (x == 0)

    def code(self):
        x = _rErrCode(self.r)
        return x

    def details(self):
        s = _rDetails(self.r)
        return s

    def row(self):
        if self.rType() != CURSOR:
           return None
        r = _rCopy(_rRow(self.r))
        if r == None:
           return None
        return Result(r)

    def next(self):
        t = self.rType()
        if t != CURSOR and t != ROW:
            return False
        x = _rNext(self.r)
        return (x == 0)

    def field(self, idx):
        x = self.rType()
        if x != CURSOR and x != ROW:
            return None

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

        else:
            print "%s != %s != %s\n" % (t, c_long(TEXT), t.value)
            return None

    def fetch(self):
        if self.rType() != CURSOR and self.rType() != ROW:
            return False
        x = _rFetch(self.r)
        return (x == 0)

    def eof(self):
        x = _rEof(self.r)
        return (x != 0)
