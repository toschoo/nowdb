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

def _setDB(db):
  global _db
  print "set DB to %d" % db
  _db = db

def execute(stmt):
    global _db
    print "%s" % _db
    r = c_void_p()
    x = _exec(_db, c_char_p(stmt), byref(r))
    print "EXEC: %d" % x
    print "RESULT: %s" % r
    return x

