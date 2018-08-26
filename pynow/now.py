from ctypes import *

libc = cdll.LoadLibrary("libc.so.6")
now = cdll.LoadLibrary("libnowdbclient.so")

explainError = now.nowdb_err_explain
explainError.restype = c_char_p

_connect = now.nowdb_connect
_connect.restype = c_long

_closeCon = now.nowdb_connection_close
_closeCon.restype = c_long

def connect(addr, port, usr, pwd):
    con = c_ulong()
    x = _connect(byref(con), c_char_p(addr), c_short(port), c_char_p(usr), c_char_p(pwd), c_long(0))
    if x == 0: # c_long(0):
        return Connection(con)
    else:
        return None

class Connection:
    def __init__(self, con):
        self.con = con

    def close(self):
        return _closeCon(self.con)
