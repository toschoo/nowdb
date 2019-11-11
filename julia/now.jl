"""
   Basic Julia NOWDB Client Interface
 
   -----------------------------------
   (c) Tobias Schoofs, 2018 - 2020
   
   This file is part of the NOWDB CLIENT Library.

   It provides in particular
   - a Connection Type
   - functions to execute SQL statements on connections
   - Cursors, Rows and other Result types
   - Function to convert Date, Time and DateTime objects
     to NoWDB time stamps and vice versa.
  
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
"""
module NoW

export NoConnectionError, ClientError, DBError,
       InvalidAliasError, InvalidStatementError,
       TEXT, DATE, TIME, FLOAT, INT, UINT, BOOL,
       datetime2now, now2datetime, now2datetimens,
       now2date, now2time, now2datetimepair, now,
       connect, close, reconnect, withconnection,
       execute, fill, asarray, onerow, onevalue,
       tfield, field, fieldcount, release

using Dates, DataFrames, DataFramesMeta

const lib = "libnowdbclient"

"""
       NoConnectionError()

       connection object has no active connection to the database.
"""
struct NoConnectionError <: Exception end 

struct WrongTypeError    <: Exception end

struct InvalidAliasError <: Exception
  msg::String
end

struct InvalidStatementError <: Exception
  msg::String
end

"""
       NothingError()

       a function expected to return a valid result returned nothing.
"""
struct NothingError      <: Exception end
"""
       ClientError(code, msg)

       Error in the client library.
"""
struct ClientError       <: Exception
  code::Int
  msg::String
end
"""
       DBError(code, msg)

       Error in the database.
"""
struct DBError           <: Exception
  code::Int
  msg::String
end

# C types
const ConT = Culonglong
const CursorT = Culonglong
const ResultT = Culonglong
const NowValue = Culonglong

# Error constants
const OK      =  0
const INVALID = -6
const EOF     =  8
const NOMEM = 1
const TOOBIG = 5
const KEYNOF = 26
const DUPKEY = 27
const TIMEOUT = 36
const USRERR  =  74
const SELFLOCK =  75 
const DEADLOCK =  76
const NOTMYLOCK =  77

# Result types
const NOTHING = 0
const STATUS  = 33
const REPORT  = 34
const ROW     = 35
const CURSOR  = 36

# Data Types
const TEXT = 1
const DATE = 2
const TIME = 3
const FLOAT = 4
const INT = 5
const UINT = 6
const BOOL = 9

const NPERSEC = 1000000000

# Initialize the C library
function __init__()
  _libinit()
end

# convert now time datetime via unix
function now2unix(t::Int64)
  f = Float64(t)/NPERSEC
  Dates.unix2datetime(f)
end

# get milliseconds, microseconds and nanoseconds
function belowsec(t::Int64)
  s = t÷NPERSEC
  xs = t - s*NPERSEC
  ys = xs÷1000
  ms = ys÷1000
  ns = xs - 1000ys
  us = ys - 1000ms
  (ms, us, ns)
end

"""
    datetime2now(dt; us=0, ns=0)

    Convert Dates.DateTime object (dt) to NoW timestamp.
    Use us (microsecond) and ns (nanosecond) for precision
    below millisecond level.
"""
function datetime2now(t::Dates.DateTime; us=0, ns=0)
  f = Dates.datetime2unix(t)
  n = Int64(1000f)
  return 1000000n + 1000us + ns
end

"""
    now2datetime(t::Int64)

    Convert NoW timestamp to Dates.DateTime object.
"""
function now2datetime(t::Int64)
  return now2unix(t)
end

"""
    now2datetimens(t::Int64)

    Convert NoW timestamp to Dates.DateTime object
    and additional microcsecond and nanosecond precision as
    triple (dt, us, ns)
"""
function now2datetimens(t::Int64)
  _, us, ns = belowsec(t)
  (now2unix(t), us, ns)
end

"""
    now2date(t::Int64)

    Convert NoW timestamp to Dates.Date object.
"""
function now2date(t::Int64)
  Dates.Date(now2unix(t))
end

"""
    now2time(t::Int64)

    Convert NoW timestamp to Dates.Time object.
"""
function now2time(t::Int64)
  nix = now2unix(t)
  ms, us, ns = belowsec(t)
  Dates.Time(Dates.hour(nix),
             Dates.minute(nix),
             Dates.second(nix),
             ms, us, ns)
end

"""
    now2datetimepair(t::Int64)

    Convert NoW timestamp to a pair of
    (Date.Date, Dates.Time).
"""
function now2datetimepair(t::Int64)
  (now2date(t), now2time(t))
end

mutable struct Connection
  _con::ConT
  _addr::String
  _port::String
  _usr::String
  _pwd::String
  _db::String
  function Connection(c, addr, port, usr, pwd)
    me = new(c, addr, port, usr, pwd, "")
    finalizer(close, me)
    return me
  end
end

function _connect(srv::String, port::String, usr::String, pwd::String)
  c::Ref{ConT} = 0
  x = ccall(("nowdb_connect", lib), Cint,
            (Ref{ConT},
             Cstring,
             Cstring,
             Cstring,
             Cstring,
             Cint), c, srv, port, usr, pwd, 0)
  if x != 0
     # INVALID
     if x == INVALID
        throw(ArgumentError("check arguments"))
     # NOMEM
     elseif x == NOMEM
        throw(OutOfMemoryError("on connect"))
     else
     # ADDR
     # unknown
        throw(ClientError("$x"))
     end
  end
  return c
end

"""
    connect(srv::String, port::String, usr::String, pwd::String)

    Create a connection to the database server identified by
    the host (name or ip address) and
    the service (service name or port number)
    for the indicated user authenticated by the given password.

    On success, return a Connection object;
    Otherwise, throw an exception.

    The connection will be closed and all resources freed
    when the connection object is collected by the GC.
    However, it is good style to close the connection
    explicitly as soon as it is not needed anymore.
    Since we do not know, when the GC will run,
    not closing unused connections may lead to a waste
    of resource both in the client and the database server.

# Examples
```
julia> con = NoW.connect("localhost", "50677", "foo", "bar")
Main.NoW.Connection(0x00000000023f6310, "localhost", "50677", "foo", "bar", "")
```

# Related
  close, reconnect, withconnection
"""
function connect(srv::String, port::String, usr::String, pwd::String)
  c = _connect(srv, port, usr, pwd)
  Connection(c[], srv, port, usr, pwd)
end

"""
    close(con::Connection)

    Close the indicated connection to the database server and
    free all resources used by this connection.

# Related
  connect, reconnect, withconnection
"""
function close(con::Connection)
  if con._con == 0 return end
  x = ccall(("nowdb_connection_close", lib), Cint, (ConT,), con._con)
  if x != 0
      ccall(("nowdb_connection_destroy", lib), Cvoid, (ConT,), con._con)
  end
  con._con = 0
end

"""
    reconnect(con::Connection)

    Try to reconnect a Connection object that lost its connection
    to the datbase.

# Related
  connect, withconnection, close
"""
function reconnect(con::Connection)
  c = _connect(con._srv, con._port, con._usr, con._pwd)
  con._con = c[]
  if con._db != ""
     execute(con, "use $db"); con._db = db
  end
end

"""
    withconnect(srv::String, port::String,
                usr::String, pwd::String,
                db::String, f::Function)

    Connect to the database; on success, apply function 'f' on the result
    and, finally, close the connection.
    Return the result of function 'f'.

# Examples
```
julia> NoW.withconnection("localhost", "50677", "foo", "bar", "mydb", con -> begin
         for row in NoW.execute(con, "select * from mytable")
           # here we do something
         end
       end)
```

# Related
  connect, reconnect, close
"""
function withconnection(f::Function, srv::String, port::String,
                        usr::String, pwd::String, db::String)
  con = connect(srv, port, usr, pwd)
  try
     if db != ""
        execute(con, "use $db"); con._db = db
     end
     return f(con)
  finally
     close(con)
  end
end

mutable struct Result
   _res::ResultT
   _type::Int
   _cur::CursorT
   _ctype::Int8
   _fcount::Int64
   function Result(res, t, cur, ct)
     me = new(res, t, cur, ct, -1)
     finalizer(release, me)
     return me
   end
end

function _execute(con::Connection, stmt::String, ctype::Int8)
  con._con != 0 || throw(ArgumentError("no connection"))
  r::Ref{ResultT} = 0
  x = ccall(("nowdb_exec_statement", lib), Cint,
            (ConT, Cstring, Ref{ResultT}),
            con._con, stmt, r)
  if x != 0 throw(ClientError(rc, "")) end
  t = _resulttype(r[])
  cid = t == CURSOR ? _curid(r[]) : 0
  if !_ok(r[])
    rc = _errcode(r[])
    msg = _errmsg(r[])
    _release(r[])
    throw(DBError(rc, msg))
  end
  if t == STATUS
    _release(r[])
    return nothing
  end
  Result(r[], t, cid, ctype)
end

function execute(con::Connection, stmt::String)
  return _execute(con, stmt, Int8(0))
end

function asarray(cur::Result)
  cur._ctype = Int8(1)
  return cur
end

function onerow(con::Connection, stmt::String)
  res = execute(con, stmt)
  isa(res, Nothing) && throw(NothingError())
  for r in res
      a = row2array(r)
      release(res) 
      return a
  end
end

function onevalue(con::Connection, stmt::String)
  return onerow(con, stmt)[1]
end

function describe(con::Connection, obj)
   a = Array{Tuple{String,String},1}()
   for row in execute(con, "describe $obj") |> asarray
       push!(a, (row[1], row[2]))
   end
   return a
end

function fill(con::Connection, stmt::String; T=Any, cols=0, count="", limit=0)
  l = limit; i = 1
  c = cols
  if c <= 0 
     fs = _parseselect(con, stmt)
     c = size(fs, 1)
     if c <= 0 throw(InvalidStatementError(stmt)) end
  end
  if count != "" l = onevalue(con, count) end 
  m = Matrix{T}(undef, l, c)
  for row in execute(con, stmt) |> asarray
    if l > 0
       for j=1:c m[i,j] = row[j] end
    else
       m = vcat(m,permutedims(row))
    end
    if l > 0 && i == l break end
    i+=1
  end
  return m
end 

function loadsql(con::Connection, stmt::String; T=Any, count="", limit=0)
  fs = _parseselect(con, stmt)
  df = DataFrames.DataFrame(fill(con, stmt, T=T,
                                 cols=size(fs,1),
                                 count=count,
                                 limit=limit))
  DataFrames.names!(df, [Symbol(f) for f in fs])
  return df
end

function now(con::Connection)
  return onevalue(con, "select now()")
end

function release(res::Result)
  if res._res == 0 return end
  _release(res._res)
  res._res = 0
end

function Base.iterate(res::Result, have=false)
  if !have
     res._type == CURSOR || res._type == ROW || throw(WrongTypeError())
     res._res != 0 || throw(ArgumentError("not a valid result"))
     err = _errcode(res._res)
     if err == EOF return nothing end
     if err != OK throw(DBError(err, _errmsg(res._res))) end
     res._fcount = fieldcount(res)
  elseif !_nextrow(res._res)
     if res._type == ROW return nothing end
     if !_fetch(res._res) return nothing end
  end
  ret = res._ctype == 0 ? res : row2array(res)
  return (ret, true)
end

function row2array(row::Result)
  Any[field(row,i) for i in 0:row._fcount-1]
end

function _convert(t::Int, v::Ptr{Nothing})
  if t == TEXT
    unsafe_string(convert(Cstring, v))
  elseif t == TIME || t == DATE || t == INT
    unsafe_load(Ptr{Clonglong}(v))
  elseif t == UINT
    unsafe_load(Ptr{Culonglong}(v))
  elseif t == FLOAT
    unsafe_load(Ptr{Cdouble}(v))
  elseif t == BOOL
    unsafe_load(Ptr{Cuchar}(v))
  else nothing end
end

function tfield(res::Result,idx::Int)
  t, v = _rfield(res._res, idx)
  (t, _convert(t,v))
end

function field(res::Result,idx::Int)
  _, v = tfield(res, idx)
  return v
end

function fieldcount(res::Result) 
  _rcount(res._res)
end

function _libinit()
  if ccall(("nowdb_client_init", lib), Cuchar, ()) == 0
     error("cannot init nowdbclient lib") # InitError
  end
end

function _libclose()
  ccall(("nowdb_client_close", lib), Cvoid, ())
end

function _resulttype(r::ResultT)
  ccall(("nowdb_result_type", lib), Cint, (ResultT,), r)
end

function _resultstatus(r::ResultT)
  ccall(("nowdb_result_status", lib), Cint, (ResultT,), r)
end

function _ok(r::ResultT)
  _resultstatus(r) == OK
end

function _eof(r::ResultT)
  _errcode(r) == EOF
end

function _errcode(r::ResultT)
  ccall(("nowdb_result_errcode", lib), Cint, (ResultT,), r)
end

function _errmsg(r::ResultT)
  unsafe_string(ccall(("nowdb_result_details", lib), Cstring, (ResultT,), r))
end

function _release(r::ResultT)
  ccall(("nowdb_result_destroy", lib), Cvoid, (ResultT,), r)
end

function _curid(r::ResultT)
  ccall(("nowdb_cursor_id", lib), CursorT, (ResultT,), r)
end

function _nextrow(r::ResultT)
  x = ccall(("nowdb_row_next", lib), Cint, (ResultT,), r)
  if x == EOF return false end
  if x != OK throw(ClientError(err, "")) end
  if !_ok(r)
     err = _errcode(r)
     if err == EOF return false end
     throw(DBError(err, _errmsg(r)))
  end
  return true
end

function _fetch(r::ResultT)
  x = ccall(("nowdb_cursor_fetch", lib), Cint, (ResultT,), r)
  if x == EOF return false end
  if x != 0
     throw(ClientError(_errcode(r), _errmsg(r)))
  end
  if !_ok(r)
     err = _errcode(r)
     if err == EOF return false end
     throw(DBError(err, _errmsg(r)))
  end
  ccall(("nowdb_row_rewind", lib), Cvoid, (ResultT,), r)
  return true
end

function _rfield(r::ResultT, idx::Int)
  t::Ref{Cint} = NOTHING
  v = ccall(("nowdb_row_field", lib), Ptr{Cvoid},
            (ResultT, Cint, Ref{Cint}),
             r, idx, t)
  return (Int(t[]), v)
end

function _rcount(r::ResultT)
  ccall(("nowdb_row_count", lib), Cint, (ResultT,), r)
end

const white = [' ', '\n', '\r', '\t']

_whitespace(c) = c in white

function _skipwhite(s)
  x = lastindex(s)
  for i=1:x-1
    if !_whitespace(s[i]) return (i-1,false) end
  end
  return (0,true)
end

function _parsefield(s)
  x = lastindex(s); i=1
  instr = 0
  inpar = 0
  while i <= x
     if instr > 0
        if s[i] == '\'' instr-=1 end
        i=nextind(s,i)
        continue
     end
     if inpar > 0
        if s[i] == ')' inpar -=1 elseif s[i] == '(' inpar += 1 end
        i+=1; continue
     end
     if s[i] == '\'' instr = 1; i+=1; continue end
     if s[i] == '(' inpar = 1; i+=1; continue end
     if s[i] == ',' return (i-1, false) end
     ok = true
     for j=0:3
         if !isvalid(s,i+j) ok=false; break end
     end
     if !ok i=nextind(s,i); continue end
     if lowercase(s[i:i+3]) == "from"  &&
        (i == 1 || _whitespace(s[i-1])) &&
        (i+4 >= x || _whitespace(s[i+4])) return (i-2,true)
     end
     i+=1
  end
  return (x,true)
end

function _parsefields(s)
  r = []
  a = 1
  x = false
  while !x
    (z,x) = _skipwhite(s[a:end])
    if x break end
    a += z
    (z,x) = _parsefield(s[a:end])
    push!(r, s[a:a+z-1])
    a += z+1
  end
  return r
end

function _alias(f)
  i = lastindex(f)
  e = i
  a = 1
  w = false
  x = false
  while i > 0
    if !isvalid(f[i]) throw(InvalidAliasError(f)) end
    if _whitespace(f[i])
       if w a = i+1; x = true end
       i-=1; continue
    end
    if i == 0 && !x throw(InvalidAliasError(f)) end
    if !w w=true; e=i
    elseif x
       if f[i] != 's' || f[i-1] != 'a' throw(InvalidAliasError(f)) else break end
    end
    i-=1
  end
  return f[a:e]
end

_aliases(fs) = map(_alias, fs)

function _parseselect(con::Connection, stmt)
  s = SubString(stmt)
  i, x = _skipwhite(s)
  i += 1 
  if x || lowercase(s[i:i+5]) != "select"
     throw(InvalidStatementError(s))
  end
  i+=6
  a, x = _skipwhite(s[i:end])
  if x return [] end
  i+=a
  if s[i] == '*'
     return [t[1] for t in describe(con, _getentity(s[i+1:end]))]
  end
  _parsefields(s[i:end]) |> _aliases
end

function _getentity(s)
  i, x = _skipwhite(s)
  i+=1
  if x || lowercase(s[i:i+3]) != "from" 
     println(s[i:i+3])
     throw(InvalidStatementError(s))
  end
  i += 4
  a, x = _skipwhite(s)
  if x throw(InvalidStatementError(s)) end
  i += a
  l = lastindex(s[1:end])
  e = l
  for j=i:l
    if _whitespace(s[j]) l=j; break end
  end
  return s[i:e]
end
end
