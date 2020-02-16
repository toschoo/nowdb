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

"""
       InvalidAliasError()

       Alias given in a select statement is invalid
       (e.g. is not an identifier,
             contains non-ASCII characters,
             the alias definition is not of the form 'a as b' or 'a')
"""
struct InvalidAliasError <: Exception
  msg::String
end

"""
       InvalidAliasError()

       The statement is syntactically wrong.
"""
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

"""
      NoWDB Types
"""
const TEXT = 1
const DATE = 2
const TIME = 3
const FLOAT = 4
const INT = 5
const UINT = 6
const BOOL = 9

# nanoseconds per second
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

# connection object
mutable struct Connection
  _con::ConT
  _addr::String
  _port::String
  _usr::String
  _pwd::String
  _db::String
  function Connection(c, addr, port, usr, pwd, db)
    me = new(c, addr, port, usr, pwd, db)
    finalizer(close, me)
    return me
  end
end

# low-level connect 
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
    connect(srv::String, port::String, usr::String, pwd::String, db="")

    Create a connection to the database server identified by
    the host (name or ip address) and
    the service (service name or port number)
    for the indicated user authenticated by the given password.
    If 'db' is given, the connection will issue a 'use' statement.

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
  close, reconnect, withconnection, use
"""
function connect(srv::String, port::String, usr::String, pwd::String, db="")
  c = _connect(srv, port, usr, pwd)
  con = Connection(c[], srv, port, usr, pwd, db)
  use(con, db)
  return con
end

"""
    close(con::Connection)

    Close the indicated connection to the database server and
    free all resources used by this connection.

# Related
  connect, reconnect, withconnection, use
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
  connect, withconnection, close, use
"""
function reconnect(con::Connection)
  c = _connect(con._srv, con._port, con._usr, con._pwd)
  con._con = c[]
  use(con, db)
end

"""
    withconnect(f::Function,
                srv::String, port::String,
                usr::String, pwd::String,
                db::String)

    Connect to the database; on success, apply function 'f' on the result
    and, finally, close the connection.
    Return the result of function 'f'.

# Examples
```
julia> NoW.withconnection("localhost", "50677", "foo", "bar", "mydb") do con
         for row in NoW.execute(con, "select * from mytable")
           # here we do something
         end
       end
```

# Related
  connect, reconnect, close, use
"""
function withconnection(f::Function, srv::String, port::String,
                        usr::String, pwd::String, db::String)
  con = connect(srv, port, usr, pwd, db)
  try return f(con) finally close(con) end
end

# result type
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

# low-level execute
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

"""
   execute(con::Connection, stmt::String)

   send the SQL statement 'stmt' to the database.
   If the the execution results in a row, cursor or report,
   the result is returned to the caller. Otherwise,
   nothing is returned.
   Throw an exception on error.

# Examples
```
julia> for row in NoW.execute(con, "select * from customer")
         # here we do something
       end
```

# Related
  asarray, fill, loadsql, onerow, onevalue
"""
function execute(con::Connection, stmt::String)
  return _execute(con, stmt, Int8(0))
end

"""
   asarry(cur::Result)

   instruct the result (row or cursor) to present results
   as arrays rather than in the internal row format.
   This function is typically used with piping (see examples).
   
# Examples
```
julia> for row in NoW.execute(con, "select * from customer") |> asarray
         # here we do something
       end
```

# Related
  execute, fill, loadsql, onerow, onevalue
"""
function asarray(cur::Result)
  cur._ctype = Int8(1)
  return cur
end

"""
   onerow(con::Connection, stmt::String)
   
   execute the statement 'stmt' and
   return the first row of its result as array.
   It is usually used for statements that are known
   to return just one line, like
   "select count(*), sum(amount) from sales" or
   "select pi(), e()"

# Examples
```
julia> row = NoW.onerow(con, "select count(*), sum(amount) from sales where customerid = 123")
```

# Related
  execute, fill, loadsql, onevalue
"""
function onerow(con::Connection, stmt::String)
  res = execute(con, stmt)
  isa(res, Nothing) && throw(NothingError())
  for r in res
      a = row2array(r)
      release(res) 
      return a
  end
end

"""
   onevalue(con::Connection, stmt::String)
   
   execute the statement 'stmt' and
   return the first field of the first row of its result.
   It is usually used for statements that are known
   to return just one value, like
   "select count(*) from sales" or
   "select now()"

# Examples
```
julia> cnt = NoW.onevalue(con, "select count(*) from sales where customerid = 123")
```

# Related
  execute, fill, loadsql, onerow
"""
function onevalue(con::Connection, stmt::String)
  return onerow(con, stmt)[1]
end

"""
   use(con::Connection, db::String)
   
   send a 'use' statement to the server, i.e. "use(\$db)";
   return nothing on success and throw an exception on error.

# Examples
```
julia> NoW.use(con, "salesdb")
```

# Related
  execute, connect, withconnection, reconnect
"""
use(con::Connection, db::String) = if db != "" execute(con, "use $(db)") end

"""
   describe(con::Connection, entity::String)
   
   send a 'describe' statement to the server, i.e. "describe \$entity",
   where 'entity' must be the name of a vertex or edge.
   return an array of attribute name, attribute type per attribute
   in the indicated entity.

# Examples
```
julia> NoW.describe(con, "sales")
```

# Related
  execute, asarray
"""
function describe(con::Connection, obj)
   a = Array{Tuple{String,String},1}()
   for row in execute(con, "describe $obj") |> asarray
       push!(a, (row[1], row[2]))
   end
   return a
end

"""
   now(con::Connection)
   
   send the statement "select now()" to the server
   and return the result as single Int64 value.

# Examples
```
julia> NoW.now(con)
1573563491879453212
```

# Related
  execute
"""
function now(con::Connection)
  return onevalue(con, "select now()")
end

"""
   release(res::Result)

   release all resource on server and client related to that result
   (row, cursor, report). The user usually does not need to release results.
   Results are automatically released by the GC; furthermore the iterator
   on cursors and rows (for row in NoW.execute(con, "select * from sales"))
   releases all resources when ready.
"""
function release(res::Result)
  if res._res == 0 return end
  _release(res._res)
  res._res = 0
end

"""
   fill(con::Connection, stmt::String; T=Any, cols=0, count="", limit=0)
 
   execute the statement and return the result as a matrix.
   The statement must be a select statement.

   Keywords: 
   - 'T' is a type; if this is given, *all* columns of the matrix will have type T,
     otherwise, they are of type 'Any';
   - 'cols' indicates the number of columns in the select statement;
     if not present, fill will parse the select clause of the statement to obtain this information
     and, if necessary, issue a describe statement.
   - 'count' is an additional SQL statement of the form "select count(*) ..."
     to obtain the numbers of rows in the result set;
   - 'limit' is a limit of rows (e.g. 100: produce at most 100 rows);
     if neither count nor limit are given, rows are catted to the result matrix
     (using vcat). Otherwise, the matrix is allocated with either count or limit number of rows
     with limit having preference over count. This is much faster than the vcat approach.

# Examples
```
julia> NoW.fill(con, "select * from sales", count="select count(*) from sales")
100×10 Array{Any,2}:
```

# Related
  execute, loadsql, describe
"""
function fill(con::Connection, stmt::String; T=Any, cols=0, count="", limit=0)
  l = limit; i = 1
  c = cols
  if c <= 0 
     fs = _parseselect(con, stmt)
     c = size(fs, 1)
     if c <= 0 throw(InvalidStatementError(stmt)) end
  end
  if count != "" && l <= 0 l = onevalue(con, count) end 
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

"""
   loadsql(con::Connection, stmt::String; count="", limit=0)
 
   execute the statement and return the result as DataFrame.
   The statement must be a select statement.
   The column names are extracted from the select clause.

   Keywords: 
   - 'count' is an additional SQL statement of the form "select count(*) ..."
     to obtain the numbers of rows in the result set;
   - 'limit' is a limit of rows (e.g. 100: produce at most 100 rows);
     if neither count nor limit are given, rows are catted to the result matrix
     (using vcat). Otherwise, the matrix is allocated with either count or limit number of rows
     with limit having preference over count. This is much faster than the vcat approach.

# Examples
```
julia> NoW.loadsql(con, "select * from sales", count="select count(*) from sales")
100×10 DataFrames.DataFrame
```

# Related
  execute, fill, describe
"""
function loadsql(con::Connection, stmt::String; count="", limit=0)
  fs = _parseselect(con, stmt)
  df = DataFrames.DataFrame(fill(con, stmt,
                                 cols=size(fs,1),
                                 count=count,
                                 limit=limit))
  DataFrames.names!(df, [Symbol(f) for f in fs])
  return df
end

"""
   iterate(res::Result, have=false)

   iterate over the result set created by execute;
   the result set must be a cursor or row;
   the result is automatically released.

# Examples
```
julia> for row in NoW.execute(con, "select * from sales") |> asarray
           print("\$(row[1]), \$(row[2])")
       end
```

# Related
  execute, asarray
"""
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

"""
    row2array(row::Result)

    transform the row into a vector.

# Related
  execute, asarray
"""
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

"""
    tfield(res::Result, idx::Int)

    return the value at idx in the row as tuple (type, value).
    The result must be a row; the indexing uses base 0!
    The type in the tuple is the NoWDB Type.

# Examples
```
julia> NoW.withconnection("localhost", "50677", "foo", "bar", "mydb") do con
         for row in NoW.execute(con, "select * from mytable")
             t, v = NoW.tfield(row, 0)
             println("\$v as type \$t") 
         end
       end
```

# Related
  execute, field, fieldcount
"""
function tfield(res::Result,idx::Int)
  t, v = _rfield(res._res, idx)
  (t, _convert(t,v))
end

"""
    field(res::Result, idx::Int)

    return the value at idx in the row.
    The result must be a row; the indexing uses base 0!

# Examples
```
julia> NoW.withconnection("localhost", "50677", "foo", "bar", "mydb") do con
         for row in NoW.execute(con, "select * from mytable")
             v = NoW.field(row, 0)
             println(v)
         end
       end
```

# Related
  execute, tfield, fieldcount
"""
function field(res::Result,idx::Int)
  _, v = tfield(res, idx)
  return v
end

"""
    fieldcount(res::Result)

    return the number of fields in the row.

# Examples
```
julia> NoW.withconnection("localhost", "50677", "foo", "bar", "mydb") do con
         for row in NoW.execute(con, "select * from mytable")
            e = NoW.fieldcount(row)
            for i=0:e-1
               println(print("\$(NoW.field(row, i)) ")
            end
         end
         println("")
       end
```

# Related
  execute, tfield, field
"""
function fieldcount(res::Result) 
  _rcount(res._res)
end

# init the library
function _libinit()
  if ccall(("nowdb_client_init", lib), Cuchar, ()) == 0
     error("cannot init nowdbclient lib") # InitError
  end
end

# close the library
function _libclose()
  ccall(("nowdb_client_close", lib), Cvoid, ())
end

# get result type from result
function _resulttype(r::ResultT)
  ccall(("nowdb_result_type", lib), Cint, (ResultT,), r)
end

# get result status (ok or not ok) form result
function _resultstatus(r::ResultT)
  ccall(("nowdb_result_status", lib), Cint, (ResultT,), r)
end

# check if result status is ok (boolean)
function _ok(r::ResultT)
  _resultstatus(r) == OK
end

# check if errcode is EOF
function _eof(r::ResultT)
  _errcode(r) == EOF
end

# Get errcode from result
function _errcode(r::ResultT)
  ccall(("nowdb_result_errcode", lib), Cint, (ResultT,), r)
end

# Get errmessage from result
function _errmsg(r::ResultT)
  unsafe_string(ccall(("nowdb_result_details", lib), Cstring, (ResultT,), r))
end

# Release the result 
function _release(r::ResultT)
  ccall(("nowdb_result_destroy", lib), Cvoid, (ResultT,), r)
end

# Get curid from result
function _curid(r::ResultT)
  ccall(("nowdb_cursor_id", lib), CursorT, (ResultT,), r)
end

# Advance to next row
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

# Fetch next bunch of rows
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

# Get field from row  as (type, value) pair
function _rfield(r::ResultT, idx::Int)
  t::Ref{Cint} = NOTHING
  v = ccall(("nowdb_row_field", lib), Ptr{Cvoid},
            (ResultT, Cint, Ref{Cint}),
             r, idx, t)
  return (Int(t[]), v)
end

# Count number of fields in row
function _rcount(r::ResultT)
  ccall(("nowdb_row_count", lib), Cint, (ResultT,), r)
end

# Select Parser: whitespace
const white = [' ', '\n', '\r', '\t']

# Select Parser: check if whitespace
_whitespace(c) = c in white

# Select Parser: skipe whitespace
function _skipwhite(s)
  x = lastindex(s)
  for i=1:x-1
    if !_whitespace(s[i]) return (i-1,false) end
  end
  return (0,true)
end

# Select Parser: parse one field in the format
#                a as b
#                a
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

# Select Parser: parse all fields
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

# Select Parser: Get alias from field descriptor
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

# Select Parser: Get aliases from all field descriptors
_aliases(fs) = map(_alias, fs)

# Select Parser: parse the select clause
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

# Select Parser: get (mandatory) entity in 'from'
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
