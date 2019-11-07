module NoW

using Dates

# lib = Libdl.dlopen("libnowdbclient")
# sym_init = Libdl.dlsym(lib, "nowdb_client_init")

# TODO
#   + release cursor after loop

const lib = "libnowdbclient"

# exceptions
struct NoConnectionError <: Exception end 
struct WrongTypeError    <: Exception end
struct NothingError      <: Exception end
struct UnknownError      <: Exception
  code::Int
  msg::String
end
struct ClientError       <: Exception
  code::Int
  msg::String
end
struct DBError           <: Exception
  code::Int
  msg::String
end

const ConT = Culonglong
const CursorT = Culonglong
const ResultT = Culonglong
const NowValue = Culonglong

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

const NOTHING = 0
const STATUS  = 33
const REPORT  = 34
const ROW     = 35
const CURSOR  = 36

const TEXT = 1
const DATE = 2
const TIME = 3
const FLOAT = 4
const INT = 5
const UINT = 6
const BOOL = 9

const NPERSEC = 1000000000

function _libinit()
  if ccall(("nowdb_client_init", lib), Cuchar, ()) == 0
     error("cannot init nowdbclient lib") # InitError
  end
end

function _libclose()
  ccall(("nowdb_client_close", lib), Cvoid, ())
end

function __init__()
  _libinit()
end

function now2unix(t::Int64)
  f = Float64(t)/NPERSEC
  Dates.unix2datetime(f)
end

function belowsec(t::Int64)
  s = t÷NPERSEC
  xs = t - s*NPERSEC
  ys = xs÷1000
  ms = ys÷1000
  ns = xs - 1000ys
  us = ys - 1000ms
  (ms, us, ns)
end

function datetime2now(t::Dates.DateTime; us=0, ns=0)
  f = Dates.datetime2unix(t)
  n = Int64(1000f)
  return 1000000n + 1000us + ns
end

function now2datetime(t::Int64)
  return now2unix(t)
end

function now2datetimens(t::Int64)
  _, us, ns = belowsec(t)
  (now2unix(t), us, ns)
end

function now2date(t::Int64)
  Dates.Date(now2unix(t))
end

function now2time(t::Int64)
  nix = now2unix(t)
  ms, us, ns = belowsec(t)
  Dates.Time(Dates.hour(nix),
             Dates.minute(nix),
             Dates.second(nix),
             ms, us, ns)
end

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

function connect(srv::String, port::String, usr::String, pwd::String)
  c = _connect(srv, port, usr, pwd)
  Connection(c[], srv, port, usr, pwd)
end

function close(con::Connection)
  if con._con == 0 return end
  x = ccall(("nowdb_connection_close", lib), Cint, (ConT,), con._con)
  if x != 0
      ccall(("nowdb_connection_destroy", lib), Cvoid, (ConT,), con._con)
  end
  con._con = 0
end

function reconnect(con::Connection)
  c = _connect(con._srv, con._port, con._usr, con._pwd)
  con._con = c[]
  if con._db != ""
     execute(con, "use $db"); con._db = db
  end
end

function withconnection(srv::String, port::String,
                        usr::String, pwd::String,
                        db::String, f::Function)
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

function execute(con::Connection, stmt::String, ctype::Int8)
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
  return execute(con, stmt, Int8(0))
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

function fill(con::Connection, stmt::String, cols; T=Any, count="", limit=0)
  l = limit; i = 1
  if count != "" l = onevalue(con, count) end 
  m = Matrix{T}(undef, l, cols)
  for row in execute(con, stmt) |> asarray
    if l > 0
       for j=1:cols m[i,j] = row[j] end
    else
       m = vcat(m,permutedims(row))
    end
    if l > 0 && i == l break end
    i+=1
  end
  return m
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
  Any[field(row,i) for i in 0:row._fcount]
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

end
