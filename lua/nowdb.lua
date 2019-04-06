---------------------------------------------------------------------------
-- Basic Lua NOWDB Stored Procedure Interface
 
--------------------------------------
-- (c) Tobias Schoofs, 2018 -- 2019
--------------------------------------
   
-- This file is part of the NOWDB Stored Procedure Library.

-- It provides in particular
-- - an execute function
-- - a polymorphic result type
-- - an iterable cursor and row result type
-- - time conversion functions
-- - error handling functions

-- The NOWDB Stored Procedure Library
-- is free software; you can redistribute it
-- and/or modify it under the terms of the GNU Lesser General Public
-- License as published by the Free Software Foundation; either
-- version 2.1 of the License, or (at your option) any later version.
  
-- The NOWDB Stored Procedure Library
-- is distributed in the hope that it will be useful,
-- but WITHOUT ANY WARRANTY; without even the implied warranty of
-- MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
-- Lesser General Public License for more details.
  
-- You should have received a copy of the GNU Lesser General Public
-- License along with the NOWDB CLIENT Library; if not, see
-- <http://www.gnu.org/licenses/>.
---------------------------------------------------------------------------

---------------------------------------------------------------------------
-- The nowdb library is implemented in this global variable
-- It can be accessed immediately (without 'require') just like
-- the standard lua libraries.
---------------------------------------------------------------------------
nowdb = {}

---------------------------------------------------------------------------
-- special errors
---------------------------------------------------------------------------
nowdb.OK = 0
nowdb.EOF = 8
nowdb.NOMEM = 1
nowdb.TOOBIG = 5
nowdb.NOTACUR = -10
nowdb.NOTAROW = -11
nowdb.USRERR  =  74

---------------------------------------------------------------------------
-- result types
---------------------------------------------------------------------------
nowdb.NOTHING = 0
nowdb.STATUS = 33
nowdb.REPORT = 34
nowdb.ROW = 35
nowdb.CURSOR = 36

---------------------------------------------------------------------------
-- static types
---------------------------------------------------------------------------
nowdb.TEXT = 1
nowdb.DATE = 2
nowdb.TIME = 3
nowdb.FLOAT= 4
nowdb.INT  = 5
nowdb.UINT = 6
nowdb.BOOL = 9

---------------------------------------------------------------------------
-- End of Row
---------------------------------------------------------------------------
nowdb.EOR = 10

---------------------------------------------------------------------------
-- Time handling
---------------------------------------------------------------------------
nowdb.second = 1000000000
nowdb.minute = 60*nowdb.second
nowdb.hour   = 60*nowdb.minute
nowdb.day    = 24*nowdb.hour
nowdb.year   = 365*nowdb.day

---------------------------------------------------------------------------
-- Round time to 'd' (e.g. day | hour | minute | scecond)
---------------------------------------------------------------------------
function nowdb.round(t, d)
  return d*math.floor(t/d)
end

---------------------------------------------------------------------------
-- Format time descriptor according to canonical time format
---------------------------------------------------------------------------
function nowdb.timeformat(t)
  return string.format('%04d-%02d-%02dT%02d:%02d:%02d.%09d',
                       t.year, t.month, t.day, 
                       t.hour, t.min, t.sec, t.nano)
end

---------------------------------------------------------------------------
-- Format time descriptor according to canonical date format
---------------------------------------------------------------------------
function nowdb.dateformat(t)
  return string.format('%04d-%02d-%02d',
                       t.year, t.month, t.day)
end

---------------------------------------------------------------------------
-- Raise error
---------------------------------------------------------------------------
function nowdb.raise(rc, msg)
  local m = msg or ''
  error(string.format("%d: %s", rc, m), 2)
end

-- internal use only
local function errdetails(r)
  return now2lua_errdetails(r)
end

-- internal use only
local function errcode(r)
  return now2lua_errcode(r)
end

---------------------------------------------------------------------------
-- Create polymorphic result type (with 'r' being a C pointer)
-- the function is used in success, error, execute and makerow, etc.
---------------------------------------------------------------------------
local function mkResult(r)
  local self = {cr=r}

  -- check if healthy
  local function ok()
    if not self.cr then return end
    return now2lua_errcode(self.cr) == nowdb.OK
  end

  -- is end of file
  local function eof()
    if not self.cr then return end
    return (now2lua_errcode(self.cr) == nowdb.EOF)
  end

  -- get error code
  local function errcode()
    if not self.cr then return end
    return now2lua_errcode(self.cr)
  end

  -- get error details
  local function errdetails()
    if not self.cr then return end
    return now2lua_errdetails(self.cr)
  end

  -- get result type
  local function resulttype()
    if not self.cr then return nowdb.Nothing end
    return now2lua_rtype(self.cr)
  end

  -- fetch from cursor (if cursor)
  local function fetch()
    if resulttype() ~= nowdb.CURSOR then return nowdb.NOTACUR end
    local rc = now2lua_fetch(self.cr)
    rc = (rc == nowdb.OK) and errcode() or rc
    if rc ~= nowdb.OK then
       if rc == nowdb.EOF then return rc, nil end
       return rc, errdetails(self.cr)
    end
    return nowdb.OK
  end

  -- go to next row
  local function nextrow()
    local r = resulttype()
    if r ~= nowdb.CURSOR and r ~= nowdb.ROW
    then
       return nowdb.NOTACUR
    end
    local rc = now2lua_nextrow(self.cr)
    rc = (rc == nowdb.OK) and errcode() or rc
    if rc ~= nowdb.OK then return rc end
    return nowdb.OK
  end

  -- open cursor
  local function open()
    if self.opened then return nowdb.OK end
    if resulttype() ~= nowdb.CURSOR then return nowdb.NOTACUR end
    now2lua_open(self.cr)
    self.opened = true
    return nowdb.OK
  end

  -- iterator (see below)
  local function _rows()
    if not self.opened then 
       rc = open()
       if rc ~= nowdb.OK then return nil end
       fetch()
       if not ok() then return nil end
    end
    if self.neednext then
       local rc = nextrow()
       if rc ~= nowdb.OK then
          if rc == nowdb.EOF then
             if fetch() ~= nowdb.OK then return nil end
             return self
          else
             return nil
          end
       end
    end
    if not self.neednext then self.neednext = true end
    return self
  end

  -- how many fields do we have per row (cursor and row)
  local function countfields()
    local r = resulttype()
    if r ~= nowdb.CURSOR and r ~= nowdb.ROW
    then
       return nowdb.raise(nowdb.NOTAROW, "not a row")
    end
    local rc, msg = now2lua_countfields(self.cr)
    if rc ~= nowdbOK then nowdb.raise(rc, msg) end
    return rc
  end

  -- type conversion is easy
  local function conv(t,v)
    if t == nowdb.NOTHING then return nil else return v end
  end

  -- get ith field from row (or cursor)
  local function field(i)
    local r = resulttype()
    if r ~= nowdb.CURSOR and r ~= nowdb.ROW
    then
       return nil
    end
    local t, v = now2lua_field(self.cr, i)
    return conv(t,v)
  end

  -- add v as type t to the result (which needs to be a row)
  local function add2row(t,v)
    if resulttype() ~= nowdb.ROW then nowdb.raise(nowdb.NOTAROW, "not a row") end
    local x = now2lua_rowcapacity(self.cr)
    if t == nowdb.TEXT then 
       if string.len(v) >= x then nowdb.raise(nowdb.TOOBIG, "text too big for row") end
    else
       if x <= 8 then nowdb.raise(nowdb.TOOBIG, "no space left in row") end
    end
    local rc, msg = now2lua_add2row(self.cr,t,v)
    if rc ~= nowdb.OK then nowdb.raise(rc, msg) end
  end

  -- add EOR to row
  local function closerow()
    if resulttype() ~= nowdb.ROW then nowdb.raise(nowdb.NOTAROW, "not a row") end
    local rc, msg = now2lua_closerow(self.cr)
    if rc ~= nowdb.OK then nowdb.raise(rc, msg) end
  end

  -- release the type.
  -- This may be left to the gc; however,
  -- since there is no control over when
  -- the GC will reclaim the memory,
  -- it is safer to explicitly call release,
  -- when the result is not needed any longer.
  local function release()
    if self.cr == nil then return end
    now2lua_release(self.cr)
    self.cr = nil
  end

  -- gets the errdetails from the result and releases it
  local function toerr()
    if ok() then return end
    if not self.cr then return end
    local msg = errdetails(self.cr)
    self.release()
    return msg
  end

  -- the public interface
  local ifc = {
    ok = ok,
    eof = eof,
    errcode = errcode,
    errdetails = errdetails,
    toerr = toerr,
    resulttype = resulttype,
    open  = open,
    fetch = fetch,
    nextrow = nextrow,
    countfields = countfields,
    field = field,
    add2row = add2row,
    closerow = closerow,
    release = release
  }

  -- row iterator:
  local function riter(cur)
    if _rows() == nil then return nil else return cur end
  end

  ------------------------------------------------------------------------
  -- row iterator factory, it is used on a cursor (or row) like:
  --     for row in cur.rows() do
  --         dosomethingwith(row.field(0))
  --         dosomethingelsewith(row.field(1))
  --         ...
  --     end
  ------------------------------------------------------------------------
  ifc.rows = function()
     return riter, ifc
  end

  -- for internal use only --
  function ifc.toDB()
     local r = self.cr
     self.cr = nil
     return r
  end

  setmetatable(ifc, {__gc = release})
  return ifc
end

---------------------------------------------------------------------------
-- Create a result representing 'success'
---------------------------------------------------------------------------
function nowdb.success()
  return mkResult(now2lua_success())
end

---------------------------------------------------------------------------
-- Create a result representing an error
---------------------------------------------------------------------------
function nowdb.error(rc, msg)
   if not rc then return nowdb.success() end
   if msg == nil then m = "no details available"
   elseif type(msg) ~= 'string' then
      nowdb.raise(nowdb.USRERR, "error called with wrong type")
   end
   local r, m = mkResult(now2lua_error(rc, m))
   if r ~= 0 and not m then return r end
   nowdb.raise(rc, msg)
end

---------------------------------------------------------------------------
-- Create a row result
---------------------------------------------------------------------------
function nowdb.makerow()
   rc, row = now2lua_makerow()
   if rc ~= nowdb.OK then nowdb.raise(rc, row) end
   return mkResult(row)
end

---------------------------------------------------------------------------
-- Create a row result with one field, name v as type t
---------------------------------------------------------------------------
function nowdb.makeresult(t,v)
   local row = nowdb.makerow()
   row.add2row(t,v)
   row.closerow()
   return row
end

---------------------------------------------------------------------------
-- Executes the statement 'stmt'
-- return an error code and the result
-- if the error code is not 'OK',
-- then the result is a string describing the error details
-- otherwise, the result is the result of the statement
---------------------------------------------------------------------------
function nowdb.pexecute(stmt)
  local rc, r = now2lua_execute(nowdb._db, stmt)
  rc = (rc == nowdb.OK) and errcode(r) or rc
  if rc ~= nowdb.OK then
     if r == nil then return rc, "no error details available" end
     local msg = ''
     if type(r) == 'string' then
        msg = r 
     else
        msg = errdetails(r)
        now2lua_release(r)
     end
     return rc, msg
  end
  return nowdb.OK, mkResult(r)
end

---------------------------------------------------------------------------
-- Like pexecute, but does not return an error code, but only the result.
-- If an error occurs, an exception is raised
---------------------------------------------------------------------------
function nowdb.execute(stmt)
  local rc, r = nowdb.pexecute(stmt)
  if rc ~= nowdb.OK then nowdb.raise(rc, r) end
  return r
end

---------------------------------------------------------------------------
-- Like execute, but does not return the result.
-- This may be convenient for statements where the result is not relevant,
-- e.g. DDL and DML 
---------------------------------------------------------------------------
function nowdb.execute_(stmt)
  local r = nowdb.execute(stmt)
  if r ~= nil then r.release() end
end

---------------------------------------------------------------------------
-- Converts a nowdb timestamp to a time descriptor,
-- a table with the fields
--  year :
--  month: 01 - 12
--  day  : 01 - 31
--  wday : week day with sunday = 1 and saturday = 6
--  yday : day of the year with January, 1, being 1
--  hour : 00 - 23
--  min  : 00 - 59
--  sec  : 00 - 59
--  nano :  0 - 999999999
--
-- The function may raise an error!
---------------------------------------------------------------------------
function nowdb.from(t)
  local frm  = [[select year(%d), month(%d), mday(%d),
                        wday(%d), yday(%d),
                        hour(%d), minute(%d), second(%d),
                        nano(%d)]]
  local stmt = string.format(frm, t, t, t, t, t, t, t, t, t)
  local row = nowdb.execute(stmt)
  local r = {
    year = row.field(0),
    month = row.field(1),
    day = row.field(2),
    wday = row.field(3) + 1,
    yday = row.field(4) + 1,
    hour = row.field(5),
    min = row.field(6),
    sec = row.field(7),
    nano =row.field(8)
  }
  row.release()
  return r
end

---------------------------------------------------------------------------
-- Converts a time descriptor to a nowdb timestamp
-- The function may raise an error!
---------------------------------------------------------------------------
function nowdb.to(t)
  local frm = "select '%s'"
  local stmt = string.format(frm, nowdb.timeformat(t))
  local row = nowdb.execute(stmt)
  local r = row.field(0)
  row.release()
  return r
end

---------------------------------------------------------------------------
-- Gets the current time
-- The function may raise an error!
---------------------------------------------------------------------------
function nowdb.getnow()
  local row = nowdb.execute("select now()")
  local n = row.field(0)
  row.release()
  return n
end

-- internal use only ---
function _nowdb_caller(callee, ...)
  local ok, r = pcall(callee, ...)
  if not ok then return now2lua_error(nowdb.USRERR, r) end
  if r == nil then return now2lua_success() end
  if r.toDB ~= nil then return r.toDB() end
  return now2lua_error(nowdb.USRERR, "unexpected result type")
end

-- internal use only ---
function _nowdb_setDB(db)
  nowdb._db = db
end
