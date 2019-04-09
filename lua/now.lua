---------------------------------------------------------------------------
-- Basic Lua NOWDB Client Interface

--------------------------------------
-- (c) Tobias Schoofs, 2019
--------------------------------------
   
-- This file is part of the NOWDB CLIENT Library.

-- It provides in particular
-- - a connection object and constructor
-- - an execute method
-- - a polymorphic result type
-- - an iterable cursor result type
  
-- The NOWDB CLIENT Library is free software; you can redistribute it
-- and/or modify it under the terms of the GNU Lesser General Public
-- License as published by the Free Software Foundation; either
-- version 2.1 of the License, or (at your option) any later version.
  
-- The NOWDB CLIENT Library
-- is distributed in the hope that it will be useful,
-- but WITHOUT ANY WARRANTY; without even the implied warranty of
-- MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
-- Lesser General Public License for more details.
--
-- You should have received a copy of the GNU Lesser General Public
-- License along with the NOWDB CLIENT Library; if not, see
-- <http://www.gnu.org/licenses/>.
---------------------------------------------------------------------------
require('libnowluaifc')  -- the C interface
local now = {alive=true} -- the module

---------------------------------------------------------------------------
-- close the module
-- this is typically used only once per process
-- one can (and should) even leave it to lua
---------------------------------------------------------------------------
local function close(now)
  if now == nil then return end
  if not now.alive then return end
  cnow_stop()
end
setmetatable(now, {__gc=close}) -- close when it goes out of scope

---------------------------------------------------------------------------
-- special errors
---------------------------------------------------------------------------
now.OK = 0
now.EOF = 8
now.NOTACUR = -10
now.NOTAROW = -11

---------------------------------------------------------------------------
-- result types
---------------------------------------------------------------------------
now.NOTHING = 0
now.STATUS = 33
now.REPORT = 34
now.ROW = 35
now.CURSOR = 36

---------------------------------------------------------------------------
-- static types
---------------------------------------------------------------------------
now.TEXT = 1
now.DATE = 2
now.TIME = 3
now.FLOAT= 4
now.INT  = 5
now.UINT = 6
now.BOOL = 9

---------------------------------------------------------------------------
-- End of Row
---------------------------------------------------------------------------
now.EOR = 10

---------------------------------------------------------------------------
-- Time handling
---------------------------------------------------------------------------
now.second = 1000000000
now.minute = 60*now.second
now.hour   = 60*now.minute
now.day    = 24*now.hour
now.year   = 365*now.day

function now.round(t, d)
  return d*math.floor(t/d)
end

function now.timeformat(t)
  return string.format('%04d-%02d-%02dT%02d:%02d:%02d.%09d',
                       t.year, t.month, t.day, 
                       t.hour, t.min, t.sec, t.nano)
end

function now.dateformat(t)
  return string.format('%04d-%02d-%02d',
                       t.year, t.month, t.day)
end

---------------------------------------------------------------------------
-- Raise error
---------------------------------------------------------------------------
function now.raise(rc, msg)
  local m = msg or 'now details available'
  error(string.format("%d: %s", rc, m), 2)
end

-- internal use only 
local function errcode(r)
  return cnow_errcode(r)
end

-- internal use only 
local function errdetails(r)
  return cnow_errdetails(r)
end

-- internal use only 
local function isok(r)
  return (cnow_errcode(r) == now.OK)
end

---------------------------------------------------------------------------
-- Polymorphic Result type
-- this function is called by execute,
-- in fact execute is the only way to create results.
---------------------------------------------------------------------------
local function makeResult(r)
  local self = {cr = r}

  -- check if healthy
  local function ok()
    if not self.cr then return end
    return isok(self.cr)
  end

  -- is end of file
  local function eof()
    if not self.cr then return end
    return (cnow_errcode(self.cr) == now.EOF)
  end

  -- get error code
  local function errcode()
    if not self.cr then return end
    return cnow_errcode(self.cr)
  end

  -- get error details
  local function errdetails()
    if not self.cr then return end
    return cnow_errdetails(self.cr)
  end

  -- get result type
  local function resulttype()
    if not self.cr then return now.Nothing end
    return cnow_rtype(self.cr)
  end

  -- get cursor id (if cursor)
  local function getid()
    if not self.cr then return end
    return cnow_curid(self.cr)
  end

  -- fetch from cursor (if cursor)
  local function fetch()
    if resulttype() ~= now.CURSOR
    then
       return now.NOTACUR, 'result is not a cursor'
    end
    local rc = cnow_fetch(self.cr)
    rc = (rc == now.OK) and errcode() or rc
    if rc ~= now.OK then
       if rc == now.EOF then return rc, nil end
       return rc, errdetails(self.cr)
    end
    return now.OK
  end

  -- go to next row
  local function nextrow()
    local r = resulttype()
    if r ~= now.CURSOR and r ~= now.ROW
    then
       return now.NOTACUR
    end
    local rc = cnow_nextrow(self.cr)
    rc = (rc == now.OK) and errcode() or rc
    if rc ~= now.OK then
       if rc == now.EOF then return rc end
       return rc
    end
    return now.OK
  end

  -- iterator (see below)
  local function _rows()
    if self.neednext then
       local rc = nextrow()
       if rc ~= now.OK then
          if resulttype() ~= now.CURSOR then return nil end
          local rc, msg = fetch()
          if rc ~= now.OK then
             if rc == now.EOF then return nil end
             now.raise(rc, msg)
          end
          return self
       end
    end
    if not self.neednext then self.neednext = true end
    return self
  end

  -- how many fields do we have per row (cursor and row)
  local function countfields()
    local r = resulttype()
    if r ~= now.CURSOR and r ~= now.ROW
    then
       now.raise(now.NOTAROW, nil)
    end
    return cnow_countfields(self.cr)
  end

  -- type conversion is easy
  local function conv(t,v)
    if t == now.NOTHING then return nil
    elseif t == now.BOOL then
      if v == 0 then return false else return true end
    else return v end
  end

  -- get value of ith field from row (or cursor)
  local function field(i)
    local r = resulttype()
    if r ~= now.CURSOR and r ~= now.ROW
    then
       return nil
    end
    local t, v = cnow_field(self.cr, i)
    return conv(t,v)
  end

  -- get type and value of ith field from row (or cursor)
  local function typedfield(i)
    local r = resulttype()
    if r ~= now.CURSOR and r ~= now.ROW
    then
       return now.NOTHING, nil
    end
    local t, v = cnow_field(self.cr, i)
    return t, conv(t,v)
  end

  -- release the type.
  -- This may be left to the gc, however,
  -- since there is no control over when
  -- the GC will reclaim the memory,
  -- it is safer to explicitly call release,
  -- when the result is not needed any longer.
  local function release()
    if self.cr == nil then return end
    cnow_release(self.cr)
    self.cr = nil
  end

  -- the interface
  local ifc = {
    ok = ok,
    eof = eof,
    errcode = errcode,
    errdetails = errdetails,
    resulttype = resulttype,
    getid = getid,
    fetch = fetch,
    nextrow = nextrow,
    countfields = countfields,
    field = field,
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

  -- release the result type automatically
  setmetatable(ifc, {__gc=release})

  return ifc

end

---------------------------------------------------------------------------
-- Connection
---------------------------------------------------------------------------
function now.connect(srv, port, usr, pwd)
  local self = {server=srv, port=port, user=usr, pwd=pwd}
  local rc, c = cnow_connect(srv, port, usr, pwd)
  if rc ~= now.OK then
     return rc, c -- in case of error c is the error message
  end

  self.con = c

  -- close the connection
  -- may be left to lua if
  -- only one connection is needed in the process
  local function close()
    if not self.con then return end
    cnow_close(self.con)
    self.con = nil
  end

  -- execute the statement (protected)
  -- returns a result or an errorcode and message
  local function pexecute(stmt)
    local rc, r = cnow_execute(self.con, stmt)
    rc = (rc == now.OK) and errcode(r) or rc
    if rc ~= now.OK then
       if r == nil then return rc, 'no details available' end
       local msg = ''
       if type(r) == 'string' then
          msg = r
       else
          msg = errdetails(r)
          r.release()       
       end
       return rc, msg
    end
    return now.OK, makeResult(r)
  end

  -- like pexecute, but may call error
  local function execute(stmt)
    local rc, r = pexecute(stmt)
    if rc ~= now.OK then now.raise(rc, r) end
    return r
  end

  -- like execute, but does not return the result
  local function execute_(stmt)
    local r = execute(stmt)
    if r ~= nil then r.release() end
  end

  -- use this database in this session
  local function use(db)
    local stmt = "use " .. db
    local r = execute(stmt)
    if r then r.release() end
  end

  -------------------------------------------------------------------------
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
  -------------------------------------------------------------------------
  local function from(t)
    local frm  = [[select year(%d), month(%d), mday(%d),
                          wday(%d), yday(%d),
                          hour(%d), minute(%d), second(%d),
                          nano(%d)]]
    local stmt = string.format(frm, t, t, t, t, t, t, t, t, t)
    local cur = execute(stmt)
    local r = {}
    for row in cur.rows() do
        r.year = row.field(0)
        r.month = row.field(1)
        r.day = row.field(2)
        r.wday = row.field(3) + 1
        r.yday = row.field(4) + 1
        r.hour = row.field(5)
        r.min = row.field(6)
        r.sec = row.field(7)
        r.nano =row.field(8) 
    end
    cur.release()
    return r
  end

  -------------------------------------------------------------------------
  -- Converts a time descriptor to a nowdb timestamp
  -- The function may raise an error!
  -------------------------------------------------------------------------
  local function to(t)
    local frm = "select '%s'"
    local stmt = string.format(frm, now.timeformat(t))
    local cur = execute(stmt)
    for row in cur.rows() do
        r = row.field(0)
    end
    cur.release()
    return r
  end

  -------------------------------------------------------------------------
  -- Gets the current time
  -- The function may raise an error!
  -------------------------------------------------------------------------
  local function getnow()
    local cur = execute("select now()")
    local n = 0
    for row in cur.rows() do
        n = row.field(0)
    end
    cur.release()
    return n
  end

  -- the public interface 
  local ifc = {
     close = close,
     use = use, 
     pexecute = pexecute,
     execute = execute,
     execute_ = execute_,
     getnow   = getnow,
     from  = from,
     to    = to 
  }

  ---------------------------------------------------------------
  -- row iterator factory directly over execute
  --     for row in c.rows() do
  --         dosomethingwith(row.field(0))
  --         dosomethingelsewith(row.field(1))
  --         ...
  --     end
  --
  -- CAUTION:
  -- the generator may throw an error (i.e. call error)
  ---------------------------------------------------------------
  ifc.rows = function(stmt)
     return execute(stmt).rows()
  end

  setmetatable(ifc, {__gc=close})

  return now.OK, ifc
end

-- return the module
return now
