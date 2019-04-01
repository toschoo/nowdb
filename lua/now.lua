---------------------------------------------------------------------------
-- Basic Lua NOWDB Client Interface

--------------------------------------
-- (c) Tobias Schoofs, 2018
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
-- TODO
---------------------------------------------------------------------------

---------------------------------------------------------------------------
-- Get error code for this guy
---------------------------------------------------------------------------
local function errcode(r)
  return cnow_errcode(r)
end

---------------------------------------------------------------------------
-- Get error details of this guy
---------------------------------------------------------------------------
local function errdetails(r)
  return cnow_errdetails(r)
end

---------------------------------------------------------------------------
-- Quick check: everything healthy?
---------------------------------------------------------------------------
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
  local function resultType()
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
    if resultType() ~= now.CURSOR
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
    local r = resultType()
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
          if rc == now.EOF then
             if fetch() ~= now.OK then return nil end
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
    local r = resultType()
    if r ~= now.CURSOR and r ~= now.ROW
    then
       return now.NOTHING, nil
    end
    return cnow_countfields(self.cr)
  end

  -- type conversion is easy
  local function conv(t,v)
    if t == now.NOTHING then return nil else return v end
  end

  -- get ith field from row (or cursor)
  local function field(i)
    local r = resultType()
    if r ~= now.CURSOR and r ~= now.ROW
    then
       return now.NOTHING, nil
    end
    local t, v = cnow_field(self.cr, i)
    return conv(t,v)
  end

  -- release the type (may be left to the gc)
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
    resultType = resultType,
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

  -- row iterator factory, it is used on a cursor (or row) like:
  --     for row in cur.rows() do
  --         dosomethingwith(row.field(0))
  --         dosomethingelsewith(row.field(1))
  --         ...
  --     end
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

  -- execute the statement
  -- returns a result or an errorcode and message
  local function execute(stmt)
    local rc, r = cnow_execute(self.con, stmt)
    rc = (rc == now.OK) and errcode(r) or rc
    if rc ~= now.OK then
       if r == nil then return rc end
       local msg = (type(r) == 'string') and r or errdetails(r)
       return rc, msg
    end
    return now.OK, makeResult(r)
  end

  -- like execute, but may call error
  local function errexecute(stmt)
    rc, r = execute(stmt)
    if rc ~= now.OK then
       error(rc .. ": " .. r)
    end
    return r
  end

  -- use this database in this session
  local function use(db)
    local stmt = "use " .. db
    local rc, r = execute(stmt)
    if r == nil then return rc end
    rc = rc == now.OK and r.errcode() or rc
    if rc ~= now.OK then
       local msg = (type(r) == 'string') and r or r.errdetails()
       r.release()
       return rc, msg
    end
    return now.OK
  end

  -- the public interface 
  local ifc = {
     close = close,
     use = use, 
     execute = execute,
     errexecute = errexecute
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
     local rc, r = execute(stmt)
     if r == nil then error('no result') end
     if type(r) == 'string' then error(rc .. ": " .. r) end
     rc = (rc == now.OK) and r.errcode() or rc
     if rc ~= now.OK then 
        msg = r.errdetails()
        r.release()
        error(rc .. ": " .. msg)
     end
     return r.rows()
  end

  setmetatable(ifc, {__gc=close})

  return now.OK, ifc
end

-- return the module
return now
