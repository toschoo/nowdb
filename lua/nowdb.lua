
_nowdb_db = 0

function _nowdb_setDB(db)
  _nowdb_db = db
end

---------------------------------------------------------------------------
-- special errors
---------------------------------------------------------------------------
nowdb_OK = 0
nowdb_EOF = 8
nowdb_NOMEM = 1
nowdb_TOOBIG = 5
nowdb_NOTACUR = -10
nowdb_NOTAROW = -11

---------------------------------------------------------------------------
-- result types
---------------------------------------------------------------------------
nowdb_NOTHING = 0
nowdb_STATUS = 33
nowdb_REPORT = 34
nowdb_ROW = 35
nowdb_CURSOR = 36

---------------------------------------------------------------------------
-- static types
---------------------------------------------------------------------------
nowdb_TEXT = 1
nowdb_DATE = 2
nowdb_TIME = 3
nowdb_FLOAT= 4
nowdb_INT  = 5
nowdb_UINT = 6
nowdb_BOOL = 9

---------------------------------------------------------------------------
-- End of Row
---------------------------------------------------------------------------
nowdb_EOR = 10

---------------------------------------------------------------------------
-- Time handling
---------------------------------------------------------------------------
nowdb_second = 1000000000
nowdb_minute = 60*nowdb_second
nowdb_hour   = 60*nowdb_minute
nowdb_day    = 24*nowdb_hour
nowdb_year   = 365*nowdb_day

function nowdb_round(t, d)
  return d*math.floor(t/d)
end

function nowdb_timeformat(t)
  return string.format('%04d-%02d-%02dT%02d:%02d:%02d.%d',
                       t.year, t.month, t.day, 
                       t.hour, t.min, t.sec, t.nano)
end

function nowdb_dateformat(t)
  return string.format('%04d-%02d-%02d',
                       t.year, t.month, t.day)
end

local function errdetails(r)
  return now2lua_errdetails(r)
end

local function errcode(r)
  return now2lua_errcode(r)
end

local function mkResult(r)
  local self = {cr=r}

  -- check if healthy
  local function ok()
    if not self.cr then return end
    return now2lua_errcode(self.cr) == nowdb_OK
  end

  -- is end of file
  local function eof()
    if not self.cr then return end
    return (now2lua_errcode(self.cr) == nowdb_EOF)
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
    if not self.cr then return nowdb_Nothing end
    return now2lua_rtype(self.cr)
  end

  -- get cursor id (if cursor)
  local function getid()
    if not self.cr then return end
    return now2lua_curid(self.cr)
  end

  -- fetch from cursor (if cursor)
  local function fetch()
    if resulttype() ~= nowdb_CURSOR
    then
       return nowdb_NOTACUR, 'result is not a cursor'
    end
    local rc = now2lua_fetch(self.cr)
    rc = (rc == nowdb_OK) and errcode() or rc
    if rc ~= nowdb_OK then
       if rc == nowdb_EOF then return rc, nil end
       return rc, errdetails(self.cr)
    end
    return nowdb_OK
  end

  -- go to next row
  local function nextrow()
    local r = resulttype()
    if r ~= nowdb_CURSOR and r ~= nowdb_ROW
    then
       return nowdb_NOTACUR
    end
    local rc = now2lua_nextrow(self.cr)
    rc = (rc == nowdb_OK) and errcode() or rc
    if rc ~= nowdb_OK then return rc end
    return nowdb_OK
  end

  -- open cursor
  local function open()
    if self.opened then return nowdb_OK end
    if resulttype() ~= nowdb_CURSOR then return nowdb_NOTACUR end
    now2lua_open(self.cr)
    self.opened = true
    return nowdb_OK
  end

  -- iterator (see below)
  local function _rows()
    if not self.opened then 
       rc = open()
       if rc ~= nowdb_OK then return nil end
       fetch()
       if not ok() then return nil end
    end
    if self.neednext then
       local rc = nextrow()
       if rc ~= nowdb_OK then
          if rc == nowdb_EOF then
             if fetch() ~= nowdb_OK then return nil end
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
    if r ~= nowdb_CURSOR and r ~= nowdb_ROW
    then
       return nowdb_NOTHING, nil
    end
    return now2lua_countfields(self.cr)
  end

  -- type conversion is easy
  local function conv(t,v)
    if t == nowdb_NOTHING then return nil else return v end
  end

  -- get ith field from row (or cursor)
  local function field(i)
    local r = resulttype()
    if r ~= nowdb_CURSOR and r ~= nowdb_ROW
    then
       return nil
    end
    local t, v = now2lua_field(self.cr, i)
    print(string.format("have field of type %d: ", t) .. tostring(v))
    return conv(t,v)
  end

  local function add2row(t,v)
    if resulttype() ~= nowdb_ROW then return nowdb_NOTAROW end
    local x = now2lua_rowcapacity(self.cr)
    if t == nowdb_TEXT then 
       if len(v) >= x then return nowdb_TOOBIG, "text too big for row" end
    else
       if x <= 8 then return nowdb_TOOBIG, "no space left in row" end
    end
    return now2lua_add2row(self.cr,t,v)
  end

  local function closerow()
    if resulttype() ~= nowdb_ROW then return nowdb_NOTAROW end
    return now2lua_closerow(self.cr)
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

  local ifc = {
    ok = ok,
    eof = eof,
    errcode = errcode,
    errdetails = errdetails,
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

  function ifc.toDB()
     local r = self.cr
     self.cr = nil
     return r
  end

  setmetatable(ifc, {__gc = release})
  return ifc
end

function nowdb_success()
  return mkResult(now2lua_success())
end

function nowdb_error(rc, msg)
   return mkResult(now2lua_error(rc, msg))
end

function nowdb_makerow()
   rc, row = now2lua_makerow()
   if rc ~= nowdb_OK then return rc, row end
   return nowdb_OK, mkResult(row)
end

function nowdb_makeresult(t,v)
   local rc, row = nowdb_makerow()
   if rc ~= nowdb_OK then return rc, row end
   local rc, msg = row.add2row(t,v)
   if rc ~= nowdb_OK then return rc, msg end
   local rc, msg = row.closerow()
   if rc ~= nowdb_OK then return rc, msg end
   return nowdb_OK, row
end

function nowdb_execute(stmt)
  local rc, r = now2lua_execute(_nowdb_db, stmt)
  if rc ~= nowdb_OK then
     if r == nil then return rc, "unknown error" end
     local msg = ''
     if type(r) == 'string' then
        msg = r 
     else
        msg = errdetails(r)
        now2lua_release(r)
     end
     return rc, msg
  end
  return nowdb_OK, mkResult(r)
end

-- like execute, but may call error
function nowdb_errexecute(stmt)
  rc, r = execute(stmt)
  if rc ~= nowdb_OK then
     error(rc .. ": " .. r)
  end
  return r
end

-- like errexecute, but does not return the result
function nowdb_pexecute(stmt)
  rc, r = execute(stmt)
  if rc ~= nowdb_OK then
    error(rc .. ": " .. r)
  end
  if r ~= nil then
     r.release()
  end
end

function nowdb_from(t)
  local frm  = [[select year(%d), month(%d), mday(%d),
                        wday(%d), yday(%d),
                        hour(%d), minute(%d), second(%d),
                        nano(%d)]]
  local stmt = string.format(frm, t, t, t, t, t, t, t, t, t)
  local rc, cur = nowdb_execute(stmt)
  if rc ~= nowdb_OK then
     return rc, cur
  end
  local r = {}
  for row in cur.rows() do
      r.year = row.field(0)
      r.month = row.field(1)
      r.day = row.field(2)
      r.wday = row.field(3) + 1
      r.yday = row.field(4)
      r.hour = row.field(5)
      r.min = row.field(6)
      r.sec = row.field(7)
      r.nano =row.field(8) 
  end
  cur.release()
  return nowdb_OK, r
end

function nowdb_to(t)
  local frm = "select '%s'"
  local stmt = string.format(frm, nowdb_timeformat(t))
  local rc, cur = nowdb_execute(stmt)
  if rc ~= nowdb_OK then
     return rc, cur
  end
  for row in cur.rows() do
      r = row.field(0)
  end
  cur.release()
  return nowdb_OK, r
end

function nowdb_getnow()
  local rc, cur = nowdb_execute("select now()")
  if rc ~= nowdb_OK then
     return nowdb_OK, cur
  end
  local n = 0
  for row in cur.rows() do
      n = row.field(0)
  end
  cur.release()
  return nowdb_OK, n
end

function _nowdb_caller(callee, ...)
  print("LUA CALLER")
  local r = callee(...)
  if r == nil then return now2lua_success() end
  if r.toDB ~= nil then return r.toDB() end
  return now2lua_error(nowdb_USRERR, "unexpected result type")
end
