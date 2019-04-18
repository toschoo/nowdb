local ipc = require('ipc')
local unid = require('unid')
local recache = require('recache')

function sayhello()
  print('hello')
end

function timetest(t)
  local cur = nowdb.execute([[select count(*) from buys
                               where stamp <= ]] .. t)
  local r = {}
  for row in cur.rows() do
      r = nowdb.makeresult(nowdb.UINT, row.field(0))
  end
  cur.release()
  return r
end

function mycount(tab)
  local sql = string.format("select count(*) from %s", tab)
  local cur = nowdb.execute(sql)
  for row in cur.rows() do
      local r =  nowdb.makeresult(nowdb.UINT, row.field(0))
      cur.release()
      return r
  end
end

local function myadd(t,a,b)
  return nowdb.makeresult(t,a+b)
end

function myfloatadd(a,b)
  return myadd(nowdb.FLOAT, a, b)
end

function myuintadd(a,b)
  return myadd(nowdb.UINT, a, b)
end

function myintadd(a,b)
  return myadd(nowdb.INT, a, b)
end

function mylogic(o,a,b)
  local c = true
  if o == 'and' then
     c = a and b
  elseif o == 'or' then
     c = a or b
  elseif o == 'xor' then
     c = (a and not b) or (not a and b)
  end
  return nowdb.makeresult(nowdb.BOOL, c)
end

local function eastergauss(y)
    local a = y % 19
    local b = y % 4
    local c = y % 7
    local k = y // 100
    local p = (8*k + 13) // 25
    local q = k // 4
    local m = (15 + k - p - q) % 30
    local n = (4 + k - q) % 7
    local d = (19 * a + m) % 30
    local e = (2*b + 4*c + 6*d + n) % 7
    return (22 + d + e)
end

function easter(y)
    local m = 3
    local d = eastergauss(y)
    if d > 31 then
       d = d - 31
       m = m + 1
    end
    local t = nowdb.to(nowdb.date(y,m,d))
    return nowdb.makeresult(nowdb.TIME, t)
end

local function getunique(t)
  local sql = "select max(id) from unique"
  cur = nowdb.execute(sql)
  for row in cur.rows() do
    local x = row.field(0) + 1
    local sql = string.format(
                [[insert into unique (id, desc)
                              values (%d, '%s')]], x, t)
    nowdb.execute_(sql)
    return x
  end
end

function groupbuy(p)
  local sql = string.format(
              [[select coal(sum(quantity), 0),
                       coal(avg(quantity), 0),
                       coal(max(quantity), 0),
                       coal(sum(price), 0),
                       coal(avg(price), 0),
                       coal(max(price), 0)
                  from buys
                 where destin = %d]], p)

  local uid = getunique('groupbuy')
  local t = nowdb.getnow()

  local cur = nowdb.execute(sql)
  for row in cur.rows() do
    local ins = string.format(
                [[insert into result (origin, destin, stamp,
                                      f1, f2, f3, f4, f5, f6)
                              values (%d, %d, %d,
                                      %f, %f, %f, %f, %f, %f)]],
                                      uid, p, t,
                                      row.field(0),
                                      row.field(1),
                                      row.field(2),
                                      row.field(3),
                                      row.field(4),
                                      row.field(5))
    nowdb.execute_(ins)
  end
  cur.release()
  local sql = string.format(
              [[select destin, f1, f2, f3, f4, f5, f6
                  from result where origin = %d]], uid)
  return nowdb.execute(sql)
end

local _fibn1 = 0
local _fibn2 = 1

function fibreset()
  _fibn1 = 0
  _fibn2 = 1
end

function fib()
  local f = _fibn1 + _fibn2
  _fibn1, _fibn2 = _fibn2, f
  return nowdb.makeresult(nowdb.UINT, _fibn1)
end

function locktest(lk)
  math.randomseed(os.time())
  ipc.createlock(lk)
  for i = 1,10 do
      local m = 'w'
      if math.random(1,2) == 1 then m = 'r' end
      local t = math.random(0,10) * 100
      ipc.lock(lk,m,t)
      print(string.format(
        "LOCKED for '%s' set timeout = %d", m, t))
      ipc.unlock(lk)

      ipc.withlock(lk, m, tmo, function()
         print("with lock!")
      end)

      ipc.withxlock(lk,tmo,function()
         print("with xlock!")
      end)
  end
  ipc.droplock(lk)
end

function uniquetest(nm)
  unid.create(nm)
  return nowdb.makeresult(nowdb.UINT, unid.get(nm))
end

local function modvisits(m)
  local stmt = string.format(
    [[select origin, destin, stamp, quantity, price
        from visits where origin %% %d = 0]], m)
  -- print("EXECUTING: " .. stmt)
  local rc, cur = nowdb.pexecute(stmt)
  if rc ~= nowdb.OK then
     if rc == nowdb.EOF then return end
     nowdb.raise(rc, cur)
  end
  for row in cur.rows() do
      coroutine.yield(row)
  end
  cur.release()
end

local function createifnotexists(nm)
  local pld = {}
  pld[1] = {['type'] = nowdb.UINT,
            ['name'] = 'client'}
  pld[2] = {['type'] = nowdb.TEXT,
            ['name'] = 'store'}
  pld[3] = {['type'] = nowdb.TIME,
            ['name'] = 'visit'}
  pld[4] = {['type'] = nowdb.UINT,
            ['name'] = 'quantity'}
  pld[5] = {['type'] = nowdb.FLOAT,
            ['name'] = 'paid'}
  recache.create(nm, pld)
end

function recachetest(nm,m,v)
   createifnotexists(nm)
   local co = coroutine.create(function()
     modvisits(m)
   end)
   local valid = function(rid)
     return v
   end
   return recache.withcache(nm, co, valid, {m})
end

function dropcache(nm)
  recache.drop(nm)
end

