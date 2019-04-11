local unid = require('unid')
local recache = require('recache')

function sayhello()
  print("hello, db is " .. nowdb._db)
end

function add(a,b)
  local row = nowdb.makeresult(nowdb.FLOAT, a+b)
  return row
end

function fastcount(t)
  local stmt = string.format("select count(*) from %s", t)
  local cur = nowdb.execute(stmt)
  for row in cur.rows() do
      local r = nowdb.makeresult(nowdb.UINT, row.field(0))
      cur.release()
      return r
  end
end

function slowcount(t)
  local stmt = string.format("select * from %s", t)
  local cur = nowdb.execute(stmt)
  local cnt = 0
  for row in cur.rows() do
      cnt = cnt + 1
  end
  cur.release()
  return nowdb.makeresult(nowdb.UINT, cnt)
end

function timetest()
  local n = nowdb.getnow()
  local t = nowdb.from(n)
  local p = nowdb.to(t)
  if p ~= n then
     local m = string.format("to(from(n)) ~= n: %d ~= %d", n, p)
     nowdb.raise(nowdb.USRERR, m)
  end
  local res = nowdb.makerow()
  res.add2row(nowdb.TIME, n)
  res.add2row(nowdb.TEXT, nowdb.timeformat(t))
  res.closerow()
  return res
end

function testunique()
  unid.create('myuni')
  for i = 1,10 do
      print(string.format("%03d: %04d", i, unid.get('myuni')))
  end
  unid.drop('myuni')
end

function testeof()
  nowdb.execute_([[create type myemptytype (
                     id uint primary key)
                   if not exists]])
  local cur = nowdb.execute("select * from myemptytype")
  for row in cur.rows() do
      print("empty: " .. row.field(0))
  end
  cur.release()
  nowdb.execute_("drop type myemptytype")
end

local function generate()
  local cur = nowdb.execute("select price from visits")
  for row in cur.rows() do
      coroutine.yield(row) -- cocursor
  end
  cur.release()
end

function testrecache()
  local pld = {}
  pld[1] = {['name']='field_1', ['type']=nowdb.FLOAT}
  recache.setDebug(true)
  recache.create('mycache', pld)
  local valid = function(rid) return true end
  local generator = nowdb.cocursor("select price from visits")
  cur = recache.withcache('mycache', valid, generator, {1,'test',38.0,-8.0})
  print("RESULT:")
  for row in cur.rows() do
      print(string.format("| %.4f |", row.field(0)))
  end
  recache.drop('mycache')
end
