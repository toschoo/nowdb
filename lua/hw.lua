
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
