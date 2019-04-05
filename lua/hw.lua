
function sayhello()
  print("hello, db is " .. _nowdb_db)
end

function add(a,b)
  local rc, row = nowdb_makeresult(nowdb_FLOAT, a+b)
  if rc ~= nowdb_OK then return nowdb_error(rc, row) end
  return row
end

function icancount(t)
  local stmt = string.format("select count(*) from %s", t)
  local rc, cur = nowdb_execute(stmt)
  if rc ~= nowdb_OK then return nowdb_error(rc, cur) end
  for row in cur.rows() do
      local rc, r = nowdb_makeresult(nowdb_UINT, row.field(0))
      cur.release()
      if rc ~= nowdb_OK then return nowdb_error(rc, r) end
      return r
  end
end
