
function sayhello()
  print("hello, db is " .. _nowdb_db)
end

function add(a,b)
  local rc, row = nowdb_makeresult(nowdb_FLOAT, a+b)
  if rc ~= nowdb_OK then return nowdb_error(rc, row) end
  return row
end
