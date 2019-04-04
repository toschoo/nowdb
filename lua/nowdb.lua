
nowdb_db = 0

function _setDB(db)
  print("LUA greets NoWDB (" .. db .. ")!")
  nowdb_db = db
end

function nowdb_caller(callee, ...)
  r = callee(...)
  -- on r == nil return success
  if r ~= nil and r.toDB ~= nil then
     return r.toDB()
  end
end
