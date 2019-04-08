#!/usr/bin/env lua

--[[
Actually, since "test" and "hello" are literal strings, they aren't collected until the enclosing function's prototype gets collected.
--]]

now = require('now')
db = require('db250')

local rc, con = now.connect('localhost', '55505', nil, nil)
if rc ~= now.OK then
   error("cannot connect: " .. rc .. " (" .. con .. ")")
end

local function fastcount(tab)
  local stmt = string.format("select count(*) from %s", tab)
  local cur = con.errexecute(stmt) 
  local n = 0
  for row in  cur.rows() do
      print(string.format("% 8s: % 5d", tab, row.field(0)))
      n = row.field(0)
  end
  cur.release()
  return n
end

local function slowcount(tab)
  local stmt = string.format("select * from %s", tab)
  local cnt=0
  local cur = con.errexecute(stmt)
  for row in cur.rows() do cnt = cnt + 1 end
  cur.release()
  return cnt
end

while true do
-- for i = 1,100 do
  -- print("iteration " .. i)
  db.createDB(con, 'db250')
  local pd, cl, sr, bu, vs = db.addRandomData(con, {products=100,
                                                    clients = 50,
                                                    stores = 25,
                                                    buys = 1000,
                                                    visits = 150})

  -- count
  if fastcount("client") ~= slowcount("client") then
     error("client count differs")
  end
  if fastcount("product") ~= slowcount("product") then
     error("product count differs")
  end
  if fastcount("store") ~= slowcount("store") then
     error("store  count differs")
  end
  if fastcount("buys") ~= slowcount("buys") then
     error("buys   count differs")
  end
  if fastcount("visits") ~= slowcount("visits") then
     error("visits count differs")
  end
  pd=nil;cl=nil;sr=nil;bu=nil;vs=nil
  collectgarbage("collect")
  print(collectgarbage("count"))
end
