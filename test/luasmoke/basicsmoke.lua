#!/usr/bin/env lua

now = require('now')
db = require('db')

local rc, con = now.connect('localhost', '55505', nil, nil)
if rc ~= now.OK then
   error("cannot connect: " .. rc .. " (" .. con .. ")")
end

db.createDB(con, 'db250')
db.addRandomData(con, {products=100,
                       clients = 50,
                       stores = 25,
                       buys = 1000,
                       visits = 150})

local function fastcount(tab)
  local stmt = string.format("select count(*) from %s", tab)
  for row in con.rows(stmt) do
      print(string.format("% 8s: % 5d", tab, row.field(0)))
      return row.field(0)
  end
end
local function slowcount(tab)
  local stmt = string.format("select * from %s", tab)
  local cnt=0
  for row in con.rows(stmt) do cnt = cnt + 1 end
  return cnt
end

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
