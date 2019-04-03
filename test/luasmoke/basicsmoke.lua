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

-- time
local function weekday(n)
  if     n == 1 then return 'Sun'
  elseif n == 2 then return 'Mon'
  elseif n == 3 then return 'Tue'
  elseif n == 4 then return 'Wed'
  elseif n == 5 then return 'Thu'
  elseif n == 6 then return 'Fri'
  else               return 'Sat'
  end
end

local rc, n = con.getnow()
if rc ~= now.OK then
   error("cannot get now: " .. rc .. " (" .. n .. ")")
end
local rc, t = con.fromnow(n)
if rc ~= now.OK then
   error("cannot convert from now: " .. rc .. " (" .. t .. ")")
end

local f = string.format('%04d-%02d-%02dT%02d:%02d:%02d.%d',
                           t.year, t.month, t.day, 
                           t.hour, t.min, t.sec,
                           math.floor(t.nano/1000000))

print(f .. " was a " .. weekday(t.wday) .. ", " .. t.yday .. ". day of the year")

print(now.timeformat(t))
print(now.dateformat(t))

local rc, m = con.tonow(t)
if rc ~= now.OK then
   error("cannot convert to now: " .. rc .. " (" .. m .. ")")
end

print(string.format("%d ?= %d", n, m))

if n ~= m then
   error("tonow(fromnow(n)) is not n")
end

