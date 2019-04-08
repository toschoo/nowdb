#!/usr/bin/env lua

now = require('now')
db = require('db')

local rc, c = now.connect('localhost', '55505', nil, nil)
if rc ~= now.OK then
   error("cannot connect: " .. rc .. " (" .. c .. ")")
end

c.use("db250")

c.execute_("drop procedure fastcount if exists")
c.execute_("create procedure hw.fastcount(t text) language lua ")
c.execute_("drop procedure slowcount if exists")
c.execute_("create procedure hw.slowcount(t text) language lua ")

function mycount()
  cur = c.errexecute("exec fastcount('buys')")
  local f = 0
  for row in cur.rows() do
    f = row.field(0)
  end
  cur.release()
  cur = c.errexecute("exec slowcount('buys')")
  local s = 0
  for row in cur.rows() do
    s = row.field(0)
  end
  cur.release()
  if s ~= f then error("results differ") end
  return s
end

while true do
  print(mycount())
  -- collectgarbage("collect")
  print(collectgarbage("count"))
end
