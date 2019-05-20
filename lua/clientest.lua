local now = require('now')

local rc, c = now.connect('localhost', '55505', nil, nil)
if rc ~= now.OK then
   error('cannot connect: ' .. c .. ' (' .. tostring(rc) .. ')')
end

local rc, msg = c.use('db150')
if rc ~= now.OK then
   error('cannot use db150')
end

local rc, cur = c.execute("select prod_key, prod_desc from product")
if rc ~= now.OK then
   error('cannot execute: ' .. cur)
end

print("number of fields: " .. cur.countfields())
print("curid: " .. cur.getid())
print("------------------")
print("key  | description")
print("------------------")
local cnt = 0
while true do
  print(tostring(cur.field(0)) .. " | " .. tostring(cur.field(1)))
  cnt = cnt + 1
  local rc, msg = cur.nextrow()
  if rc ~= now.OK then
     if rc == now.EOF then
        rc, msg = cur.fetch()
        if rc ~= now.OK then
           if rc ~= now.EOF then 
              error('cannot fetch: ' .. msg)
           end
        end
     else
        print(msg) 
     end
     break
  end
end
print("read: " .. cnt)

rc, cur = c.execute("select prod_key, prod_desc from product")
if rc ~= now.OK then
   error('cannot execute: ' .. cur)
end

cnt = 0
print("curid: " .. cur.getid())
print("------------------")
print("key  | description")
print("------------------")
for row in cur.rows() do
  print(tostring(row.field(0)) .. " | " .. tostring(row.field(1)))
  cnt = cnt + 1
end
print("read: " .. cnt)

cnt = 0
print("------------------")
print("key  | description")
print("------------------")
for row in c.rows("select prod_key, prod_desc from product") do
  print(tostring(row.field(0)) .. " | " .. tostring(row.field(1)))
  cnt = cnt + 1
end
print("read: " .. cnt)

rc, cur = c.execute("this is not sql")
if rc ~= now.OK then
   print(rc .. ': ' .. cur)
end

for row in c.rows("no sql either") do
  print(tostring(row.field(0)) .. " | " .. tostring(row.field(1)))
  cnt = cnt + 1
end

c = nil
collectgarbage()
