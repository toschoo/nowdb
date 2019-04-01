#!/usr/bin/env lua

now = require('now')
db = require('db')

local rc, con = now.connect('localhost', '55505', nil, nil)
if rc ~= now.OK then
   error("cannot connect: " .. rc .. " (" .. con .. ")")
end

db.createDB(con, 'db250')
db.addRandomData(con, {products=100,
                       cients = 50,
                       stores = 25,
                       buys = 1000,
                       visits = 150})

