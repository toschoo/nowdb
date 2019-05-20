#!/usr/bin/env lua

now = require('now')
db = require('db')

local rc, con = now.connect('localhost', '55505', nil, nil)
if rc ~= now.OK then
   error("cannot connect: " .. rc .. " (" .. con .. ")")
end
