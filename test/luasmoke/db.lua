#!/usr/bin/env lua

now = require('now')

local db = {}

db.PRODUCTRANGE = 1000
db.CLIENTRANGE = 9000000

function db.createDB(c,dbname)
  local stmt = "drop schema " .. dbname .. " if exists"
  local rc, res = c.execute(stmt)
  if rc ~= now.OK then
     error("db creation failed: " .. rc .. " (" .. res .. "), stmt: " .. stmt)
  end
  stmt = "create schema " .. dbname
  rc, res = c.execute(stmt)
  if rc ~= now.OK then
     error("db creation failed: " .. rc .. " (" .. res .. "), stmt: " .. stmt)
  end
  rc, res = c.use(dbname)
  if rc ~= now.OK then
     error("db creation failed: " .. rc .. " (" .. res .. "), stmt: " .. stmt)
  end

  rc, res = c.execute([[create type product ( 
                          prod_key uint primary key,
                          prod_desc    text, 
                          prod_cat     uint, 
                          prod_packing uint, 
                          prod_price float)]])
  if rc ~= now.OK then
     error("cannot create type product")
  end

  rc, res = c.execute([[create type product2 ( 
                          prod_key uint primary key, 
                          prod_desc    text, 
                          prod_cat     uint, 
                          prod_packing uint, 
                          prod_price float)]])
  if rc~= now.OK then
     error("cannot create type product")
  end
   
  rc, res = c.execute([[create type client ( 
                          client_key uint primary key,
                          client_name text,
                          birthdate time)]])
  if rc ~= now.OK then
     error("cannot create type client")
  end
   
  rc, res = c.execute([[create type store ( 
                          store_name text primary key, 
                          city text, 
                          address text, 
                          size float)]])
  if rc ~= now.OK then
     error("cannot create type client")
  end
   
  rc, res = c.execute([[create stamped edge buys ( 
                          origin client, 
                          destin product, 
                          quantity uint, 
                          price float)]])
  if rc ~= now.OK then
     error("cannot create edge buys")
  end
   
  rc, res = c.execute([[create stamped edge visits ( 
                          origin client, 
                          destin store, 
                          quantity uint, 
                          price float)]])
  if rc ~= now.OK then
     error("cannot create edge vists")
  end
end

local function randomstring(n)
  s = ""
  m = 0
  while m == 0 do m = math.random(1,n) end
  for i = 1,m+1 do
      k = math.random(0,25) + 65
      s = s ..  utf8.char(k)
  end
  return s
end

local function createProducts(c, ps)
  local prods = {}
  for i = 1,ps+1 do
     local k = db.PRODUCTRANGE + i
     local desc = randomstring(10)
     local cat = math.random(1,3)
     local pack = math.random(1,5)
     local price = math.random() * 5.0
     prods[i] = db.createProduct(k, desc, cat, pack, price)
  end
  return prods
end

function db.addRandomData(c,cfg)
  math.randomseed(os.time())
  products = createProducts(c,cfg.products)

  for k,v in ipairs(products) do
      v.store(c)
  end
end

function db.createProduct(key, desc, cat, pack, price)
  local prod = {key  = key,
                desc = desc,
                cat  = cat,
                pack = pack,
                price = price}

  print("creating product " .. key .. " | " .. desc .. " | " .. price)

  prod.iopen  = [[insert into product (
                  prod_key, prod_desc,
                  prod_cat, prod_packing,
                  prod_price) values (]]

  function prod.store(c)
     stmt = prod.iopen ..
            prod.key .. ', ' ..
            "'" .. prod.desc .. "', " ..
            prod.cat .. ", " ..
            prod.pack .. ", " ..
            prod.price .. ")"
     c.errexecute(stmt)
  end

  function prod.tocsv()
     print("tocsv")
  end

  return prod
end



return db
