now = require('now')

local db = {}

db.PRODUCTRANGE = 1000
db.CLIENTRANGE = 9000000
db.STORERANGE = 100

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
  res.release()
end

local function randomstring(n)
  local s = ""
  local m = 0
  while m == 0 do m = math.random(1,n) end
  for i = 1,m+1 do
      local k = math.random(0,25) + 65
      s = s ..  utf8.char(k)
  end
  return s
end

local function randomdate(c, years)
  local y = math.random(6,70) * now.year
  local rc, n = c.getnow()
  local d = math.random(1,now.year)
  return now.round(n+d-y, now.day)
end

local function randomtime(c, days)
  local d = math.random(0,days) * now.day
  local x = math.random(0,now.day)
  local rc, n = c.getnow()
  return n-(x+d)
end

local function createProducts(c, ps)
  local prods = {}
  for i = 1,ps do
     local k = db.PRODUCTRANGE + i
     local desc = randomstring(10)
     local cat = math.random(1,3)
     local pack = math.random(1,5)
     local price = math.floor(100*(math.random() * 5.0))/100.0
     prods[i] = db.createProduct(k, desc, cat, pack, price)
  end
  return prods
end

local function createClients(c, cs)
  local clients = {}
  for i = 1,cs do
      local k = db.CLIENTRANGE + i
      local cname = randomstring(6)
      local sname = randomstring(9)
      local bd = randomdate(c,6)
      clients[i] = db.createClient(k, cname .. ' ' .. sname, bd)
  end
  return clients
end

local function createStores(c, s)
  local stores = {}
  for i = 1,s do
      local city = randomstring(9)
      local k = string.sub(city,1,1) .. '-' .. tostring(db.STORERANGE + i)
      local address = randomstring(20)
      local size = math.random()*50.0
      stores[i] = db.createStore(k, city, address, size)
  end
  return stores 
end

local function createEdges(c, clients, prods, stores, es, vs)
  local bdg = {}
  local vdg = {}
  for i = 1,es do
      local cl = clients[math.random(1,#clients)]
      local pr = prods[math.random(1,#prods)]
      local tp = randomtime(c,60)
      local q = math.random(1,10)
      bdg[i] = db.createBuy(cl, pr, tp, q)
      if i <= vs then
         local sr = stores[math.random(1,#stores)]
         vdg[i] = db.createVisit(cl, pr, sr, tp, q)
      end
  end
  return bdg, vdg 
end

function db.addRandomData(c,cfg)
  math.randomseed(os.time())

  local products = createProducts(c,cfg.products)
  local clients  = createClients(c,cfg.clients)
  local stores   = createStores(c,cfg.stores)
  local bgd, vgd = createEdges(c,clients, products, stores, cfg.buys, cfg.visits)

  for k,v in ipairs(products) do
      v.store(c)
  end
  for k,v in ipairs(clients) do
      v.store(c)
  end
  for k,v in ipairs(stores) do
      v.store(c)
  end
  for k,v in ipairs(bgd) do
      v.store(c)
  end
  for k,v in ipairs(vgd) do
      v.store(c)
  end

  return products, clients, stores, bgd, vgd
end

local piopen  = [[insert into product (
                    prod_key, prod_desc,
                    prod_cat, prod_packing,
                    prod_price) values (]]

function db.createProduct(key, desc, cat, pack, price)
  local self = {key  = key,
                desc = desc,
                cat  = cat,
                pack = pack,
                price = price}

  -- print("creating product " .. key .. " | " .. desc .. " | " .. price)

  function self.store(c)
     local stmt = piopen ..
           self.key .. ', ' ..
           "'" .. self.desc .. "', " ..
           self.cat .. ", " ..
           self.pack .. ", " ..
           self.price .. ")"
     c.pexecute(stmt)
  end

  function self.tocsv()
     print("tocsv")
  end

  return self
end

local ciopen  = [[insert into client (
                  client_key, client_name, birthdate)
                  values (]]

function db.createClient(key, name, birthdate)
  local self = {key  = key,
                name = name,
                birthdate = birthdate}

  function self.store(c)
     local stmt = ciopen ..
                  self.key .. ', ' ..
                  "'" .. self.name .. "', " ..
                  self.birthdate .. ")"
     c.pexecute(stmt)
  end

  function self.tocsv()
     print("tocsv")
  end

  return self
end

local siopen  = [[insert into store (
                  store_name, city, address, size)
                  values (]]

function db.createStore(name, city, address, size)
  local self = {name = name,
                city = city, 
                address = address, 
                size = size}

  function self.store(c)
     local stmt = siopen ..
                  "'" .. self.name .. "', " ..
                  "'" .. self.city .. "', " ..
                  "'" .. self.address .. "', " ..
                  self.size .. ")"
     c.pexecute(stmt)
  end

  function self.tocsv()
     print("tocsv")
  end

  return self
end


local biopen  = [[insert into buys (
                  origin, destin, stamp, quantity, price)
                  values (]]

function db.createBuy(c,p,tp,q)
  local self = {client = c.key,
                product = p.key,
                stamp = tp,
                quant = q,
                price = q*p.price}

  function self.store(c)
     local stmt = biopen ..
                  self.client .. ", " ..
                  self.product .. ", " ..
                  self.stamp .. ", " ..
                  self.quant.. ", " ..
                  self.price .. ")" 
     c.pexecute(stmt)
  end

  function self.tocsv()
     print("tocsv")
  end

  return self
end

local viopen  = [[insert into visits (
                  origin, destin, stamp, quantity, price)
                  values (]]

function db.createVisit(c,p,s,tp,q)
  local self = {client = c.key,
                shop   = s.name,
                stamp = tp,
                quant = q,
                price = q*p.price}

  function self.store(c)
     local stmt = viopen ..
                  self.client .. ", " ..
                  "'" .. self.shop .. "', " ..
                  self.stamp .. ", " ..
                  self.quant.. ", " ..
                  self.price .. ")" 
     c.pexecute(stmt)
  end

  function self.tocsv()
     print("tocsv")
  end

  return self
end

return db
