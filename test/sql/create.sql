drop scope db200 if exists;
create scope db200; use db200;

create table sales;

create type client (
    client_key uint primary key,
    client_name text,
    client_birth time
);

create type product (
    prod_key uint primary key,
    prod_desc text,
    prod_cat  uint,
    prod_packing uint,
    prod_price float
);

create edge buys (
   origin client,
   destin product,
   weight uint,
   weight2 float
);
