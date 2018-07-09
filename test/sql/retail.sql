drop schema retail if exists;
create schema retail; use retail;

create large table tx;
create table baskets;
create table weekpeak;
create table stats;

create type product (
	product_key uint primary key,
	product_desc text,
	family text,
	category text,
	subcat text,
	brand text,
	product_type text,
	brand_type text,
	product_weight float
);
create type store (
	store_key uint primary key,
	store_name text,
	store_group text,
	cluster_area uint,
	store_type text,
	zip_code text,
	region text,
	director text,
	manager text,
	sales_area float,
	coord_x float,
	coord_y float
);
create type client (
	card_key uint primary key,
	account_id uint,
	age uint,
	gender text,
	city text,
	province text,
	country text,
	registration date,
	status text,
	household_members uint,
	email text,
	phone text,
	mobile text
);
create edge buys (
	origin client,
	dest product,
	weight float,
	label text
);
create edge tx (
	origin client,
	dest store,
	weight float,
	label text
);
create edge quantity (
	origin client,
	dest product,
	weight float,
	label text
);
load '/opt/dbs/products.csv' into vertex use header as product;
load '/opt/dbs/clients.csv' into vertex use header as client;
select * from vertex;
