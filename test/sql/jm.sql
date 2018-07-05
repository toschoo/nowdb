drop schema jm if exists;
create schema jm; use jm;

create large table tx;
create table baskets;
create table weekpeak;
create table stats;

create type product (
	area_code uint,
	area_desc text,
	div_code uint,
	div_desc text,
	family_code uint,
	family_desc text,
	cat_code uint,
	cat_desc text,
	subcat_code uint,
	subcat_desc text,
	product_key uint,
	product_desc text,
	brand_code uint,
	brand_desc text,
	product_type_code uint,
	product_type_desc text,
	brand_type text,
	product_weight float
);
create type store (
	store_key uint,
	store_name text,
	org_code text,
	org_desc text,
	cluster_area uint,
	store_type text,
	zip_code text,
	region text,
	director text,
	manager text,
	sales_area float,
	x float,
	y float
);
create type client (
	account_id uint,
	card_key uint,
	birthdate date,
	gender text,
	city text,
	cep_4 uint,
	cep_3 uint,
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
