drop schema test if exists;
create schema test; use test;
drop table ctx_tiny if exists;
create table ctx_tiny;
load 'rsc/kilo.csv' into ctx_tiny;
-- select * from ctx_tiny;
