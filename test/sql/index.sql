create schema test if not exists; use test;
create table ctx_tiny if not exists;
drop index idx_on_tiny_oredge if exists;
create index idx_on_tiny_oredge on ctx_tiny (origin, edge);
load 'rsc/kilo.csv' into ctx_tiny;
select * from ctx_tiny;
