-- we drop the scope should it exist
drop scope test if exists;
create scope test; use test;
drop context ctx_tiny if exists;
create tiny context ctx_tiny;
load 'rsc/kilo.csv' into ctx_tiny;
-- select * from ctx_tiny;
