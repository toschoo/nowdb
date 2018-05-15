drop scope test if exists;
create scope test; use test;
load 'rsc/vertex.csv' into vertex;
drop context ctx_tiny if exists;
create tiny context ctx_tiny;
load 'rsc/kilo.csv' into ctx_tiny;
-- select * from ctx_tiny;
