create scope test /* if not exists */; use test;
create context ctx_tiny /* if not exists */;
load 'rsc/kilo.csv' into ctx_tiny;
-- select * from ctx_tiny;
