create scope test2 if not exists;
use test2;
create big  context ctx_big  if not exists;
create big index idx_big_edgori on ctx_big (edge, origin) if not exists;
-- create big index idx_big_edgdst on ctx_big (edge, destin) if not exists;
load '/opt/dbs/csv/hmillion.csv' into ctx_big;
-- select * from ctx_tiny;
