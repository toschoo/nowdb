create scope test2 if not exists;
use test2;
create big  context ctx_big  if not exists;
load '/opt/dbs/csv/hmillion.csv' into ctx_big;
-- select * from ctx_tiny;
