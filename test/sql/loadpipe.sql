create scope test2 if not exists;
use test2;
create tiny context ctx_tiny if not exists;
create big  context ctx_big  if not exists;
load 'rsc/kilo.csv'  into ctx_tiny;
load '/opt/dbs/fifo' into ctx_big;
-- select * from ctx_tiny;
