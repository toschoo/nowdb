create scope bench if not exists; use bench;
create big context ctx_bench if not exists;
load 'rsc/million.csv' into ctx_bench;
