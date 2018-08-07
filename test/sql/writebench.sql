create scope bench if not exists; use bench;
create big context ctx_bench if not exists;
create big index idx_bench1 on ctx_bench (origin, edge);
create big index idx_bench2 on ctx_bench (destin, edge);
