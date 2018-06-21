create scope test if not exists; use test;
create index idx_on_vertex_rovi on vertex (role,vid) if not exists;
load 'rsc/vertex.csv' into vertex;
