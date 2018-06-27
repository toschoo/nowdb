drop schema ctt if exists;
create schema ctt; use ctt;
create table ctt set stress=constant;
create index idx_ctt_oredg on ctt (origin, edge);
create index idx_ctt_dsedg on ctt (destin, edge);
create index idx_ctt_time  on ctt (timestamp);
load '/opt/dbs/fifo' into ctt;
-- select * from ctt;
