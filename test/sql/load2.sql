-- we drop the scope should it exist
drop scope db500 if exists;
create scope db500; use db500;
create type sensor (
     id uint primary key
);
create type loc (
     id uint primary key,
     name text,
     lat float,
     lon float
);
create edge measure (
     origin sensor origin,
     destin loc    destin,
     stamp  time    stamp,
     label  int,
     weight int
);
load 'rsc/loc.csv' into loc use header;
load 'rsc/kilo.csv' into measure use header;
-- select * from ctx_tiny;
