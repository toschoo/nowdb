-- we drop the scope should it exist
drop scope db500 if exists;
create scope db500; use db500;
create tiny context ctx_measure;
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
     origin sensor,
     destin loc,
     label  uint,
     weight uint,
     weight2 uint
);
load 'rsc/kilo.csv' into ctx_measure;
load 'rsc/loc.csv' into loc use header;
-- select * from ctx_tiny;
