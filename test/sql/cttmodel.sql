drop schema cttmodel if exists;
create schema cttmodel; use cttmodel;

create table ctt set stress=constant;
create index idx_ctt_oredg on ctt (origin, edge);
create index idx_ctt_dsedg on ctt (destin, edge);
create index idx_ctt_time  on ctt (timestamp);

create type Actor (
	id text primary key,
	actor_type text 
);

create edge AZ_2D (
	origin Actor,
	destination Actor,
	weight int
);
create edge AZ_D (
	origin Actor,
	destination Actor,
	weight int
);
create edge AZ_E (
	origin Actor,
	destination Actor,
	weight int
);
create edge DEV_2D (
	origin Actor,
	destination Actor,
	weight int
);
create edge DEV_E (
	origin Actor,
	destination Actor,
	weight int
);
create edge N_2D (
	origin Actor,
	destination Actor,
	weight int
);
create edge N_D (
	origin Actor,
	destination Actor,
	weight int
);
create edge N_DA (
	origin Actor,
	destination Actor,
	weight int
);
create edge N_E (
	origin Actor,
	destination Actor,
	weight int
);
create edge N_REC (
	origin Actor,
	destination Actor,
	weight int
);
create edge N_SEQ (
	origin Actor,
	destination Actor,
	weight int
);
create edge R_D (
	origin Actor,
	destination Actor,
	weight int
);
create edge R_E (
	origin Actor,
	destination Actor,
	weight int
);

load '/opt/data/ctt/ctt.actor.csv' into vertex use header as Actor;
load '/opt/data/ctt/ctt.model.10.1.csv' into ctt as edge;
-- select * from vertex;
-- select * from ctt;
