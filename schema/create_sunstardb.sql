create table star
  (id			serial		not null,
   hd			integer			,
   proper		varchar(32)		,
   bayer		varchar(32)		,
   flamsteed		varchar(32)		,	
   simbad		varchar(32)		,
   insert_time		timestamp	not null,
   --
   constraint pk_star
     primary key (id),
   --
   constraint uq_star_hd
     unique (hd),
   --
   constraint uq_proper
     unique (proper),
   --
   constraint uq_bayer
     unique (bayer),
   --
   constraint uq_flamsteed
     unique (flamsteed),
   --
   constraint uq_simbad
     unique (simbad)
  );

create table property_type
  (id			serial		not null,
   name			varchar(32)	not null,
   description		text		not null,
   --
   constraint pk_property_type
     primary key (id),
   --
   constraint uq_property_type_name
     unique (name)
  );

create table reference
  (id			serial		not null,
   name			varchar(64)	not null, -- a short name, e.g. Egeland et al. 2014
   bibline		text		not null, -- full bibliography line
   ads_url		varchar(1024)	not null, -- ADS entry
   insert_time		timestamp	not null,
   --
   constraint pk_reference
     primary key (id),
   --
   constraint uq_reference_name
     unique (name)
  );

create table origin
  (id			serial		not null,
   name			varchar(64)	not null, -- short, descriptive name e.g. Lowell Observatory
   kind			varchar(32)	not null,
   url			varchar(1024)		,
   doc_url		varchar(1024)	not null,
   description		text		not null,
   insert_time		timestamp	not null,
   --
   constraint pk_origin
     primary key (id),
   --
   constraint uq_origin_name
     unique (name)
   -- TODO enumerate valid 'kind'
  );

create table source
  (id			serial		not null,
   origin		integer		not null,
   kind			varchar(32)	not null,
   source		integer			,
   name			varchar(1024)	not null,
   version		varchar(32)		,
   modify_time		timestamp	not null,
   insert_time		timestamp	not null,
   --
   constraint pk_source
     primary key (id),
   --
   constraint uq_source_version
     unique (name, version),
   --
   constraint fk_source_origin
     foreign key (origin) references origin (id)
   -- TODO enumerate valid 'kind'
  );

-- TODO: use Postgres range type for errors?
create table property
  (id			serial		not null,
   star			integer		not null,
   type			integer		not null,
   source		integer		not null,
   reference		integer		not null,
   value		double precision not null,
   err_pos		double precision	,
   err_neg		double precision 	,
   meta			json			,
   obs_time		timestamp	not null,
   insert_time		timestamp	not null,
   meta_time		timestamp		,
   --
   constraint pk_property
     primary key (id),
   --
   constraint fk_property_star
     foreign key (star) references star (id),
   --
   constraint fk_property_type
     foreign key (type) references property_type (id),
   --
   constraint fk_property_source
     foreign key (source) references source (id),
   --
   constraint fk_property_reference
     foreign key (reference) references reference (id)
  );

create table profile
  (id			serial		not null,
   name			varchar(64)	not null,
   description		text		not null,
   insert_time		timestamp	not null,
   --
   constraint pk_profile
     primary key (id),
   --
   constraint uq_profile_name
     unique (name)
  );

create table profile_map
  (profile		integer		not null,
   star			integer		not null,
   type			integer		not null,
   property		integer		not null,
   --
   constraint pk_profile_map
     primary key (profile, star, type, property)
  );

create table timeseries
  (id			serial		not null,
   star			integer		not null,
   type			integer		not null,
   source		integer		not null,
   reference		integer		not null,
   meta			json		not null,
   insert_time		timestamp	not null,
   append_time		timestamp	not null,
   meta_time		timestamp	not null,
   --
   constraint pk_timeseries
     primary key (id),
   --
   constraint fk_timeseries_star
     foreign key (star) references star (id),
   --
   constraint fk_timeseries_type
     foreign key (type) references property_type (id),
   --
   constraint fk_timeseries_source
     foreign key (source) references source (id),
   --
   constraint fk_timeseries_reference
     foreign key (reference) references reference (id)
  );

-- TODO: use Postgres range type for errors?
create table timeseries_data
  (timeseries		integer		not null,
   obs_time		timestamp	not null,
   value		double precision not null,
   err_pos		double precision	,
   err_neg		double precision	,
   insert_time		timestamp	not null,
   --
   constraint pk_timeseries_data
     primary key (timeseries, obs_time),
   --
   constraint fk_timeseries_data_timeseries
     foreign key (timeseries) references timeseries (id)
  );
