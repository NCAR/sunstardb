create table star
  (id			serial		not null,
   hd			integer			, -- Henry Draper catalog name, Simbad 'HD'
   bright		varchar(32)		, -- Simbad '*', bright star name: Bayer or Flamsteed
   proper		varchar(32)		, -- Simbad 'NAME', proper name
   insert_time		timestamp	not null default current_timestamp,
   --
   constraint pk_star
     primary key (id),
   --
   constraint uq_star_hd
     unique (hd),
   --
   constraint uq_proper
     unique (bright),
   --
   constraint uq_bayer
     unique (proper)
  );

-- Reference to published work using data and/or describing accumulation of it
create table reference
  (id			serial		not null,
   name			varchar(64)	not null, -- a short name, e.g. Egeland et al. 2014
   bibline		text		not null, -- full bibliography line
   bibcode		varchar(1024)	not null, -- ADSABS bibcode
   insert_time		timestamp	not null default current_timestamp,
   --
   constraint pk_reference
     primary key (id),
   --
   constraint uq_reference_name
     unique (name)
  );

-- Origin of a data source; An authorative source of data
create table origin
  (id			serial		not null,
   name			varchar(64)	not null, -- short, descriptive name e.g. SIMBAD
   kind			varchar(32)	not null check (kind in ('RAW', 'VO', 'WEB', 'PAPER')),
   url			varchar(1024)		, -- link to website, VO, or adsabs
   doc_url		varchar(1024)	not null, -- internal documentation URL
   description		text		not null, -- short paragraph describing origin
   insert_time		timestamp	not null default current_timestamp,
   --
   constraint pk_origin
     primary key (id),
   --
   constraint uq_origin_name
     unique (name)
  );

-- Source of a stellar property data.  Data either comes from a data FILE or a CODE
create table source
  (id			serial		not null,
   kind			varchar(32)	not null check (kind in ('FILE', 'CODE')),
   url			varchar(1024)	not null, -- URL to the source file (ideally local with DB)
   version		varchar(32)	    	, -- version of the source (for CODE, possibly for FILE)
   origin		integer		not null, -- where this source originated
   source		integer			, -- source of the source, e.g. file input to code
   source_time		timestamp	not null, -- modify time of the source at time of insert
   insert_time		timestamp	not null default current_timestamp,
   --
   constraint pk_source
     primary key (id),
   --
   constraint uq_source_url
     unique (url, version),
   --
   constraint fk_source_origin
     foreign key (origin) references origin (id)
  );

-- Observational instrument a property is derived from
create table instrument
  (id			serial		not null,
   name			varchar(32)	not null, -- short name, e.g. MWO-HK
   long			varchar(64)	not null, -- long name, e.g. Mount Wilson Observatory HK Project
   url			varchar(1024)		, -- canonical URL to instrument information
   doc_url		varchar(1024)	not null, -- internal documentation URL
   description		text		not null, -- short paragraph describing the instrument
   insert_time		timestamp	not null default current_timestamp,
   --
   constraint pk_instrument
     primary key (id),
   --
   constraint uq_instrument_name
     unique (name)
  );

-- Data column descriptor
create table property_type
  (id			serial		not null,
   name			varchar(32)	not null, -- name of property, e.g. MWO-HK_S
   type			varchar(32)	not null, -- e.g. MEASURE, STRING, BOUND, RANGE
   units		varchar(32)		, -- physical units.  NULL when property is non-numeric or uniteless
   description		text		not null, -- paragraph describing the property
   --
   constraint pk_property_type
     primary key (id),
   --
   constraint uq_property_type_name
     unique (name)
  );


-- Property: a single measurement on or classification of a star
create table property
  (id			serial		not null,
   star			integer		not null,
   type			integer		not null,
   source		integer		not null,
   reference		integer		not null,
   instrument		integer			,
   val			double precision        , -- reported value (when property is numeric)
   err			numrange		, -- error arround reported value (range type)
   strval		varchar(64)		, -- reported value when property is of string type
   obs_time		timestamp		, -- instant of time that measurement was taken
   int_time		tsrange			, -- time range over which individual measurements -> this meas.
   meta			json			, -- metadata associated with this measurement
   meta_time		timestamp	not null default current_timestamp, -- time that metadata was updated
   insert_time		timestamp	not null default current_timestamp,
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
   insert_time		timestamp	not null default current_timestamp,
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
   insert_time		timestamp	not null default current_timestamp,
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
   insert_time		timestamp	not null default current_timestamp,
   --
   constraint pk_timeseries_data
     primary key (timeseries, obs_time),
   --
   constraint fk_timeseries_data_timeseries
     foreign key (timeseries) references timeseries (id)
  );
