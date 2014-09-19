create table star
  (id			serial		not null,
   name			varchar(32)	not null, -- canonical name, one of the below, perhaps with prefix
   coord		varchar(32)	not null, -- ICRS coordinates, e.g. '06 45 08.91728 -16 42 58.0171'
   ra			float		not null, -- Right Ascention in decimal degrees
   dec			float		not null, -- Declination in decimal degrees
   insert_time		timestamp	not null default current_timestamp,
   --
   constraint pk_star
     primary key (id),
   --
   constraint uq_star_name
     unique (name)
  );

create table star_alias
  (star			integer		not null,
   type			varchar(32)	not null, -- SIMBAD acronym
   name			varchar(64)	not null, -- SIMBAD name
   --
   constraint uq_star_alias_name
     unique (name),
   --
   constraint fk_star_alias_star
     foreign key (star) references star (id)
  );

create index ix_star_alias_star on star_alias (star);
create index ix_star_alias_type on star_alias (type);

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
   name			varchar(64)	not null, -- source data package
   kind			varchar(32)	not null check (kind in ('FILE', 'CODE')),
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
     unique (name, version),
   --
   constraint fk_source_origin
     foreign key (origin) references origin (id)
  );

create index ix_source_origin on source (origin);

-- Observational instrument a property is derived from
create table instrument
  (id			serial		not null,
   name			varchar(32)	not null, -- short name, e.g. MWO-HK
   long			varchar(64)	not null, -- long name, e.g. Mount Wilson Observatory HK Project
   url			varchar(1024)		, -- canonical URL to instrument information
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
   name			varchar(32)	not null, -- name of property, e.g. Pcyc
   type			varchar(32)	not null check (type in ('MEASURE', 'LABEL', 'TIMESERIES')),
   units		varchar(32)		, -- physical units.  NULL when property is non-numeric or uniteless
   description		text		not null, -- paragraph describing the property
   --
   constraint pk_property_type
     primary key (id),
   --
   constraint uq_property_type_name
     unique (name)
  );


-- Property: a single measurement on or label of a star
-- This table holds all the identity, provenence references, and meta-data
-- Actual data is contained in leaf tables
create table property
  (id			serial		not null,
   star			integer		not null,
   type			integer		not null,
   source		integer		not null,
   reference		integer		not null,
   instrument		integer			,
   insert_time		timestamp	not null default current_timestamp,
   --
   constraint pk_property
     primary key (id),
   --
   constraint uq_property
     unique (star, type, source),
   -- for dataset_map foreign key
   constraint uq_property_dataset
     unique (id, star, type),
   --
   constraint fk_property_star
     foreign key (star) references star (id),
   --
   constraint fk_property_type
     foreign key (type) references property_type (id),
   --
   constraint fk_property_source
     foreign key (source) references source (id)
     on delete cascade,
   --
   constraint fk_property_reference
     foreign key (reference) references reference (id),
   --
   constraint fk_property_instrument
     foreign key (instrument) references instrument (id)
  );

create index ix_property_star on property (star);
create index ix_property_type on property (type);
create index ix_property_source on property (source);
create index ix_property_reference on property (reference);
create index ix_property_instrument on property (instrument);

create table dataset
  (id			serial		not null,
   name			varchar(64)	not null,
   description		text		not null,
   insert_time		timestamp	not null default current_timestamp,
   --
   constraint pk_dataset
     primary key (id),
   --
   constraint uq_dataset_name
     unique (name)
  );

create table dataset_map
  (dataset		integer		not null,
   star			integer		not null,
   type			integer		not null,
   property		integer		not null,
   --
   constraint pk_dataset_map
     primary key (dataset, star, type),
   --
   constraint fk_dataset_map_dataset
     foreign key (dataset) references dataset (id),
   --
   constraint fk_dataset_map_star
     foreign key (star) references star (id),
   --
   constraint fk_dataset_map_type
     foreign key (type) references property_type (id),
   --
   constraint fk_dataset_map_property
     foreign key (property, star, type) references property (id, star, type)
  );

create index ix_dataset_map_star on dataset_map (star);
create index ix_dataset_map_type on dataset_map (type);
create index ix_dataset_map_property on dataset_map (property);

create table timeseries
  (id			serial		not null,
   star			integer		not null,
   type			integer		not null,
   source		integer		not null,
   reference		integer		not null,
   instrument		integer			,
   insert_time		timestamp	not null default current_timestamp,
   append_time		timestamp	not null,
   meta			json		not null, -- meta information for the whole timeseries
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
     foreign key (reference) references reference (id),
   --
   constraint fk_timeseries_instrument
     foreign key (instrument) references instrument (id)
  );

create index ix_timeseries_star on timeseries (star);
create index ix_timeseries_type on timeseries (type);
create index ix_timeseries_source on timeseries (source);
create index ix_timeseries_reference on timeseries (reference);
create index ix_timeseries_instrument on timeseries (instrument);

/*** Table Templates ***

<MEASURE>
create table dat_%(name)s
  (property		integer			not null,
   star			integer			not null,
   type			integer			not null default %(id)s check (type = %(id)s),
   source		integer			not null,
   %(name)s		double precision	not null,
   errlo		double precision	,
   errhi		double precision	,
   errbounds		numrange		,
   obs_time		timestamp		,
   int_time		tsrange			,
   meta			json			,
   meta_time		timestamp		not null default current_timestamp,
   --
   constraint pk_dat_%(name)s
     primary key (property),
   --
   constraint uq_dat_%(name)s_property_integ
     unique (star, type, source),
   --
   constraint fk_dat_%(name)s_property
     foreign key (property) references property (id)
     on delete cascade,
   --
   constraint fk_dat_%(name)s_property_integ
     foreign key (star, type, source) references property (star, type, source),
   --
   constraint fk_dat_%(name)s_star
     foreign key (star) references star (id),
   --
   constraint fk_dat_%(name)s_type
     foreign key (type) references property_type (id),
   --
   constraint fk_dat_%(name)s_source
     foreign key (source) references source (id)
  );
</MEASURE>

<LABEL>
create table dat_%(name)s
  (property		integer			not null,
   star			integer			not null,
   type			integer			not null default %(id)s check (type = %(id)s),
   source		integer			not null,
   %(name)s		varchar(100)		not null,
   meta			json			,
   meta_time		timestamp		not null default current_timestamp,
   --
   constraint pk_dat_%(name)s
     primary key (property),
   --
   constraint uq_dat_%(name)s_property_integ
     unique (star, type, source),
   --
   constraint fk_dat_%(name)s_property
     foreign key (property) references property (id)
     on delete cascade,
   --
   constraint fk_dat_%(name)s_property_integ
     foreign key (star, type, source) references property (star, type, source),
   --
   constraint fk_dat_%(name)s_star
     foreign key (star) references star (id),
   --
   constraint fk_dat_%(name)s_type
     foreign key (type) references property_type (id),
   --
   constraint fk_dat_%(name)s_source
     foreign key (source) references source (id)
  );
</LABEL>

<TIMESERIES>
create table ser_%(name)s
  (timeseries		integer			not null,
   star			integer			not null,
   type			integer			not null default %(id)s check (type = %(id)s),
   source		integer			not null,
   obs_time		timestamp		not null,
   %(name)s		double precision	not null,
   errlo		double precision	,
   errhi		double precision	,
   errbounds		numrange		,
   insert_time		timestamp		not null default current_timestamp,
   meta			json			,
   meta_time		timestamp		not null default current_timestamp,
   --
   constraint pk_ser_%(name)s
     primary key (timeseries, obs_time),
   --
   constraint uq_ser_%(name)s_property_integ
     unique (star, type, source),
   --
   constraint fk_ser_%(name)s_property
     foreign key (property) references timeseries (id)
     on delete cascade,
   --
   constraint fk_ser_%(name)s_property_integ
     foreign key (star, type, source) references timeseries (star, type, source),
   --
   constraint fk_ser_%(name)s_star
     foreign key (star) references star (id),
   --
   constraint fk_ser_%(name)s_type
     foreign key (type) references property_type (id),
   --
   constraint fk_ser_%(name)s_source
     foreign key (source) references source (id)
  );
</TIMESERIES>

*/
