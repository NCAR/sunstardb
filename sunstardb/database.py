from functools import wraps
import os
import os.path

import psycopg2, psycopg2.extras
import astroquery.simbad
import astropy.units
import astropy.coordinates
from sqlhappy import *

from . import utils
from . import schema

# Consider all dicts as Json type
psycopg2.extensions.register_adapter(dict, psycopg2.extras.Json)

# useful globals
TABLE_TEMPLATES = {}

def _set_templates():
    """Find the database schema and extract table templates within"""
    schemafile = schema.file('create.sql')
    file = open(schemafile)
    template = None
    for line in file:
        line = line.rstrip()
        if len(line) > 3 and line[0] == '<' and line[-1] == '>' and line[1] != '/': # tag like <NAME>
            template = line[1:-1] # gets NAME
            TABLE_TEMPLATES[template] = "" # initialize string
            continue
        elif template is not None:
            if len(line) > 3 and line[0:2] == '</' and line[-1] == '>': # end tag </NAME>
                template = None
                continue
            else:
                # Add line to saved templates
                TABLE_TEMPLATES[template] += line + "\n"
    file.close()
    if template is not None:
        raise Exception("Template parsing finished without closing tag '%s'" % template)

def split_simbad_id(simbad_id):
    """Split a SIMBAD id (e.g. 'HD 1234') into (idtype, id) pair"""
    idtype, id = simbad_id.split(" ", 1)
    idtype = idtype.strip()
    id = id.strip()
    return idtype, id

def strip_simbad_id(simbad_id):
    """Remove superflous prefix from certain SIMBAD ids (NAME, *, **)"""
    idtype, id = split_simbad_id(simbad_id)
    if idtype in ['NAME', '*', '**']:
        return id
    else:
        return simbad_id

def lookup_simbad_ids(object_name):
    """Lookup the given object in Simbad and return a dict of arrays"""

    table = astroquery.simbad.Simbad.query_objectids(object_name)
    if table is None:
        return None
    names = {}
    for o in table:
        simbad_id = utils.compress_space(o[0])
        idtype, id = split_simbad_id(simbad_id)
        if idtype not in names:
            names[idtype] = []
        names[idtype].append(simbad_id)
    return names

def lookup_simbad_info(object_name):
    """Information from Simbad.query_object as a dictionary
    
    Input:
     - object_name <str> : name to lookup in SIMBAD database
     
     Output:
      - info <dict> : information dictionary
      
    The info dict contains the following keys:
     - (everything included by default in astroquery.simbad.Simbad.query_object())
     - coord : the string ICRS coordinates of the object
     - skycoord : astropy.coordinates.SkyCoord object
     - ra : right ascention in decimal degrees
     - dec : declination in decimal degrees
    """
    simbad_info = astroquery.simbad.Simbad.query_object(object_name)
    if simbad_info is None:
        return None
    info_dict = {}
    for k in simbad_info.keys():
        # Result is in first (only) row
        # lowercase keys in result
        info_dict[k.lower()] = simbad_info[0][k]
    
    # Get rid of multiple spaces in Simbad identifier
    info_dict['main_id'] = utils.compress_space(info_dict['main_id'])

    # Change astroquery numpy string representation of coordinates to something more useful
    coord = info_dict['ra'] + ' ' + info_dict['dec']
    skycoord = astropy.coordinates.SkyCoord(coord, 'icrs',
                                            unit=(astropy.units.hourangle,
                                                  astropy.units.degree))
    info_dict['coord'] = coord
    info_dict['skycoord'] = skycoord
    info_dict['ra'] = skycoord.ra.value
    info_dict['dec'] = skycoord.dec.value

    return info_dict

class DatabaseKeyError(Exception):
    """Error raised when a DB function is called without supplying data for the required columns"""
    def __init__(self, misslist, givenlist):
        message = "Missing keys: " + repr(misslist) + "; Given keys: " + repr(givenlist)
        Exception.__init__(self, message)

class MissingDataError(Exception):
    """Error raised when data expected to be present in database is not found"""
    pass

def db_bind_keys(*reqkeys):
    """
    Decorator for functions which take database column key-value pairs as arguments.

    First argument, if a dict, is taken as the collection of key-value
    pairs.  Key-value pairs are checked for required values.
    Non-required keys are ignored.

    Note: MUST call decorator with (), even if no keys are required
      @db_bind_keys()
      def myfunc(...):         # correct

      @db_bind_keys
      def myfunc(...):         # WRONG
    """
    def wrap(f):
        @wraps(f)
        def wrapped_f(self, *args, **kwargs):
            if len(args) == 1:
                kwargs = args[0] # Assumed to be dict-like. TODO: check?
            misslist = []
            for req in reqkeys:
                if req not in kwargs:
                    misslist.append(req)
            if len(misslist) > 0:
                raise DatabaseKeyError(misslist, kwargs.keys())
            else:
                return f(self, **kwargs)
        return wrapped_f
    return wrap

class SunStarDB(Database):
    """Class providing access to the solar-stellar database"""
    @staticmethod
    def cli_connect():
        """For scripts, connect using command line arguments"""
        parser = db_optparser()
        (options, args) = parser.parse_args()
        db_params = db_kwargs(options)
        print "Connecting to database...",
        db = SunStarDB(**db_params)
        print "Done."
        return options, args, db

    @db_bind_keys('name')
    def fetch_property_type(self, **kwargs):
        """Fetch a property type given its (name)"""
        sql = "SELECT * FROM property_type WHERE name=%(name)s"
        db_property_type = self.fetch_row(sql, kwargs)
        return db_property_type

    def fetchall_property_types(self, **kwargs):
        """Fetch all existing property types from the database"""
        sql = "SELECT name FROM property_type"
        return self.fetch_column(sql)

    @db_bind_keys('type_id')
    def fetch_property_type_by_id(self, **kwargs):
        """Fetch a property type given its ID (type_id)"""
        sql = "SELECT * FROM property_type WHERE id=%(type_id)s"
        db_property_type = self.fetch_row(sql, kwargs)
        return db_property_type

    @db_bind_keys('name', 'type', 'units', 'description')
    def insert_property_type(self, **kwargs):
        """Insert a property type given (name, type, units, description)"""
        # Prepare the DDL schema templates if it has not already been done
        if not TABLE_TEMPLATES:
            _set_templates()
        sql = """INSERT INTO property_type (name, type, units, description)
                      VALUES (%(name)s, %(type)s, %(units)s, %(description)s)"""
        self.execute(sql, kwargs)
        ptype = self.fetch_property_type(kwargs)
        template = TABLE_TEMPLATES[ptype['type']]
        create_ddl = template % ptype # Set %(name) and %(id) in table creation DDL
        self.execute(create_ddl)
        return ptype
        
    @db_bind_keys('name')
    def drop_property_type(self, **kwargs):
        """Remove a property type given its (name)"""
        self.execute("""DELETE FROM dataset_map WHERE type IN
                        (SELECT id FROM property_type WHERE name = %(name)s)""", kwargs)
        self.execute("""DELETE FROM property WHERE type IN
                        (SELECT id FROM property_type WHERE name = %(name)s)""", kwargs)
        self.execute("DELETE FROM property_type WHERE name = %(name)s", kwargs)
        self.execute("DROP TABLE dat_%(name)s" % kwargs) # TODO: check arg

    @db_bind_keys('name')
    def fetch_instrument(self, **kwargs):
        """Fetch an instrument give its (name)"""
        sql = "SELECT * FROM instrument WHERE name=%(name)s"
        db_instrument = self.fetch_row(sql, kwargs)
        return db_instrument

    @db_bind_keys('name', 'long', 'url', 'description')
    def insert_instrument(self, **kwargs):
        """Insert an instrument given (name, long, url, description)"""
        sql = """INSERT INTO instrument (name, long, url, description)
                      VALUES (%(name)s, %(long)s, %(url)s, %(description)s)"""
        self.execute(sql, kwargs)
        return self.fetch_instrument(kwargs)

    @db_bind_keys('name')
    def fetch_star(self, **kwargs):
        """Fetch a star given (name), which may be any alias"""
        kwargs['name'] = utils.compress_space(kwargs['name'])
        sql = """SELECT s.*
                   FROM star s
                   JOIN star_alias sa ON sa.star = s.id
                  WHERE sa.name = %(name)s"""
        db_star = self.fetch_row(sql, kwargs)
        return db_star

    @db_bind_keys('name')
    def fetch_star_by_main_id(self, **kwargs):
        """Fetch a star given its (name), but only checking the main identifier"""
        kwargs['name'] = utils.compress_space(kwargs['name'])
        sql = "SELECT * FROM star WHERE name=%(name)s"
        db_star = self.fetch_row(sql, kwargs)
        return db_star

    @db_bind_keys('name')
    def insert_star(self, **kwargs):
        """Insert a star given (name), pulling additional info from SIMBAD."""
        star = kwargs['name']
        simbad_ids = lookup_simbad_ids(star)
        simbad_info = lookup_simbad_info(star)
        if simbad_ids is None:
            if star == 'Sun':
                # The Sun is not in SIMBAD, make exception to allow it to be inserted into database
                # TODO, make a special data file to insert this and bootstrap the database?
                simbad_info = { 'main_id' : 'Sun', 'coord' : '00 00 0.0 +00 00 0.0', 'ra' : 0.0, 'dec' : 0.0 }
                simbad_ids  = { 'NAME' : [ 'Sun' ] }
            else:
                # Require that object exists in SIMBAD
                raise Exception("Object '%s' not found in SIMBAD. Input was '%s'" % (simbad_name, repr(kwargs)))

        # Insert the SIMBAD main ID as the star's main name
        sql = """INSERT INTO star (name, coord, ra, dec) 
                      VALUES (%(main_id)s, %(coord)s, %(ra)s, %(dec)s)"""
        self.execute(sql, simbad_info)
        db_star = self.fetch_star_by_main_id(name=simbad_info['main_id'])
        
        # Insert the rest of the names found in SIMBAD
        sql = """INSERT INTO star_alias (star, type, name)
                      VALUES (%(star_id)s, %(type)s, %(name)s)"""
        for idtype, namelist in simbad_ids.items():
            for name in namelist:
                self.execute(sql, {'star_id':db_star['id'],
                                   'type':idtype,
                                   'name':name})
        return db_star

    @db_bind_keys('name')
    def fetch_reference(self, **kwargs):
        """Fetch a reference given (name)"""
        sql = """SELECT * FROM reference WHERE name=%(name)s"""
        return self.fetch_row(sql, kwargs)

    @db_bind_keys('name', 'bibline', 'bibcode')
    def insert_reference(self, **kwargs):
        """Insert a reference given (name, bibline, bibcode)"""
        sql = """INSERT INTO reference (name, bibline, bibcode)
                      VALUES (%(name)s, %(bibline)s, %(bibcode)s)"""
        self.execute(sql, kwargs)
        return self.fetch_reference(kwargs)

    @db_bind_keys('name')
    def fetch_origin(self, **kwargs):
        """Fetch an origin given (name)"""
        sql = """SELECT * FROM origin WHERE name=%(name)s"""
        return self.fetch_row(sql, kwargs)

    @db_bind_keys('name', 'kind', 'url', 'description')
    def insert_origin(self, **kwargs):
        """Insert an origin given (name, kind, url, description)"""
        sql = """INSERT INTO origin (name, kind, url, description)
                      VALUES (%(name)s, %(kind)s, %(url)s, %(description)s)"""
        self.execute(sql, kwargs)
        return self.fetch_origin(kwargs)

    @db_bind_keys('name')
    def fetch_source(self, **kwargs):
        """Fetch a source given (name)"""
        sql = """SELECT * FROM source WHERE name=%(name)s"""
        if 'version' in kwargs:
            if kwargs['version'] is not None:
                sql += " AND version=%(version)s"
            else:
                sql += " AND version IS NULL"
        return self.fetch_row(sql, kwargs)

    @db_bind_keys('name', 'kind', 'origin_id', 'source_time')
    def insert_source(self, **kwargs):
        """Insert a source given (name, kind, origin_id, source_time)"""
        if 'version' not in kwargs:
            kwargs['version'] = None
        if 'source_id' not in kwargs:
            kwargs['source_id'] = None
    
        sql = """INSERT INTO source (name, kind, version, origin, source, source_time)
                      VALUES (%(name)s, %(kind)s, %(version)s, %(origin_id)s, %(source_id)s, %(source_time)s)"""
        self.execute(sql, kwargs)
        return self.fetch_source(kwargs)
    
    @db_bind_keys('star_id', 'type_id')
    def fetch_property_by_id(self, **kwargs):
        """Fetch a property given (star_id, type_id)"""
        sql = """SELECT p.*
                          FROM property p
                          JOIN star s ON s.id = p.star
                          JOIN property_type t ON t.id = p.type
                          JOIN source src ON src.id = p.source
                          JOIN reference r ON r.id = p.reference
                     LEFT JOIN instrument i ON i.id = p.instrument
                         WHERE s.id = %(star_id)s
                           AND t.id = %(type_id)s
                           AND src.id = %(src_id)s"""
        # TODO: need to make compound object: star, source, reference, instrument
        db_property = self.fetch_row(sql, kwargs)
        return db_property

    def prepare_property(self, property, star, ptype, source, reference, instrument=None):
        """Prepare a property for insertion into the database
        
        Input:
         - property   : dict containing property data
         - star       : dict containing star data
         - ptype      : dict containing property type data
         - source     : dict containing source data
         - reference  : dict containing reference data
         - instrument : (optional) dict containing instrument data

        If (star, ptype, source, reference, instrument) do not have
        the 'id' key set, then it will be fetched from the database
        using the 'name' key.
        """
        # Star
        if 'id' not in star:
            db_star = self.fetch_star(star)
            if db_star is None:
                raise MissingDataError("Star '%s'" % repr(star))
            property['star_id'] = db_star['id']
        else:
            property['star_id'] = star['id']
            
        # Property Type
        if 'id' not in ptype:
            db_ptype = self.fetch_property_type(ptype)
            if db_ptype is None:
                raise MissingDataError("Property type '%(name)s'" % ptype)
            property['type_id'] = db_ptype['id']
        else:
            property['type_id'] = ptype['id']
        
        # Source
        if 'id' not in source:
            db_source = self.fetch_source(source)
            if db_source is None:
                raise MissingDataError("Source '%(name)s'" % source)
            property['src_id'] = db_source['id']
        else:
            property['src_id'] = source['id']

        # Reference
        if 'id' not in reference:
            db_reference = self.fetch_reference(reference)
            if db_reference is None:
                raise MissingDataError("Reference '%(name)s'" % reference)
            property['ref_id'] = db_reference['id']
        else:
            property['ref_id'] = reference['id']

        # Instrument
        if instrument is not None:
            if 'id' not in instrument:
                db_instrument = self.fetch_instrument(instrument)
                if db_instrument is None:
                    raise MissingDataError("Instrument '%(name)s'" % instrument)
                property['inst_id'] = db_instrument['id']
            else:
                property['inst_id'] = instrument['id']
        else:
            property['inst_id'] = None # explicitely

        # Explicit None for all NULLable columns
        for col in ('errlo', 'errhi', 'errbounds', 'obs_time', 'int_time', 'meta'):
            if col not in property:
                property[col] = None

        # Error
        if 'err' in property:
            # Case where error is expressed as a percent
            if isinstance(property['err'], basestring) and property['err'].endswith('%'):
                property['err'] = abs(property['val']) * float(property['err'].rstrip('%')) / 100.0

            # Set lo and hi bounds of the error
            property['errhi'] = property['err']
            property['errlo'] = property['err']
            del property['err']

        # Error as range
        if property['errbounds'] is None and property['errlo'] is not None and property['errhi'] is not None:
            lo = property['val'] - property['errlo']
            hi = property['val'] + property['errhi']
            property['errbounds'] = psycopg2.extras.NumericRange(lo, hi, '[]')

        return property
    
    @db_bind_keys('star_id', 'type_id', 'src_id', 'ref_id', 'inst_id', 'meta')
    def insert_property(self, **kwargs):
        """Insert a property given database ids (star_id, type_id, src_id, ref_id, inst_id) and (meta)
        
        Use prepare_property to fetch the necessary IDs from the database
        """
        sql = """INSERT INTO property (star, type, source, reference, instrument)
                      VALUES (%(star_id)s, %(type_id)s, %(src_id)s, %(ref_id)s, %(inst_id)s)
               """
        self.execute(sql, kwargs)
        db_prop = self.fetch_property_by_id(kwargs) # TODO: this fetch should contain property_type.{type, name}
        db_type = self.fetch_property_type_by_id(kwargs) #  to eliminate this fetch...
        kwargs['prop_id'] = db_prop['id']
        kwargs['name'] = db_type['name']
        if db_type['type'] == 'MEASURE':
            self.insert_measure(kwargs)
        elif db_type['type'] == 'LABEL':
            self.insert_label(kwargs)
        else:
            raise Exception("Unexpected property_type.type: %(type)s" % db_type)
        return db_prop

    @db_bind_keys('name', 'prop_id', 'star_id', 'src_id',
                  'val', 'errlo', 'errhi', 'errbounds', 'obs_time', 'int_time', 'meta')
    def insert_measure(self, **kwargs):
        """Insert a measurement-type property into its data table
        
        Requires ('name', 'prop_id', 'star_id', 'src_id', 'val',
                  'errlo', 'errhi', 'errbounds', 'obs_time',
                  'int_time', 'meta')
        """
        sql = """INSERT INTO dat_%(name)s (property, star, source,
                                           %(name)s, errlo, errhi, errbounds, obs_time, int_time, meta)
                      VALUES (%%(prop_id)s, %%(star_id)s, %%(src_id)s,
                              %%(val)s, %%(errlo)s, %%(errhi)s, %%(errbounds)s,
                              %%(obs_time)s, %%(int_time)s, %%(meta)s)""" % kwargs # set 'name' first
        self.execute(sql, kwargs) # DB driver to bind the rest
        return None # TODO: return measure?

    @db_bind_keys('name', 'prop_id', 'star_id', 'src_id',
                  'label', 'meta')
    def insert_label(self, **kwargs):
        """Insert a label-type property into its data table

        Requires ('name', 'prop_id', 'star_id', 'src_id',
                  'label', 'meta')
        """
        sql = """INSERT INTO dat_%(name)s (property, star, source, %(name)s, meta)
                      VALUES (%%(prop_id)s, %%(star_id)s, %%(src_id)s,
                              %%(label)s, %%(meta)s)""" % kwargs # set 'name' first
        self.execute(sql, kwargs) # DB driver to bind the rest
        return None # TODO: return measure?

    @db_bind_keys('name')
    def create_dataset_from_source(self, **kwargs):
        """Create a dataset given a source (name)"""
        db_source = self.fetch_source(kwargs)
        sql = """INSERT INTO dataset (name, description) VALUES (%(name)s, %(description)s)"""
        dataset = { 'name'        : db_source['name'],
                    'description' : "Dataset automatically generated from data source '%s'" % db_source['name'] }
        self.execute(sql, dataset)
        sql = """INSERT INTO dataset_map (dataset, star, type, property)
                 SELECT ds.id, p.star, p.type, p.id
                   FROM dataset ds
                   JOIN source s on s.name = ds.name
                   JOIN property p on p.source = s.id
                  WHERE ds.name = %(name)s"""
        self.execute(sql, dataset)

    def sanity_check(self, tasks, source, verbose=True):
        """Execute sanity checks for the given source"""
        def maybe_print(msg):
            if verbose:
                print msg
        if 'exists_all_stars' in tasks:
            for ptype in tasks['exists_all_stars']:
                maybe_print("Checking: '%s' data exists for all stars..." % ptype)
                self.check_exists_all_stars(ptype, source['id'])
                maybe_print("CONFIRMED '%s' data exists for all stars." % ptype)

    def check_exists_all_stars(self, ptype, src_id):
        """Sanity check that data of the given ptype exists for all stars of the given src_id"""
        sql = """SELECT DISTINCT s.*
                   FROM property p
                   JOIN star s on s.id = p.star
                  WHERE p.source = %(src_id)s
                    AND p.star NOT IN (SELECT p2.star
                                         FROM property p2
                                         JOIN property_type pt on pt.id = p2.type
                                        WHERE p2.source = %(src_id)s
                                          AND pt.name = %(ptype)s)
              """
        results, colnames = self.fetchall(sql, { 'ptype' : ptype, 'src_id' : src_id }, colnames=True)
        if results is not None:
            n_bad = len(results)
            message = "CHECK FAILED: The following %i stars do not have '%s' data:\n" % (n_bad, ptype)
            message += "\t".join(colnames) + "\n"
            for row in results:
                message += "\t".join(str(i) for i in row) + "\n"
            raise Exception(message)
        else:
            return True

    def fetch_data(self, ptype):
        """Fetch data and all associated info for the given property type"""
        
        sql = """SELECT s.id star_id, s.hd, s.bright, s.proper,
                        r.name reference, o.name origin, o.kind origin_kind, i.name instrument,
                        p.id prop_id, d.%(ptype)s
                   FROM %(ptype)s d
                   JOIN property p ON p.id = d.property
                   JOIN star s ON s.id = p.star
                   JOIN reference r ON r.id = p.reference
                   JOIN source src ON src.id = p.source
                   JOIN origin o ON o.id = src.origin
                   JOIN instrument i ON i.id = p.instrument"""
        # TODO: validate to prevent SQL injection
        result = self.fetchall(sql % ptype)
        return result

    def fetch_data_table(self, dataset, ptypes, nulls=True):
        """Fetch star names and data as a table for the given dataset

        Inputs:
         - dataset <str>  : dataset name
         - ptypes <array> : an array of property types to fetch
         - nulls <bool>   : whether to allow null values in the columns

         Output:
          - result <array> : an array of psycopg2.extras.DictRow

        If 'nulls' is True, all existing data will be returned.  If
        'nulls' is False, only rows for which data exists for each
        given ptype will be returned.

        The DictRow results may be accessed like a dict.  Each
        row will have a 'star' key, pointing to the star
        name, and a key for each property type given.
        """
        ixs = range(len(ptypes))
        sql = "WITH "
        for i in ixs:
            sql += """d%02i AS (
                        SELECT d.* FROM dat_%s d
                          JOIN dataset_map dm ON dm.property = d.property
                          JOIN dataset ds ON ds.id = dm.dataset
                         WHERE ds.name = %%(dataset)s ),
            """ % (i, ptypes[i])
        sql += "uq_stars AS ( "
        sql += " UNION ".join( "SELECT star FROM d%02i" % i for i in ixs)
        sql += ") SELECT s.name star, "
        sql += ", ".join( "d%02i.%s" % (i, ptypes[i]) for i in ixs )
        sql += " FROM uq_stars us JOIN star s ON s.id = us.star"
        if nulls is True:
            jointype = "LEFT JOIN"
        else:
            jointype = "JOIN"
            
        for i in ixs:
            sql += " %s d%02i ON d%02i.star = s.id" % (jointype, i, i)

        result = self.fetchall(sql, { 'dataset' : dataset})
        return result

    def fetch_data_cols(self, dataset, ptypes, nulls=True):
        """Fetch a data table as a dictionary of columns

        Inputs:
         - dataset <str>  : dataset name
         - ptypes <array> : an array of property types to fetch
         - nulls <bool>   : whether to allow null values in the columns

         Output:
          - cols <dict>   : dictionary containing columns

        If 'nulls' is True, all existing data will be returned.  If
        'nulls' is False, only rows for which data exists for each
        given ptype will be returned.

        The returned data format is like: 
          { 'ptype1' : [ 1, 2, 3, ... ], 'ptype2' : [ 1, 2, 3 ], ... }
        """

        result = self.fetch_data_table(dataset, ptypes, nulls=nulls)
        if result is None:
            return None
        cols = {}
        col_list = ['star'] + ptypes
        for c in col_list:
            cols[c] = []
        for row in result:
            for c in col_list:
                cols[c].append(row[c.lower()])
        return cols
