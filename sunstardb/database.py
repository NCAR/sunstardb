from functools import wraps
import os
import os.path
import re

import psycopg2, psycopg2.extras
import astroquery.simbad
import astropy.units
import astropy.coordinates
import astropy.time
from sqlhappy import *

from . import utils
from . import schema

# Consider all dicts as Json type
psycopg2.extensions.register_adapter(dict, psycopg2.extras.Json)

# Default configuration file location is the same as the directory containing this package
if not os.environ.get('DBCONFIG'):
    config_file = os.path.join(os.path.dirname(os.path.abspath(sys.modules[__name__].__file__)), '../sunstardb.cfg')
    if os.path.isfile(config_file):
        os.environ['DBCONFIG'] = config_file


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
    """Split a SIMBAD id (e.g. 'HD 1234', 'BD-01 68') into (idtype, id) pair"""
    idtype, id = re.split(r'[ +-]', simbad_id, 1)
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
    """Lookup the given object in Simbad and return a dict of arrays
    
    Identifiers returned from SIMBAD have their spaces compressed.
    For example, 'HD   1845' will be returned as 'HD 1845'.  This is in
    order to ensure uniform name-matching.
    """

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

    The 'main_id' identifier returned from SIMBAD have its spaces
    compressed.  For example, 'HD 1845' will be returned as 'HD 1845'.
    This is in order to ensure uniform name-matching.
    """
    simbad_info = astroquery.simbad.Simbad.query_object(object_name)
    if simbad_info is None:
        return None
    info_dict = {}
    for k in list(simbad_info.keys()):
        # Result is in first (only) row
        # lowercase keys in result
        info_dict[k.lower()] = simbad_info[0][k]
    
    # Get rid of multiple spaces in Simbad identifier
    info_dict['main_id'] = utils.compress_space(info_dict['main_id'])

    # Change astroquery numpy string representation of coordinates to something more useful
    coord = format_simbad_coord(info_dict['ra'], info_dict['dec'])
    skycoord = astropy.coordinates.SkyCoord(coord, 'icrs',
                                            unit=(astropy.units.hourangle,
                                                  astropy.units.degree))
    info_dict['coord'] = coord
    info_dict['skycoord'] = skycoord
    info_dict['ra'] = skycoord.ra.value
    info_dict['dec'] = skycoord.dec.value

    return info_dict

def format_simbad_coord(ra, dec):
    """Usually SIMBAD formats ra 'hh mm ss.ss' dec '+dd mm ss.ss'.  This fixes exceptions to that rule..."""
    result = dict(ra=ra, dec=dec)
    for k, v in list(result.items()):
        a = v.split(' ')
        if len(a) == 3:
            # this is expected
            continue
        elif len(a) == 2: # format was 'dd mm.mm' instead of 'dd mm ss.ss'
            deg = float(a[0])
            min = float(a[1])
            rem = min % 1
            min = math.floor(min)
            sec = rem * 60 # * 60 sec / 1 min
            fix = "%+02i %02i %02.6f" % (deg, min, sec) # TODO: carry over precision?
            if k == 'ra':
                fix = fix[1:] # chop off leading sign for ra
            result[k] = fix
        else:
            raise Exception("unexpected format from SIMBAD: ra='%(ra)s' dec='%(dec)s'" % result)
    return result['ra'] + ' ' + result['dec']

# Some objects; the utility here is for documentation purposes.
class RowObject:
    def __init__(self, rowdict, exceptions=[]):
        for k, v in list(rowdict.items()):
            if k not in exceptions:
                setattr(self, k, v)
    
class Source(RowObject):
    def name(self):
        """Name of the source"""
        return self.name

    def type(self):
        """Type of source; manner in which it arrived into the database
        
        `type` may be one of the following:
         - 'FILE' : Data which entered the database from a file
         - 'CODE' : Data which entered the database as output of some code
        """
        return self.type
    
class DatabaseKeyError(Exception):
    """Error raised when a DB function is called without supplying data for the required columns"""
    def __init__(self, misslist, givenlist):
        message = "Missing keys: " + repr(misslist) + "; Given keys: " + repr(givenlist)
        Exception.__init__(self, message)

class MissingDataError(Exception):
    """Error raised when data expected to be present in database is not found"""
    pass

def db_bind_keys(*bind_reqkeys, **bind_kwargs):
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
            # Substitute kwargs for dict in first argument
            if len(args) == 1:
                kwargs = args[0] # Assumed to be dict-like. TODO: check?

            # Check for required keys
            misslist = []
            for req in bind_reqkeys:
                if req not in kwargs:
                    misslist.append(req)
            if len(misslist) > 0:
                raise DatabaseKeyError(misslist, list(kwargs.keys()))

            # Set optional keys to 'None'
            if bind_kwargs.get('optional'):
                for opt in bind_kwargs['optional']:
                    kwargs.setdefault(opt, None)
            return f(self, **kwargs)
        return wrapped_f
    return wrap

class SunStarDB(Database):
    """Class providing access to the solar-stellar database"""
    @staticmethod
    def cli_connect(arguments=None):
        """For scripts, connect using command line arguments"""
        parser = db_argparser(arguments=arguments)
        args = parser.parse_args()
        db_params = db_kwargs(args)
        print("Connecting to database...", end=' ')
        db = SunStarDB(**db_params)
        print("Done.")
        return args, db

    @db_bind_keys('name')
    def fetch_datatype(self, **kwargs):
        """Fetch a datatype given its (name)"""
        sql = "SELECT * FROM datatype WHERE name=%(name)s"
        db_datatype = self.fetch_row(sql, kwargs)
        return db_datatype

    def fetchall_datatypes(self, **kwargs):
        """Fetch all existing datatypes from the database"""
        sql = "SELECT * FROM datatype"
        return self.fetchall_astropy(sql)

    @db_bind_keys('type_id')
    def fetch_datatype_by_id(self, **kwargs):
        """Fetch a datatype given its ID (type_id)"""
        sql = "SELECT * FROM datatype WHERE id=%(type_id)s"
        db_datatype = self.fetch_row(sql, kwargs)
        return db_datatype

    @db_bind_keys('name', 'struct', 'units', 'description')
    def insert_datatype(self, **kwargs):
        """Insert a datatype given (name, type, units, description)"""
        # Prepare the DDL schema templates if it has not already been done
        if not TABLE_TEMPLATES:
            _set_templates()
        sql = """INSERT INTO datatype (name, struct, units, description)
                      VALUES (%(name)s, %(struct)s, %(units)s, %(description)s)"""
        self.execute(sql, kwargs)
        datatype = self.fetch_datatype(kwargs)
        template = TABLE_TEMPLATES[datatype['struct']]
        create_ddl = template % datatype # Set %(name) and %(id) in table creation DDL
        self.execute(create_ddl)
        return datatype
        
    @db_bind_keys('name')
    def drop_datatype(self, **kwargs):
        """Remove a datatype given its (name)"""
        self.execute("""DELETE FROM dataset_map WHERE type IN
                        (SELECT id FROM datatype WHERE name = %(name)s)""", kwargs)
        self.execute("""DELETE FROM property WHERE type IN
                        (SELECT id FROM datatype WHERE name = %(name)s)""", kwargs)
        self.execute("""DELETE FROM timeseries WHERE type IN
                        (SELECT id FROM datatype WHERE name = %(name)s)""", kwargs)
        self.execute("DELETE FROM datatype WHERE name = %(name)s", kwargs)
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

    def fetchall_instruments(self):
        """Fetch all existing instruments from the database"""
        sql = """SELECT * FROM instrument"""
        return self.fetchall_astropy(sql)

    @db_bind_keys('name')
    def fetch_star(self, **kwargs):
        """Fetch a star given (name), which may be any alias
        """
        sql = """SELECT s.*
                   FROM star s
                   JOIN star_alias sa ON sa.star = s.id
                  WHERE replace(sa.name, ' ', '') = replace(%(name)s, ' ', '')"""
        db_star = self.fetch_row(sql, kwargs)
        return db_star

    @db_bind_keys('name', 'type')
    def fetch_star_alias(self, **kwargs):
        """Fetch a star alias of (type), using (name) which may be any alias
        """
        sql = """SELECT sa.name
                   FROM star_alias sa
                  WHERE sa.type = %(type)s
                    AND sa.star = (
                        SELECT s.id
                          FROM star s
                          JOIN star_alias sa ON sa.star = s.id
                         WHERE replace(sa.name, ' ', '') = replace(%(name)s, ' ', '')
                        )"""
        db_star = self.fetch_scalar(sql, kwargs)
        return db_star

    @db_bind_keys('name')
    def fetchall_star_aliases(self, **kwargs):
        """Fetch all star aliases, using (name) which may be any alias
        """
        sql = """SELECT sa.type, sa.name
                   FROM star_alias sa
                  WHERE sa.star = (
                        SELECT s.id
                          FROM star s
                          JOIN star_alias sa ON sa.star = s.id
                         WHERE replace(sa.name, ' ', '') = replace(%(name)s, ' ', '')
                        )"""
        star_aliases = self.fetchall(sql, kwargs)
        return star_aliases

    @db_bind_keys('name')
    def fetch_star_by_main_id(self, **kwargs):
        """Fetch a star given its (name), but only checking the main identifier

        The given identifier will have its spaces compressed..  For
        example, 'HD 1845' will be returned as 'HD 1845'.  This is in
        order to ensure uniform name-matching.
        """
        kwargs['name'] = utils.compress_space(kwargs['name'])
        sql = "SELECT * FROM star WHERE name=%(name)s"
        db_star = self.fetch_row(sql, kwargs)
        return db_star

    def fetch_starlist(self, listtype=None, **binds):
        if listtype is None:
            sql = "SELECT * FROM star"
            binds = {}
        elif listtype == 'dataset':
            sql = """SELECT DISTINCT s.*
                       FROM star s
                       JOIN dataset_map dsm ON dsm.star = s.id
                       JOIN dataset ds ON ds.id = dsm.id
                       JOIN datatype dt ON dt.id = dsm.type
                      WHERE ds.name = %(dataset)s"""
            sql += self.build_filter(binds, {'datatype':'dt.name'}, where=False)
        elif listtype == 'timeseries':
            sql = """SELECT DISTINCT s.*
                                FROM star s
                                JOIN timeseries ts ON ts.star = s.id
                                JOIN datatype dt ON dt.id = ts.type
                                JOIN source src on src.id = ts.source"""
            sql += self.build_filter(binds, {'datatype':'dt.name','source':'src.name'})
        elif listtype == 'property':
            sql = """SELECT DISTINCT s.*
                                FROM star s
                                JOIN property p on p.star = s.id
                                JOIN datatype dt ON dt.id = p.type
                                JOIN source src on src.id = p.source"""
            sql += self.build_filter(binds, {'datatype':'dt.name','source':'src.name'})
        result = self.fetchall(sql, binds)
        return result

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
                raise Exception("Object '%s' not found in SIMBAD. Input was '%s'" % (star, repr(kwargs)))

        # Insert the SIMBAD main ID as the star's main name
        sql = """INSERT INTO star (name, coord, ra, dec) 
                      VALUES (%(main_id)s, %(coord)s, %(ra)s, %(dec)s)"""
        self.execute(sql, simbad_info)
        db_star = self.fetch_star_by_main_id(name=simbad_info['main_id'])
        
        # Insert the rest of the names found in SIMBAD
        sql = """INSERT INTO star_alias (star, type, name)
                      VALUES (%(star_id)s, %(type)s, %(name)s)"""
        for idtype, namelist in list(simbad_ids.items()):
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

    @db_bind_keys('name', 'kind', 'description', optional=['url'])
    def insert_origin(self, **kwargs):
        """Insert an origin given (name, kind, url, description)"""
        sql = """INSERT INTO origin (name, kind, url, description)
                      VALUES (%(name)s, %(kind)s, %(url)s, %(description)s)"""
        self.execute(sql, kwargs)
        return self.fetch_origin(kwargs)

    def fetchall_origins(self):
        """Fetch all existing origins in the database"""
        sql = """SELECT * FROM origin"""
        return self.fetchall_astropy(sql)

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
    
    @db_bind_keys('name')
    def delete_source(self, **kwargs):
        self.delete_dataset(kwargs)
        self.execute("DELETE FROM source WHERE name = %(name)s", kwargs)
        # Other deletions handled by schema 'on delete cascade'

    def fetchall_sources(self):
        """Fetch all existing sources in the database"""
        sql = """SELECT * FROM source"""
        return self.fetchall_astropy(sql)

    @db_bind_keys('name')
    def delete_dataset(self, **kwargs):
        self.execute("DELETE FROM dataset WHERE name = %(name)s", kwargs)

    @db_bind_keys('star_id', 'type_id', 'src_id')
    def fetch_property_by_id(self, **kwargs):
        """Fetch a property given (star_id, type_id, src_id)"""
        sql = """SELECT p.*
                          FROM property p
                          JOIN star s ON s.id = p.star
                          JOIN datatype dt ON dt.id = p.type
                          JOIN source src ON src.id = p.source
                          JOIN reference r ON r.id = p.reference
                     LEFT JOIN instrument i ON i.id = p.instrument
                         WHERE s.id = %(star_id)s
                           AND dt.id = %(type_id)s
                           AND src.id = %(src_id)s"""
        # TODO: need to make compound object: star, source, reference, instrument
        db_property = self.fetch_row(sql, kwargs)
        return db_property

    def fetchall_properties(self, **binds):
        sql = """SELECT p.*
                          FROM property p
                          JOIN star s ON s.id = p.star
                          JOIN datatype dt ON dt.id = p.type
                          JOIN source src ON src.id = p.source
                          JOIN reference r ON r.id = p.reference
                     LEFT JOIN instrument i ON i.id = p.instrument"""
        sql += self.build_filter(binds, {'star_id'  : 's.id',
                                         'datatype' : 'dt.name',
                                         'source'   : 'src.name'})
        # TODO: need to make compound object: star, source, reference, instrument
        result = self.fetchall(sql, binds)
        return result

    @db_bind_keys('star_id', 'type_id', 'src_id')
    def fetch_timeseries_by_id(self, **kwargs):
        """Fetch a timeseries given (star_id, type_id, src_id)"""
        sql = """SELECT ts.*
                          FROM timeseries ts
                          JOIN star s ON s.id = ts.star
                          JOIN datatype dt ON dt.id = ts.type
                          JOIN source src ON src.id = ts.source
                          JOIN reference r ON r.id = ts.reference
                     LEFT JOIN instrument i ON i.id = ts.instrument
                         WHERE s.id = %(star_id)s
                           AND dt.id = %(type_id)s
                           AND src.id = %(src_id)s"""
        # TODO: need to make compound object: star, source, reference, instrument
        db_property = self.fetch_row(sql, kwargs)
        return db_property

    # TODO: candidate for sqlhappy?
    def attach_result(self, key, input, method, obj, attach=True, setid=False):
        """Set obj[key] = input if 'id' in input, else fetch new result from DB with method(input)

        attach may be set to a string, in that case the result is attached to obj[attach][key]
        
        if setid is True, then obj[key+'_id'] = result['id'] as well.
        attach may be set to None in which case only setid will be done.
        """
        db_obj = None
        if 'id' not in input:
            db_obj = method(input)
            if db_obj is None:
                raise MissingDataError("Could not set fetch data with '%s' using '%s'" % (str(method), str(input)))
        else:
            db_obj = input
        if setid:
            obj[key+'_id'] = input['id']
        if attach is not None:
            if attach is True:
                obj[key] = db_obj
            elif isinstance(attach, str):
                obj.setdefault(attach, {})
                obj[attach][key] = db_obj

    def prepare_err(self, obj):
        if obj.get('err') is not None:
            # Case where error is expressed as a percent
            if isinstance(obj['err'], str) and obj['err'].endswith('%'):
                obj['err'] = abs(obj['val']) * float(obj['err'].rstrip('%')) / 100.0

            # Set lo and hi bounds of the error
            obj['errhi'] = obj['err']
            obj['errlo'] = obj['err']
            del obj['err']

        # Error as range
        if obj.get('errbounds') is None and obj.get('errlo') is not None and obj.get('errhi') is not None:
            lo = obj['val'] - obj['errlo']
            hi = obj['val'] + obj['errhi']
            obj['errbounds'] = psycopg2.extras.NumericRange(lo, hi, '[]')

    def prepare_time(self, obj):
        # All measurement times must use the astropy.time.Time object
        # and will be stored in the database using the Barycentric
        # Coordinate Time scale.
        #
        # All non-measurement times must be datetime.datetime objects,
        # and are assumed to be in the UTC time scale.
        #
        # There are three related time measurement quantities:
        #   obs_time: left edge of observation time
        #   obs_dur:  duration of observation, in seconds
        #   obs_range: (time_start, time_stop) pair
        # if obs_range is set, then the other two are derived
        # if obs_time, obs_dur is set, then obs_range are derived
        type_exc = Exception('astropy.time.Time is the standard time object for sunstardb.')
        if obj.get('obs_time') is not None:
            if not isinstance(obj['obs_time'], astropy.time.Time):
                raise type_exc
            # Convert observation time to TCB time scale, and convert to datetime object for insertion
            obj['obs_time'] = obj['obs_time'].tcb.datetime
            # Set obs_range if obs_dur is available
            if obj.get('obs_range') is None and obj.get('obs_dur') is not None:
                t1 = obj['obs_time']
                t2 = t1 + astropy.time.TimeDelta(obj['obs_dur'], format='sec')
                obj['obs_range'] = (t1, t2)
                
        if obj.get('obs_range') is not None:
            t1 = obj['obs_range'][0]
            t2 = obj['obs_range'][1]
            for t in [t1, t2]:
                if not isinstance(t, astropy.time.Time):
                    raise type_exc
            # Convert to TCB time scale
            t1 = t1.tcb
            t2 = t2.tcb
            # Set observation time if not already set
            # obs_time is set to start time of obs_range
            if obj.get('obs_time') is None:
                obj['obs_time'] = t1.datetime
            # Set observation duration if not already set
            if obj.get('obs_dur') is None:
                obj['obs_dur'] = (t2 - t1).sec # difference is a TimeDelta object

            # Convert observation duration to TCB time scale, and convert to DateTimeRange for insertion
            obj['obs_range'] = psycopg2.extras.DateTimeRange(t1.datetime, t2.datetime, '[)')

    def explicit_null(self, obj, *colnames):
        for col in colnames:
            if col not in obj:
                obj[col] = None

    def insert_datum(self, datum, star, datatype, source, reference, instrument=None):
        datum = self.prepare_datum(datum, star, datatype, source, reference, instrument=instrument)
        
        if datatype['struct'] in ['MEASURE', 'LABEL']:
            db_datum = self.insert_property(datum)
        elif datatype['struct'] == 'TIMESERIES':
            db_datum = self.append_timeseries(datum)
        else:
            raise Exception("unexpected datatype struct '%s'" % datatype['struct'])
        return db_datum

    def prepare_datum(self, datum, star, datatype, source, reference, instrument=None):
        """Prepare a data point for insertion into the database
        
        Input:
         - datum      : dict containing data point and references
         - star       : dict containing star data
         - datatype   : dict containing datatype data
         - source     : dict containing source data
         - reference  : dict containing reference data
         - instrument : (optional) dict containing instrument data

        If (star, datatype, source, reference, instrument) do not have
        the 'id' key set, then it will be fetched from the database
        using the 'name' key.
        """
        # Set id for sub-objects
        self.attach_result('star', star, self.fetch_star, datum, attach='ref', setid=True)
        self.attach_result('type', datatype, self.fetch_datatype, datum, attach='ref', setid=True)
        self.attach_result('src', source, self.fetch_source, datum, attach='ref', setid=True)
        self.attach_result('ref', reference, self.fetch_reference, datum, attach='ref', setid=True)
        if instrument is not None:
            self.attach_result('inst', instrument, self.fetch_instrument, datum, attach='ref', setid=True)

        # Set err
        self.prepare_err(datum)

        # Set timestamps
        self.prepare_time(datum)

        # Explicit None for all NULLable columns
        self.explicit_null(datum, 'inst_id', 'errlo', 'errhi', 'errbounds', 
                                   'obs_time', 'obs_dur', 'obs_range',
                                   'meta')

        return datum
    
    @db_bind_keys('star_id', 'type_id', 'src_id', 'ref_id', 'inst_id', 'meta')
    def insert_property(self, **kwargs):
        """Insert a property given database ids (star_id, type_id, src_id, ref_id, inst_id) and (meta)
        
        Use prepare_property to fetch the necessary IDs from the database
        """
        sql = """INSERT INTO property (star, type, source, reference, instrument)
                      VALUES (%(star_id)s, %(type_id)s, %(src_id)s, %(ref_id)s, %(inst_id)s)
                      RETURNING *
               """
        db_prop = self.insert_returning(sql, kwargs)
        kwargs['prop_id'] = db_prop['id']
        db_type = kwargs['ref']['type']
        kwargs['name'] = db_type['name']
        if db_type['struct'] == 'MEASURE':
            self.insert_measure(kwargs)
        elif db_type['struct'] == 'LABEL':
            self.insert_label(kwargs)
        else:
            raise Exception("Unexpected datatype.type: %(type)s" % db_type)
        return db_prop

    @db_bind_keys('name', 'prop_id', 'star_id', 'src_id',
                  'val', 'errlo', 'errhi', 'errbounds', 'obs_time', 'obs_dur', 'obs_range', 'meta')
    def insert_measure(self, **kwargs):
        """Insert a measurement-type property into its data table
        
        Requires ('name', 'prop_id', 'star_id', 'src_id', 'val',
                  'errlo', 'errhi', 'errbounds', 'obs_time',
                  'obs_dur', 'obs_range', 'meta')
        """
        sql = """INSERT INTO dat_%(name)s (property, star, source,
                                           %(name)s, errlo, errhi, errbounds, obs_time, obs_dur, obs_range, meta)
                      VALUES (%%(prop_id)s, %%(star_id)s, %%(src_id)s,
                              %%(val)s, %%(errlo)s, %%(errhi)s, %%(errbounds)s,
                              %%(obs_time)s, %%(obs_dur)s, %%(obs_range)s, %%(meta)s)""" % kwargs # set 'name' first
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

    @db_bind_keys('star_id', 'type_id', 'src_id', 'ref_id', 'inst_id', 'meta')
    def append_timeseries(self, **kwargs):
        """Insert a property given database ids (star_id, type_id, src_id, ref_id, inst_id) and (meta)
        
        Use prepare_property to fetch the necessary IDs from the database
        """
        db_ts = self.fetch_timeseries_by_id(kwargs)
        if db_ts is None:
            # XXX TODO set global meta
            sql = """INSERT INTO timeseries (star, type, source, reference, instrument)
                          VALUES (%(star_id)s, %(type_id)s, %(src_id)s, %(ref_id)s, %(inst_id)s)
                   """
            self.execute(sql, kwargs)
            db_ts = self.fetch_timeseries_by_id(kwargs)
        else:
            sql = """UPDATE timeseries SET append_time = current_timestamp
                      WHERE star = %(star_id)s AND
                            type = %(type_id)s AND
                            source = %(src_id)s"""
            self.execute(sql, kwargs)
        kwargs['ts_id'] = db_ts['id']
        db_type = self.fetch_datatype_by_id(kwargs)
        kwargs['name'] = db_type['name']
        self.insert_timepoint(kwargs)
        return db_ts

    @db_bind_keys('name', 'ts_id', 'star_id', 'src_id',
                  'val', 'errlo', 'errhi', 'errbounds', 'obs_time', 'obs_dur', 'obs_range', 'meta')
    def insert_timepoint(self, **kwargs):
        """Insert a timeseries point into its data table
        
        Requires ('name', 'ts_id', 'star_id', 'src_id', 'val',
                  'errlo', 'errhi', 'errbounds', 'obs_time',
                  'obs_dur', 'obs_range', 'meta')
        """
        sql = """INSERT INTO dat_%(name)s (timeseries, star, source, obs_time, obs_dur, obs_range,
                                           %(name)s, errlo, errhi, errbounds, meta)
                      VALUES (%%(ts_id)s, %%(star_id)s, %%(src_id)s,
                              %%(obs_time)s, %%(obs_dur)s, %%(obs_range)s,
                              %%(val)s, %%(errlo)s, %%(errhi)s, %%(errbounds)s,
                               %%(meta)s)""" % kwargs # set 'name' first
        self.execute(sql, kwargs) # DB driver to bind the rest
        return None # TODO: return timepoint?

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

    def fetchall_datasets(self):
        """Fetch all existing datasets from the database"""
        sql = """SELECT * FROM dataset"""
        return self.fetchall_astropy(sql)

    def sanity_check(self, tasks, source, verbose=True):
        """Execute sanity checks for the given source"""
        def maybe_print(msg):
            if verbose:
                print(msg)
        if 'exists_all_stars' in tasks:
            for datatype in tasks['exists_all_stars']:
                maybe_print("Checking: '%s' data exists for all stars..." % datatype)
                self.check_exists_all_stars(datatype, source['id'])
                maybe_print("CONFIRMED '%s' data exists for all stars." % datatype)

    def check_exists_all_stars(self, datatype, src_id):
        """Sanity check that data of the given datatype exists for all stars of the given src_id"""
        sql = """SELECT DISTINCT s.*
                   FROM property p
                   JOIN star s on s.id = p.star
                  WHERE p.source = %(src_id)s
                    AND p.star NOT IN (SELECT p2.star
                                         FROM property p2
                                         JOIN datatype dt on dt.id = p2.type
                                        WHERE p2.source = %(src_id)s
                                          AND dt.name = %(datatype)s)
              """
        results, colnames = self.fetchall(sql, { 'datatype' : datatype, 'src_id' : src_id }, colnames=True)
        if results is not None:
            n_bad = len(results)
            message = "CHECK FAILED: The following %i stars do not have '%s' data:\n" % (n_bad, datatype)
            message += "\t".join(colnames) + "\n"
            for row in results:
                message += "\t".join(str(i) for i in row) + "\n"
            raise Exception(message)
        else:
            return True

    def fetchall_astropy(self, sql, binds=None, dtype=None):
        """Return all results of an SQL query as an astropy.table.Table, or None

        Input:
         - sql <string> : SELECT statement to execute
         - binds <dict> : optional bind parameters
         
        Output:
         - <Table> : table of results
        """
        result, colnames = self.fetchall(sql, binds, colnames=True)
        table = astropy.table.Table(rows=result, names=colnames, dtype=dtype)
        return table

    def fetch_data(self, datatype):
        """Fetch data and associated info for the given datatype"""
        
        sql = """SELECT s.name star, r.name reference, o.name origin, o.kind origin_kind, i.name instrument,
                        d.%(datatype)s "%(datatype)s", d.errhi, d.errlo
                   FROM dat_%(datatype)s d
                   JOIN property p ON p.id = d.property
                   JOIN star s ON s.id = p.star
                   JOIN reference r ON r.id = p.reference
                   JOIN source src ON src.id = p.source
                   JOIN origin o ON o.id = src.origin
                   LEFT JOIN instrument i ON i.id = p.instrument"""
        # TODO: validate to prevent SQL injection
        result = self.fetchall_astropy(sql % {'datatype':datatype})
        return result

    def fetch_data_table(self, dataset, datatypes, meta=None, nulls=True, errors=False):
        """Fetch star names and data as a table for the given dataset

        Inputs:
         - dataset <str>     : dataset name
         - datatypes <array> : an array of datatypes to fetch
         - nulls <bool>      : whether to allow null values in the columns

         Output:
          - result <array> : an array of psycopg2.extras.DictRow

        If 'nulls' is True, all existing data will be returned.  If
        'nulls' is False, only rows for which data exists for each
        given datatype will be returned.

        The DictRow results may be accessed like a dict.  Each
        row will have a 'star' key, pointing to the star
        name, and a key for each datatype given.
        """
        # XXX TODO: protect against using timeseries data types here,
        #           or, provide subqueries which turn timeseries into scalars
        ixs = list(range(len(datatypes)))

        # Define source sub-tables
        sql = "WITH "
        for i in ixs:
            dtype = datatypes[i]
            dcols = 'd.*'
            if meta and dtype in meta:
                # listify a simple string value
                if isinstance(meta[dtype], str):
                    meta[dtype] = [ meta[dtype] ]
                for metacol in meta[dtype]:
                    dcols += ", d.meta->>'%s' \"%s\"" % (metacol, metacol)
            sql += """d%02i AS (
                        SELECT %s FROM dat_%s d
                          JOIN dataset_map dm ON dm.property = d.property
                          JOIN dataset ds ON ds.id = dm.dataset
                         WHERE ds.name = %%(dataset)s ),
            """ % (i, dcols, datatypes[i])
        sql += "uq_stars AS ( "
        sql += " UNION ".join( "SELECT star FROM d%02i" % i for i in ixs)

        # Output columns
        # TODO: "errors" assumes all columns are MEASURE types... check datatype table instead
        sql += ") SELECT s.name star, "
        col_pattern = "d%(index)02i.%(name)s \"%(name)s\"" # preserve case in output columns
        if errors:
            col_pattern += ", d%(index)02i.errlo \"errlo_%(name)s\", d%(index)02i.errhi \"errhi_%(name)s\""
        sql += ", ".join( col_pattern % dict(index=i, name=datatypes[i]) for i in ixs )
        if meta:
            for dtype in meta:
                i = datatypes.index(dtype)
                for metacol in meta[dtype]:
                    sql += ", d%(index)02i.\"%(name)s\" \"%(name)s\"" % dict(index=i, name=metacol)

        # Source sub-tables
        sql += " FROM uq_stars us JOIN star s ON s.id = us.star"
        if nulls is True:
            jointype = "LEFT JOIN"
        else:
            jointype = "JOIN"
            
        for i in ixs:
            sql += " %s d%02i ON d%02i.star = s.id" % (jointype, i, i)

        result = self.fetchall_astropy(sql, { 'dataset' : dataset})
        return result

    def fetch_data_cols(self, dataset, datatypes, nulls=True, errors=False):
        """Fetch a data table as a dictionary of columns

        Inputs:
         - dataset <str>     : dataset name
         - datatypes <array> : an array of datatypes to fetch
         - nulls <bool>      : whether to allow null values in the columns

         Output:
          - cols <dict>   : dictionary containing columns

        If 'nulls' is True, all existing data will be returned.  If
        'nulls' is False, only rows for which data exists for each
        given datatype will be returned.

        The returned data format is like: 
          { 'datatype1' : [ 1, 2, 3, ... ], 'datatype2' : [ 1, 2, 3 ], ... }
        """

        result = self.fetch_data_table(dataset, datatypes, nulls=nulls, errors=errors)
        return self.list_to_columns(result)

    def fetch_timeseries(self, datatype, star, source=None):
        """Fetch timeseries of a given datatype, star, and (optional) source

        Inputs:
          - datatype <str> : datatype name
          - star <str>     : SIMBAD-recognized star name
          - source <str>   : (optional) source name

        Output:
          - <astropy.table.Table> : table of (obs_time, datatype, errlo, errhi)

        The 'obs_time' column is formatted as a datetime object.
        Timeseries values are found in a column with the same name as
        `datatype`.
        """
        sql = """SELECT obs_time, %(name)s \"%(name)s\", errlo, errhi
                 FROM dat_%(name)s d
                 JOIN star_alias sa ON sa.star = d.star\n""" % dict(name=datatype)
        if source is not None:
            sql += "JOIN source src ON src.id = d.source"
        where = "replace(sa.name, ' ', '') = replace(%(star)s, ' ', '')"
        binds = {'star':star}
        if source is not None:
            where += " AND src.name = %(source)s"
            binds['source'] = source
        sql += " WHERE " + where

        result = self.fetchall_astropy(sql, binds, dtype=('object', 'f', 'f', 'f'))
        return result

    def fetch_boxmatch(self, dataset, skycoord, ra_side, dec_side=None, orient='center'):
        """Search dataset for stars falling in a box near to skycoord"""
        if dec_side is None:
            dec_side = ra_side

        if orient == 'center':
            box = { 'ra_min' : skycoord.ra.degree - ra_side/2.0,
                    'ra_max' : skycoord.ra.degree + ra_side/2.0,
                    'dec_min' : skycoord.dec.degree - dec_side/2.0,
                    'dec_max' : skycoord.dec.degree + dec_side/2.0 }
        else:
            raise Exception("invalid orientation '%s'" % orient)
        
        sql = """SELECT DISTINCT s.name, s.ra, s.dec
                   FROM dataset ds
                   JOIN dataset_map dm ON dm.dataset = ds.id
                   JOIN star s ON s.id = dm.star
                  WHERE ds.name = %(dataset)s
                    AND q3c_poly_query(s.ra, s.dec,
                                      '{%(ra_max)s, %(dec_max)s,
                                       %(ra_max)s, %(dec_min)s,
                                       %(ra_min)s, %(dec_min)s,
                                       %(ra_min)s, %(dec_max)s}')"""
        result = self.fetchall(sql, dict(list({'dataset':dataset}.items()) + list(box.items())))
        return result
