from functools import wraps
from database import *
import psycopg2, psycopg2.extras
import astroquery.simbad

# Consider all dicts as Json type
psycopg2.extensions.register_adapter(dict, psycopg2.extras.Json)

# useful globals
COLS2SIMBAD = dict(hd='HD', bright='*', proper='NAME')
SIMBAD2COLS = dict((v,k) for k, v in COLS2SIMBAD.iteritems())
IDTYPES = ( 'id', 'hd', 'bright', 'proper' ) # in order of preference

def pref_idtype(names):
    """Return the preferred star ID from those available in the given dict"""
    for idtype in IDTYPES:
        if idtype in names and names[idtype] is not None:
            return idtype

def extract_star(x):
    """Return a dict with sunstardb star names extracted from x"""
    y = {}
    for idtype in IDTYPES:
        if idtype in x:
            y[idtype] = x[idtype]
    if len(y) == 0:
        return None
    else:
        return y

# TODO: move to auxilary class that does not require DB connection?
def lookup_simbad_ids(object_name):
    """Lookup the given object in Simbad and return a set of sunstardb star names"""
    table = astroquery.simbad.Simbad.query_objectids(object_name)
    if table is None:
        return None
    names = {}
    for o in table:
        idtype, id = o[0].split(" ", 1)
        idtype = idtype.strip()
        id = id.strip()
        if idtype in SIMBAD2COLS:
            names[SIMBAD2COLS[idtype]] = id
    return names

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
            if len(args) == 1 and isinstance(args[0], dict):
                kwargs = args[0]
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

    @db_bind_keys('name')
    def fetch_property_type(self, **kwargs):
        sql = "SELECT * FROM property_type WHERE name=%(name)s"
        db_property_type = self.fetch_row(sql, kwargs)
        return db_property_type

    @db_bind_keys('name', 'type', 'units', 'description')
    def insert_property_type(self, **kwargs):
        sql = """INSERT INTO property_type (name, type, units, description)
                      VALUES (%(name)s, %(type)s, %(units)s, %(description)s)"""
        self.execute(sql, kwargs)
        return self.fetch_property_type(kwargs)

    @db_bind_keys('name')
    def fetch_instrument(self, **kwargs):
        sql = "SELECT * FROM instrument WHERE name=%(name)s"
        db_instrument = self.fetch_row(sql, kwargs)
        return db_instrument

    @db_bind_keys('name', 'long', 'url', 'doc_url', 'description')
    def insert_instrument(self, **kwargs):
        sql = """INSERT INTO instrument (name, long, url, doc_url, description)
                      VALUES (%(name)s, %(long)s, %(url)s, %(doc_url)s, %(description)s)"""
        self.execute(sql, kwargs)
        return self.fetch_instrument(kwargs)

    @db_bind_keys() # no required keys
    def fetch_star(self, **kwargs):
        print "XXX fetch_star kwargs", kwargs
        idtype = pref_idtype(kwargs)
        sql = "SELECT * FROM star WHERE %s = %%(%s)s" % (idtype, idtype)
        db_star = self.fetch_row(sql, { idtype : kwargs[idtype] })
        return db_star

    @db_bind_keys() # no required keys
    def insert_star(self, **kwargs):
        idtype = pref_idtype(kwargs)
        object_name = COLS2SIMBAD[idtype] + " " + kwargs[idtype]
        simbad_ids = lookup_simbad_ids(object_name)
        if simbad_ids is not None:
            star = simbad_ids
        else:
            star = kwargs

        for id in COLS2SIMBAD:
            if id not in star:
                star[id] = None

        print "XXX inserting star", star
        insert_sql = """INSERT INTO star (hd, bright, proper) 
                             VALUES (%(hd)s, %(bright)s, %(proper)s)"""
        self.execute(insert_sql, star)
        return self.fetch_star(star)

    @db_bind_keys('name')
    def fetch_reference(self, **kwargs):
        sql = """SELECT * FROM reference WHERE name=%(name)s"""
        return self.fetch_row(sql, kwargs)

    @db_bind_keys('name', 'bibline', 'bibcode')
    def insert_reference(self, **kwargs):
        sql = """INSERT INTO reference (name, bibline, bibcode)
                      VALUES (%(name)s, %(bibline)s, %(bibcode)s)"""
        self.execute(sql, kwargs)
        return self.fetch_reference(kwargs)

    @db_bind_keys('name')
    def fetch_origin(self, **kwargs):
        sql = """SELECT * FROM origin WHERE name=%(name)s"""
        return self.fetch_row(sql, kwargs)

    @db_bind_keys('name', 'kind', 'url', 'doc_url', 'description')
    def insert_origin(self, **kwargs):
        sql = """INSERT INTO origin (name, kind, url, doc_url, description)
                      VALUES (%(name)s, %(kind)s, %(url)s, %(doc_url)s, %(description)s)"""
        self.execute(sql, kwargs)
        return self.fetch_origin(kwargs)

    @db_bind_keys('name', 'kind', 'url', 'doc_url', 'description')
    def insert_origin(self, **kwargs):
        sql = """INSERT INTO origin (name, kind, url, doc_url, description)
                      VALUES (%(name)s, %(kind)s, %(url)s, %(doc_url)s, %(description)s)"""
        self.execute(sql, kwargs)
        return self.fetch_origin(kwargs)

    @db_bind_keys('url')
    def fetch_source(self, **kwargs):
        sql = """SELECT * FROM source WHERE url=%(url)s"""
        if 'version' in kwargs:
            if kwargs['version'] is not None:
                sql += " AND version=%(version)s"
            else:
                sql += " AND version IS NULL"
        print "XXX fetch_source", sql
        print "XXX fetch_source", kwargs
        return self.fetch_row(sql, kwargs)

    @db_bind_keys('kind', 'url', 'origin_id', 'source_time')
    def insert_source(self, **kwargs):
        if 'version' not in kwargs:
            kwargs['version'] = None
        if 'source_id' not in kwargs:
            kwargs['source_id'] = None
    
        sql = """INSERT INTO source (kind, url, version, origin, source, source_time)
                      VALUES (%(kind)s, %(url)s, %(version)s, %(origin_id)s, %(source_id)s, %(source_time)s)"""
        self.execute(sql, kwargs)
        return self.fetch_source(kwargs)
    
    @db_bind_keys('star_id', 'type_id')
    def fetch_property_by_id(self, **kwargs):
        sql = """SELECT * FROM property p
                          JOIN star s ON s.id = p.star
                          JOIN property_type t ON t.id = p.type
                          JOIN source src ON src.id = p.source
                          JOIN reference r ON r.id = p.reference
                     LEFT JOIN instrument i ON i.id = p.instrument
                         WHERE s.id = %(star_id)s
                           AND t.id = %(type_id)s"""
        # TODO: need to make compound object: star, source, reference, instrument
        db_property = self.fetch_row(sql, kwargs)
        return db_property

    def prepare_property(self, property, star, ptype, source, reference, instrument=None):
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

        # String values
        if isinstance(property['val'], basestring):
            property['strval'] = property['val']
            property['val'] = None

        # Explicit None for all NULLable columns
        for col in ('val', 'err', 'strval', 'obs_time', 'int_time', 'meta'):
            if col not in property:
                property[col] = None

        # Err as range
        if property['err'] is not None:
            # TODO: allow for asymetric ranges with list, e.g. [-0.1, 0.2]
            # TODO: is err as range useful for queries, or val + err ?
            err = property['err']
            property['err'] = psycopg2.extras.NumericRange(-err, +err)

        return property
    
    @db_bind_keys('star_id', 'type_id', 'src_id', 'ref_id', 'inst_id',
                  'val', 'err', 'strval',
                  'obs_time', 'int_time',
                  'meta')
    def insert_property(self, **kwargs):
        sql = """INSERT INTO property (star, type, source, reference, instrument,
                                       val, err, strval,
                                       obs_time, int_time,
                                       meta)
                      VALUES (%(star_id)s, %(type_id)s, %(src_id)s, %(ref_id)s, %(inst_id)s,
                             %(val)s, %(err)s, %(strval)s,
                             %(obs_time)s, %(int_time)s,
                             %(meta)s)
               """
        self.execute(sql, kwargs)
        return self.fetch_property_by_id(kwargs)
