from functools import wraps
import os
import os.path

from database import *

import psycopg2, psycopg2.extras
import astroquery.simbad

# Consider all dicts as Json type
psycopg2.extensions.register_adapter(dict, psycopg2.extras.Json)

# useful globals
COLS2SIMBAD = dict(hd='HD', bright='*', proper='NAME')
SIMBAD2COLS = dict((v,k) for k, v in COLS2SIMBAD.iteritems())
IDTYPES = ( 'id', 'hd', 'bright', 'proper' ) # in order of preference
TABLE_TEMPLATES = {}

def _set_templates():
    dir = os.path.dirname(os.path.abspath(__file__))
    schema = os.path.join(dir, 'schema', 'create_sunstardb.sql')
    file = open(schema)
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

def star_str(star):
    """Return a single string to describe a star row from the DB"""
    for idtype in IDTYPES[1:]: # skip raw database ID
        if idtype in star and star[idtype] is not None:
            return "(%s) %s" % (idtype, star[idtype])

def canonical_to_simbad(idtype, id):
    """Change a canonical ID to a SIMBAD object name"""
    return COLS2SIMBAD[idtype] + " " + id
    
def split_simbad_id(simbad_id):
    idtype, id = simbad_id.split(" ", 1)
    idtype = idtype.strip()
    id = id.strip()
    return idtype, id

# TODO: move to auxilary class that does not require DB connection?
def lookup_simbad_ids(object_name):
    """Lookup the given object in Simbad and return a dict of arrays"""

    table = astroquery.simbad.Simbad.query_objectids(object_name)
    if table is None:
        return None
    names = {}
    for o in table:
        idtype, id = split_simbad_id(o[0])
        if idtype not in names:
            names[idtype] = []
        names[idtype].append(id)
    return names

def simbad_ids_to_canonical(simbad_names):
    canonical_names = {}
    for idtype, idary in simbad_names.items():
        if idtype in SIMBAD2COLS:
            # By default take the first ID of a given type in the array.
            # Special cases are below.
            canonical_id = idary[0]
            if idtype == 'HD' and len(idary) > 1:
                # Prefer HD IDs that end in letters; they are more precise
                for id in idary:
                    if id[-1].isalpha():
                        canonical_id = id
                        break
            canonical_names[ SIMBAD2COLS[idtype] ] = canonical_id

    return canonical_names

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
    @db_bind_keys('name')
    def fetch_property_type(self, **kwargs):
        sql = "SELECT * FROM property_type WHERE name=%(name)s"
        db_property_type = self.fetch_row(sql, kwargs)
        return db_property_type

    def fetchall_property_types(self, **kwargs):
        sql = "SELECT name FROM property_type"
        return self.fetch_column(sql)

    @db_bind_keys('type_id')
    def fetch_property_type_by_id(self, **kwargs):
        sql = "SELECT * FROM property_type WHERE id=%(type_id)s"
        db_property_type = self.fetch_row(sql, kwargs)
        return db_property_type

    @db_bind_keys('name', 'type', 'units', 'description')
    def insert_property_type(self, **kwargs):
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
        self.execute("""DELETE FROM profile_map WHERE type IN
                        (SELECT id FROM property_type WHERE name = %(name)s)""", kwargs)
        self.execute("""DELETE FROM property WHERE type IN
                        (SELECT id FROM property_type WHERE name = %(name)s)""", kwargs)
        self.execute("DELETE FROM property_type WHERE name = %(name)s", kwargs)
        self.execute("DROP TABLE dat_%(name)s" % kwargs) # TODO: check arg

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
        idtype = pref_idtype(kwargs)
        sql = "SELECT * FROM star WHERE %s = %%(%s)s" % (idtype, idtype)
        db_star = self.fetch_row(sql, { idtype : kwargs[idtype] })
        return db_star

    @db_bind_keys() # no required keys
    def insert_star(self, **kwargs):
        idtype = pref_idtype(kwargs)
        simbad_name = canonical_to_simbad(idtype, kwargs[idtype])
        simbad_ids = lookup_simbad_ids(simbad_name)
        if simbad_ids is not None:
            star = simbad_ids_to_canonical(simbad_ids)
        elif 'proper' in kwargs and kwargs['proper'] == 'Sun':
            # The Sun is not in SIMBAD, make exception to allow it to be inserted into database
            star = kwargs
        else:
            # Require that object exists in SIMBAD
            raise Exception("Object '%s' not found in SIMBAD. Input was '%s'" % (simbad_name, repr(kwargs)))

        # Explicit NULL for non-existing columns
        for id in COLS2SIMBAD:
            if id not in star:
                star[id] = None

        # Canonical name chosen by simbad_ids_to_cannoical() may not
        # be same as kwargs value, and may in fact already be in the
        # database.  Here we check for it in the database before
        # proceeding with the INSERT.
        db_star = self.fetch_star(star)
        if db_star is not None:
            return db_star

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

    @db_bind_keys('url')
    def fetch_source(self, **kwargs):
        sql = """SELECT * FROM source WHERE url=%(url)s"""
        if 'version' in kwargs:
            if kwargs['version'] is not None:
                sql += " AND version=%(version)s"
            else:
                sql += " AND version IS NULL"
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
        sql = """INSERT INTO dat_%(name)s (property, star, source, %(name)s, meta)
                      VALUES (%%(prop_id)s, %%(star_id)s, %%(src_id)s,
                              %%(label)s, %%(meta)s)""" % kwargs # set 'name' first
        self.execute(sql, kwargs) # DB driver to bind the rest
        return None # TODO: return measure?

    @db_bind_keys('name')
    def create_profile_from_origin(self, **kwargs):
        db_origin = self.fetch_origin(kwargs)
        sql = """INSERT INTO profile (name, description) VALUES (%(name)s, %(description)s)"""
        profile = { 'name'        : db_origin['name'],
                    'description' : db_origin['description'] }
        self.execute(sql, profile)
        sql = """INSERT INTO profile_map (profile, star, type, property)
                 SELECT prof.id, prop.star, prop.type, prop.id
                   FROM profile prof
                   JOIN origin o on o.name = prof.name
                   JOIN source s on s.origin = o.id
                   JOIN property prop on prop.source = s.id
                  WHERE prof.name = %(name)s"""
        self.execute(sql, profile)
        
    def check_exists_all_stars(self, ptype, src_id):
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

    def fetch_data_table(self, profile, ptypes, nulls=True):
        """Fetch star names and data as a table for the given profile

        Inputs:
         - profile <str>  : profile name
         - ptypes <array> : an array of property types to fetch
         - nulls <bool>   : whether to allow null values in the columns

         Output:
          - result <array> : an array of psycopg2.extras.DictRow

        If 'nulls' is True, all existing data will be returned.  If
        'nulls' is False, only rows for which data exists for each
        given ptype will be returned.

        The DictRow results may be accessed like a dict.  Each
        row will have a 'star' key, pointing to the canonical star
        name, and a key for each property type given.
        """
        ixs = range(len(ptypes))
        sql = "WITH "
        for i in ixs:
            sql += """d%02i AS (
                        SELECT d.* FROM dat_%s d
                          JOIN profile_map pm ON pm.property = d.property
                          JOIN profile pf ON pf.id = pm.profile
                         WHERE pf.name = %%(profile)s ),
            """ % (i, ptypes[i])
        sql += "uq_stars AS ( "
        sql += " UNION ".join( "SELECT star FROM d%02i" % i for i in ixs)
        sql += ") SELECT s.canon star, "
        sql += ", ".join( "d%02i.%s" % (i, ptypes[i]) for i in ixs )
        sql += " FROM uq_stars us JOIN star s ON s.id = us.star"
        if nulls is True:
            jointype = "LEFT JOIN"
        else:
            jointype = "JOIN"
            
        for i in ixs:
            sql += " %s d%02i ON d%02i.star = s.id" % (jointype, i, i)

        result = self.fetchall(sql, { 'profile' : profile})
        return result

    def fetch_data_cols(self, profile, ptypes, nulls=True):
        """Fetch a data table as a dictionary of columns

        Inputs:
         - profile <str>  : profile name
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

        result = self.fetch_data_table(profile, ptypes, nulls=nulls)
        cols = {}
        col_list = ['star'] + ptypes
        for c in col_list:
            cols[c] = []
        for row in result:
            for c in col_list:
                cols[c].append(row[c.lower()])
        return cols
