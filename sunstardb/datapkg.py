import os
import os.path
import json
import importlib
import datetime
import astropy.time
import re

from . import utils

def load_class(name):
    """Returns a DataReader class for the given datapkg name"""
    modname = 'datapkg.' + name
    mod = importlib.import_module(modname)
    return mod.DataReader()

def data_iter(name):
    """Returns a generator function to iterate through the data of the given name"""
    datareader = load_class(name)
    return datareader.data()

class BaseDataReader(object):
    """Base class for all DataReader instances
    
    Each data format is a python module which defines a DataReader
    class, which in turn defines the data() method.  The data() method
    is a generator function which is used to iterate through a series
    of objects that can be imported by sunstardb.  DataReader is the
    separation between the inhomogeneous data in the astronomy
    commmunity, and the homogenous data of sunstardb.

    This base class contains functionality which can be useful for any
    DataReader.
    """

    def __init__(self):
        """Create a new DataReader and load the info.json if present"""
        self.origin = None
        self.source = None
        self.reference = None
        self.instrument = None
        self.sanity_check = None
        self.extras = None

        # Set the directory to find files.  Child class must set childfile first!
        self.dir = os.path.dirname(self.childfile)
        self.datapkg = os.path.split(self.dir)[-1]

        # Load info file, if found
        infofile = self.file('info.json')
        if infofile is not None:
            self._load_info(infofile)

        # Prepare information about this data source
        self._set_source()
        

    def file(self, filename):
        """Return a filepath to the given filename in the data directory"""
        filepath = os.path.join(self.dir, filename)
        if not os.path.isfile(filepath):
            return None
        else:
            return filepath

    def listdir(self, dirname=None):
        """Return a list of filepaths for files in the given subdirectory"""
        if dirname is None:
            dirpath = self.dir
        else:
            dirpath = os.path.join(self.dir, dirname)
        if os.path.isdir(dirpath):
            files = [ os.path.join(dirpath, f) for f in os.listdir(dirpath)]
            return files
        else:
            raise Exception("Directory '%s' does not exist" % dirpath)

    def listdir_matching(self, regex, dirname=None):
        """Return a list of files matching regex in the given subdirectory"""
        files = []
        for file in self.listdir(dirname):
            if re.match(regex, os.path.basename(file)):
                files.append(file)
        return files

    def data(self):
        """Return a generator object for sunstardb insert objects

        To be implemented by DataReader subclass defined in each data
        module.  This base implementation only raises an exception.
        """
        raise Exception("Subclass needs to define data() method!")
    
    def _load_info(self, file):
        """Load an info.json data file in the data directory"""
        fp = open(file)
        js = json.load(fp)

        # Required keys
        self.reference = js['reference']
        self.origin = js['origin']

        # Optional keys
        self.instrument = js.get('instrument')
        self.sanity_check = js.get('sanity_check')
        
        # Extra info
        for k, v in js.items():
            if k in ('reference', 'origin', 'instrument', 'sanity_check'):
                continue
            if self.extras is None:
                self.extras = {}
            self.extras[k] = v

        fp.close()

        # If the origin is a paper, copy some data from the reference
        if self.origin['kind'] == 'PAPER':
            self.origin['name'] = self.reference['name']
            self.origin['url'] = 'adslabs.harvard.edu/abs/%s' % (self.reference['bibcode'])
            self.origin['description'] = self.reference['bibline']

    def _set_source(self):
        """Set the data package source information

        Expects taht self.dir and self.datapkg have been set first.
        """
        source_time = utils.modification_date(self.dir)
        # TODO: 'source.kind' hardcoded to "FILE".  What about 'code'?  Will
        #       'FILE' always be intermediate?
        # TODO: take 'source.version' from package __version__ ?
        self.source = dict(name=self.datapkg,
                           kind="FILE",
                           source_time=source_time)
        
    def info(self):
        """Returns information defined in the info.json file
        
        Returns a dict with keys ('reference', 'source', 'origin', 'instrument')
        """
        return { "reference"  : self.reference,
                 "source"     : self.source,
                 "origin"     : self.origin,
                 "instrument" : self.instrument }

    def extras(self):
        """Returns dict of extra information found in the info.json file"""
        return self.extras

class TextDataReader(BaseDataReader):
    def typecast(self, obj, typemap, debug=False, time_scale=None, offset=None):
        if debug:
            print "DEBUG obj:", obj
            print "DEBUG typemap:", typemap
        for k, typecode in typemap.items():
            o = obj[k].strip()
            if typecode == 's':
                pass
            elif typecode == 'i':
                if o == '':
                    obj[k] = None
                else:
                    obj[k] = int(o)
            elif typecode == 'f':
                if o == '':
                    obj[k] = None
                else:
                    obj[k] = float(o)
            elif typecode.startswith('T'): # 'T' for time
                if time_scale is None:
                    raise Exception('astropy.time.Time scale must be specified for date parsing')
                time_format = typecode[1:]
                # Option 1: format is one recognized by astropy.time.Time
                if time_format in astropy.time.Time.FORMATS.keys():
                    # Convert these to float first
                    if time_format in ('byear', 'cxcsec', 'gps', 'jd', 'jyear', 'mjd', 'plot_date', 'unix'):
                        obj[k] = float(obj[k])
                    if offset is not None:
                        obj[k] += offset
                    obj[k] = astropy.time.Time(obj[k], format=time_format, scale=time_scale)
                # Option 2: format is one recognized by datetime.strptime()
                else:
                    datestr = obj[k].replace(' ','0') # ' 1/ 2/2014 -> '01/02/2014'
                    try:
                        dateobj = datetime.datetime.strptime(datestr, time_format)
                        obj[k] = astropy.time.Time(dateobj, format='datetime', scale=time_scale)
                    except ValueError, e:
                        # TODO: this is probably a bad idea.
                        #  Return also an error dict showing which field parsed badly?
                        #  Allowing the client to try to re-parse?
                        obj[k] = None
            else:
                raise Exception('unknown typecode %s' % typecode)

    def parse_deliminated(self, file, colnames, typemap, delim=r'\s+', skip=0, debug=False, **typecast_kwargs):
        fh = open(file)
        linenum = 0
        for line in fh:
            linenum += 1
            if linenum <= skip:
                continue
            line = line.strip()
            row = re.split(delim, line)
            result = dict(zip(colnames, row))
            if debug:
                print "DEBUG %06i line:" % linenum, line
                print "DEBUG %06i row:" % linenum, row
            self.typecast(result, typemap, **typecast_kwargs)
            if debug:
                print "DEBUG %06i result:" %linenum, result
            yield result
        fh.close()

    def byteparse(self, line, spec, debug=False, **typecast_kwargs):
        line = line.rstrip('\n')
        result = {}
        typemap = {}
        if debug:
            print "DEBUG line:", line
            print "DEBUG spec:", spec
        for k in spec:
            (initial, final, typecode) = spec[k]
            result[k] = line[initial:final]
            typemap[k] = typecode
        self.typecast(result, typemap, debug=debug, **typecast_kwargs)
        return result

    def stripstr(self, datadict, strlist=None):
        for k,v in datadict.items():
            if type(v) is str and (strlist is None or k in strlist):
                datadict[k] = v.strip()
                if datadict[k] == '':
                    datadict[k] = None
        return datadict

class JsonDataReader(BaseDataReader):
    def data(self):
        fh = open(self.file('properties.json'))
        js = json.load(fh)
        properties = js['properties']
        defaults = js.get('defaults')
        scale = js.get('time_scale')

        def set_default(ptype, obj):
            if defaults is None or ptype not in defaults:
                return
            for k in defaults[ptype]:
                if k not in obj:
                    obj[k] = defaults[ptype][k]

        for ptype in properties:
            for p in properties[ptype]:
                p['type'] = ptype
                set_default(ptype, p)
                # Check for time data.  Ensure the scale is specified
                if (p.get('obs_time') is not None or p.get('obs_range') is not None) and scale is None:
                    raise Exception("Must specify time scale to use for 'obs_time' and 'obs_range'")
                # Recast times as astropy.time.Time
                if p.get('obs_time') is not None:
                    p['obs_time'] = astropy.time.Time(p['obs_time'], scale=scale)
                if p.get('obs_range') is not None:
                    t1, t2 = p['obs_range']
                    p['obs_range'] = (astropy.time.Time(t1, scale=scale), astropy.time.Time(t2, scale=scale))
                yield p
        fh.close()

class DuplicateTimeIncrementor(astropy.time.TimeDelta):
    last_time = None

    def process(self, time):
        if self.last_time is not None and self.last_time == time:
            time = time + self
        self.last_time = time
        return time

    def reset(self):
        self.last_time = None
