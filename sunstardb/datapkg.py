import os
import os.path
import json
import importlib
import datetime

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
    def byteparse(self, line, **spec):
        line = line.rstrip('\n')
        result = {}
        for k in spec:
            (initial, final, type) = spec[k]
            result[k] = line[initial:final]
            if type == 's':
                pass
            elif type == 'i':
                result[k] = int(result[k].strip())
            elif type == 'f':
                result[k] = float(result[k].strip())
            elif type.startswith('D'):
                format = type[1:]
                datestr = result[k].replace(' ','0')
                try:
                    result[k] = datetime.datetime.strptime(datestr, format)
                except ValueError, e:
                    # TODO: this is probably a bad idea.
                    #  Return also an error dict showing which field parsed badly?
                    #  Allowing the client to try to re-parse?
                    result[k] = None
            else:
                raise Exception('unknown type %s' % format)
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
                yield p
        fh.close()
