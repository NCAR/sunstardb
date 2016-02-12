#!/usr/bin/env python

import json
import sys
import os
import os.path

from sunstardb.database import SunStarDB
from sunstardb import datapkg
from sunstardb import utils

more_args = [ dict(name='datapkg'),
              dict(flag='--nocommit', action='store_true', help="For testing, skip committing at the end.") ]
(args, db) = SunStarDB.cli_connect(more_args)
dataname = args.datapkg
dataobj = datapkg.load_class(dataname)

def fatal_if(bool, message):
    if bool:
        print "ERROR:", message
        print "Exiting."
        exit(-1)

print "Begining ingestion of data package '%s'" % dataname

db_ref = db.fetch_reference(dataobj.reference)
if db_ref is None:
    print "Inserting reference '%s'" % dataobj.reference['name']
    db_ref = db.insert_reference(dataobj.reference)

db_origin = db.fetch_origin(dataobj.origin)
if db_origin is None:
    print "Inserting origin '%s'" % dataobj.origin['name']
    db_origin = db.insert_origin(dataobj.origin)

# TODO: currently source is a one-time use.  What about appending to a long time series?
print "Inserting source '%s'" % dataobj.source['name']
db_source = db.insert_source(origin_id=db_origin['id'], **dataobj.source)

global_instr = None
instrument_cache = None
if dataobj.instrument is not None:
    if isinstance(dataobj.instrument, str):
        global_instr = db.fetch_instrument({'name': dataobj.instrument})
        fatal_if(db_instr is None, "Instrument '%s' is not in the database" % dataobj.instrument)
    elif isinstance(dataobj.instrument, list):
        instrument_cache = {}
        for instrument in dataobj.instrument:
            db_i = db.fetch_instrument({'name': instrument})
            fatal_if(db_i is None, "Instrument '%s' is not in the database" % instrument)
            instrument_cache[instrument] = db_i
    else:
        print "ERROR: unexpected instrument specification."
        print "Exiting."
        exit(-1)

print "Inserting data..."
utils.time_reset()
star_cache = {}
type_cache = {}
n_data = 0
newstars = 0
for datum in dataobj.data():
    # Fetch datatype from DB or cache
    datatype = datum['type']
    if datatype not in type_cache:
        db_type = db.fetch_datatype(name=datatype)
        fatal_if(db_type is None, "datatype '%s' not found in the database." % datatype)
        type_cache[datatype] = db_type
    else:
        db_type = type_cache[datatype]

    # Fetch star from DB or cache, or insert new star
    star = datum['star']
    if star not in star_cache:
        db_star = db.fetch_star(name=star)
        if db_star is None:
            print "Inserting new star '%s'..." % star,
            db_star = db.insert_star(name=star)
            print "resolved as '%s'" % db_star['name']
            newstars += 1
        star_cache[star] = db_star
    else:
        db_star = star_cache[star]

    # Handle instrument selection for multiple instruments
    if 'instrument' in datum:
        fatal_if(instrument_cache is None, "no instrument list specified in datapkg")
        fatal_if(datum['instrument'] not in instrument_cache, "'%s' not in instrument list" % datum['instrument'])
        db_instr = instrument_cache[datum['instrument']]
    else:
        db_instr = global_instr

    print "Inserting datatype '%s' for star '%s' ('%s' in source)" % (datatype, db_star['name'], star)
    if args.debug:
        print 'DATUM:', datum
    db.insert_datum(datum, db_star, db_type, db_source, db_ref, db_instr)
    n_data += 1

n_stars = len(star_cache)
print "Inserted %i data points for %i stars (%i new)" % (n_data, n_stars, newstars),
print "in %0.3f seconds" % utils.time_total()

print "Creating dataset for source '%s'" % db_source['name']
db.create_dataset_from_source(db_source)

if dataobj.sanity_check is not None:
    print "Performing sanity checks"
    db.sanity_check(dataobj.sanity_check, db_source)

print "Finished loading data package '%s'," % dataname,
if not args.nocommit:
    print "committing"
    db.commit()
else:
    print "NOT COMMITING because of --nocommit option."

db.close()
