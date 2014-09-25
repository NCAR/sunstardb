#!/usr/bin/env python

import json
import sys
import os
import os.path

from sunstardb.database import SunStarDB
from sunstardb import datapkg
from sunstardb import utils

(options, args, db) = SunStarDB.cli_connect()
dataname = args[0]
dataobj = datapkg.load_class(dataname)

def fatal_if(bool, message):
    if bool:
        print message
        print "Exiting."
        exit(-1)

print "Begining ingestion of data package '%s'" % dataname

print "Inserting reference '%s'" % dataobj.reference['name']
db_ref = db.insert_reference(dataobj.reference)

print "Inserting origin '%s'" % dataobj.origin['name']
db_origin = db.insert_origin(dataobj.origin)

print "Inserting source '%s'" % dataobj.source['name']
db_source = db.insert_source(origin_id=db_origin['id'], **dataobj.source)

print "Inserting data..."
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

    print "Inserting datatype '%s' for star '%s' ('%s' in source)" % (datatype, db_star['name'], star)
    db.insert_datum(datum, db_star, db_type, db_source, db_ref)
    n_data += 1

n_stars = len(star_cache)
print "Inserted %i data points for %i stars (%i new)" % (n_data, n_stars, newstars)

print "Creating dataset for source '%s'" % db_source['name']
db.create_dataset_from_source(db_source)

if dataobj.sanity_check is not None:
    print "Performing sanity checks"
    db.sanity_check(dataobj.sanity_check, db_source)

print "Finished loading data package '%s', commiting." % dataname
db.commit()
db.close()
