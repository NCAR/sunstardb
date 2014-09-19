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

print "Begining ingestion of data package '%s'" % dataname

print "Inserting reference '%s'" % dataobj.reference['name']
db_ref = db.insert_reference(dataobj.reference)

print "Inserting origin '%s'" % dataobj.origin['name']
db_origin = db.insert_origin(dataobj.origin)

print "Inserting source '%s'" % dataobj.source['name']
db_source = db.insert_source(origin_id=db_origin['id'], **dataobj.source)

print "Inserting properties..."
starlist = {}
n_prop = 0
newstars = 0
for p in dataobj.data():
    star = p['star']
    ptype = p['type']

    db_star = db.fetch_star(name=star)
    if db_star is None:
        print "Inserting new star '%s'..." % star,
        db_star = db.insert_star(name=star)
        print "resolved as '%s'" % db_star['name']
        newstars += 1
    starlist[db_star['id']] = db_star

    print "Inserting property '%s' for star '%s' ('%s' in source)" % (ptype, db_star['name'], star)
    property = db.prepare_property(p, db_star, {'name':ptype}, db_source, db_ref)
    db_prop = db.insert_property(property)
    n_prop += 1

n_stars = len(starlist)
print "Inserted %i properties for %i stars (%i new)" % (n_prop, n_stars, newstars)

print "Creating dataset for source '%s'" % db_source['name']
db.create_dataset_from_source(db_source)

if dataobj.sanity_check is not None:
    print "Performing sanity checks"
    db.sanity_check(dataobj.sanity_check, db_source)

print "Finished loading data package '%s', commiting." % dataname
db.commit()
db.close()
