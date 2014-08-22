#!/usr/bin/env python

import json
import sys
import os
import os.path

import sunstardb
import utils

parser = sunstardb.db_optparser()
(options, args) = parser.parse_args()
db_params = sunstardb.db_kwargs(options)
print "Connecting to database...",
db = sunstardb.SunStarDB(**db_params)
print "Done."
        
file = os.path.abspath(args[0])
print "Loading %s," % (file), 
fp = open(file)
js = json.load(fp)
reference = js['reference']
origin = js['origin']
instrument = js['instrument']
properties = js['properties']
if 'postproc' in js:
    postproc = js['postproc']
else:
    postproc = None

# If the origin is a paper, copy some data from the reference
if origin['kind'] == 'PAPER':
    origin['name'] = reference['name']
    origin['url'] = 'adslabs.harvard.edu/abs/%s' % (reference['bibcode'])
    origin['doc_url'] = 'TODO'
    origin['description'] = reference['bibline']

print "Inserting reference '%s'" % reference['name']
db_ref = db.insert_reference(reference)

print "Inserting origin '%s'" % origin['name']
db_origin = db.insert_origin(origin)

source_time = utils.modification_date(file)
source = dict(kind="FILE", origin_id=db_origin['id'], url=file, source_time=source_time)
print "Inserting source '%s'" % source['url']
db_source = db.insert_source(source)

set_err = {}
if postproc is not None:
    if 'set_err' in postproc:
        set_err = postproc['set_err']

print "Inserting properties..."
starlist = {}
n_prop = 0
for ptype in properties:
    for p in properties[ptype]:
        star = sunstardb.extract_star(p)
        db_star = db.fetch_star(star)
        if db_star is None:
            print "Inserting star", sunstardb.star_str(star)
            db_star = db.insert_star(star)
        starlist[db_star['id']] = db_star

        # Setting error
        if ptype in set_err:
            p['err'] = set_err[ptype]

        print "Inserting property '%s' for star '%s'" % (ptype, sunstardb.star_str(db_star))
        property = db.prepare_property(p, db_star, {'name':ptype}, db_source, db_ref)
        db_prop = db.insert_property(property)
        n_prop += 1

if 'sanity_check' in js:
    print "Performing sanity checks"
    tasks = js['sanity_check']
    if 'exists_all_stars' in tasks:
        for ptype in tasks['exists_all_stars']:
            print "Checking '%s' data exists for all stars..." % ptype,
            db.check_exists_all_stars(ptype, db_source['id'])
            print "OK."

n_stars = len(starlist)
print "Inserted %i properties for %i stars" % (n_prop, n_stars)

if db_origin['kind'] == 'PAPER':
    print "Creating profile for PAPER '%s'" % db_origin['name']
    db.create_profile_from_origin(db_origin)

print "Finished loading property file '%s', commiting." % file
db.commit()
db.close()
fp.close()
