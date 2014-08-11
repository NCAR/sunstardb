#!/usr/bin/env python

import json
import sys
import os

import sunstardb
import utils

parser = sunstardb.db_optparser()
(options, args) = parser.parse_args()
db_params = sunstardb.db_kwargs(options)
print "Connecting to database...",
db = sunstardb.SunStarDB(**db_params)
print "Done."
        
file = args[0]
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

print "Inserting reference..."
print "XXX reference", reference
db_ref = db.insert_reference(reference)
print "XXX db_ref", db_ref

print "Inserting origin..."
print "XXX origin", origin
db_origin = db.insert_origin(origin)
print "XXX db_origin", db_origin
print "XXX db_origin['id']", db_origin['id']

print "Inserting source..."
source_time = utils.modification_date(file)
print "XXX source_time", type(source_time), source_time
source = dict(kind="FILE", origin_id=db_origin['id'], url=file, source_time=source_time)
print "XXX source", source
db_source = db.insert_source(source)
print "XXX db_source", db_source

set_err = {}
if postproc is not None:
    if 'set_err' in postproc:
        set_err = postproc['set_err']

print "Inserting properties..."
for ptype in properties:
    for p in properties[ptype]:
        print "XXX ptype", ptype
        print "XXX property read", p
        star = sunstardb.extract_star(p)
        print "XXX star", star
        db_star = db.fetch_star(star)
        if db_star is None:
            db_star = db.insert_star(star)
        print "XXX db_star", db_star

        # Setting error
        if ptype in set_err:
            p['err'] = set_err[ptype]

        property = db.prepare_property(p, db_star, {'name':ptype}, db_source, db_ref)

        print "XXX property built", property
        db_prop = db.insert_property(property)
        print "XXX db_prop", db_prop

if db_origin['kind'] == 'PAPER':
    print "Creating profile from PAPER"
    print "XXX db_origin", db_origin
    print "XXX db_origin['id']", db_origin['id']
    db.create_profile_from_origin(db_origin)

db.commit()
db.close()
fp.close()
