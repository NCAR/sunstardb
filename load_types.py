#!/usr/bin/env python

import json
import sys
import os

import sunstardb

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
types = js['property_types']
print len(types), "property types in file."

print "Inserting property types into database."
for t in types:
    if db.fetch_property_type(t) is None:
        print "Inserting type:", t['name']
        db.insert_property_type(t)
    else:
        print t['name'], "already exists... skipping"

db.commit()
db.close()
fp.close()
