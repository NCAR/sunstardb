#!/usr/bin/env python

import json
import sys
import os

import sunstardb

parser = sunstardb.db_optparser()
(options, args) = parser.parse_args()
db_params = sunstardb.db_kwargs(options)
print "Connecting to sunstardb...",
db = sunstardb.SunStarDB(**db_params)
print "Done."

file = args[0]
print "Loading %s," % (file), 
fp = open(file)
js = json.load(fp)
instruments = js['instruments']
print len(instruments), "instruments in file."

print "Inserting property types into database."
for i in instruments:
    if db.fetch_instrument(i) is None:
        print "Inserting instrument:", i['name']
        db.insert_instrument(i)
    else:
        print i['name'], "already exists... skipping"

db.commit()
db.close()
fp.close()
