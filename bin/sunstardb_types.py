#!/usr/bin/env python

import json
import sys
import os

from sunstardb.database import SunStarDB

(options, args, db) = SunStarDB.cli_connect()

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
