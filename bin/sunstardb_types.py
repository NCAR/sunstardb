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
types = js['datatypes']
print len(types), "datatypes in file."

print "Inserting datatypes into database."
for t in types:
    if db.fetch_datatype(t) is None:
        print "Inserting datatype:", t['name']
        db.insert_datatype(t)
    else:
        print t['name'], "already exists... skipping"

db.commit()
db.close()
fp.close()
