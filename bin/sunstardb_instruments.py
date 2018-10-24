#!/usr/bin/env python

import json
import sys
import os

from sunstardb.database import SunStarDB

(args, db) = SunStarDB.cli_connect([dict(name='json_file')])

file = args.json_file
print("Loading %s," % (file), end=' ') 
fp = open(file)
js = json.load(fp)
instruments = js['instruments']
print(len(instruments), "instruments in file.")

print("Inserting property types into database.")
for i in instruments:
    if db.fetch_instrument(i) is None:
        print("Inserting instrument:", i['name'])
        db.insert_instrument(i)
    else:
        print(i['name'], "already exists... skipping")

db.commit()
db.close()
fp.close()
