#!/usr/bin/env python

import json
import sys
import os

from sunstardb.database import SunStarDB

(options, args, db) = SunStarDB.cli_connect()
types = args
if len(types) == 0:
    raise Exception("No datatypes provided")
elif len(types) == 1 and types[0] == '*':
    print "!!! Dropping ALL datatypes !!!"
    types = db.fetchall_datatypes()


for t in types:
    print "Dropping", t
    db.drop_datatype(name=t)

db.commit()
db.close()

