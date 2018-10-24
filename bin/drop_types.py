#!/usr/bin/env python

import json
import sys
import os

from sunstardb.database import SunStarDB

(args, db) = SunStarDB.cli_connect([dict(name='types', nargs='+')])
if len(args.types) == 0:
    raise Exception("No datatypes provided")
elif len(args.types) == 1 and args.types[0] == '*':
    print("!!! Dropping ALL datatypes !!!")
    droptypes = db.fetchall_datatypes()
    droptypes = droptypes['name'].data
else:
    droptypes = args.types

for t in droptypes:
    print("Dropping", t)
    db.drop_datatype(name=t)

db.commit()
db.close()

