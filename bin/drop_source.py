#!/usr/bin/env python

import json
import sys
import os

from sunstardb.database import SunStarDB

(args, db) = SunStarDB.cli_connect([dict(name='datapkg', nargs='+')])
sources = args.datapkg
for s in sources:
    print "Dropping", s
    db.delete_source(name=s)

db.commit()
db.close()
