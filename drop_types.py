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

types = args
if len(types) == 0:
    raise Exception("No property types provided")
elif len(types) == 1 and types[0] == '*':
    print "!!! Dropping ALL property types !!!"
    types = db.fetchall_property_types()


for t in types:
    print "Dropping", t
    db.drop_property_type(name=t)

db.commit()
db.close()

