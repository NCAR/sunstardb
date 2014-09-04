#!/usr/bin/env python

import json
import sys
import os
import os.path

import sunstardb
import utils
import plotfunc

parser = sunstardb.db_optparser()
(options, args) = parser.parse_args()
db_params = sunstardb.db_kwargs(options)
print "Connecting to database...",
db = sunstardb.SunStarDB(**db_params)
print "Done."

command, profile, ptypes = args[0], args[1], args[2:]

if command == 'print':
    data = db.fetch_data_table(profile, ptypes, nulls=True)
    print "\t".join( ['star'] + ptypes )
    for row in data:
        print "\t".join( str(v) for v in row.values() )
elif command == 'scatter':
    x, y = ptypes[0:2]
    data = db.fetch_data_cols(profile, [x, y], nulls=False)
    plotfunc.show_scatter(data[x], data[y], "%s vs %s" % (x, y),
                          xlabel=x, ylabel=y, s=10)
elif command == 'hist':
    x = ptypes[0]
    data = db.fetch_data_cols(profile, [x], nulls=False)
    plotfunc.show_hist(data[x], x)
