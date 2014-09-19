#!/usr/bin/env python

import json
import sys
import os
import os.path

from sunstardb.database import SunStarDB
from sunstardb import utils
import plothappy

(options, args, db) = SunStarDB.cli_connect()

command, dataset, ptypes = args[0], args[1], args[2:]

def no_data_exit():
    print "No data for dataset '%s'" % dataset
    exit()

if command == 'print':
    data = db.fetch_data_table(dataset, ptypes, nulls=True)
    if data is None:
        no_data_exit()
    print "\t".join( ['star'] + ptypes )
    for row in data:
        print "\t".join( str(v) for v in row.values() )
elif command == 'scatter':
    x, y = ptypes[0:2]
    data = db.fetch_data_cols(dataset, [x, y], nulls=False)
    if data is None:
        no_data_exit()
    plothappy.show_scatter(data[x], data[y], "%s vs %s" % (x, y),
                          xlabel=x, ylabel=y, s=10)
elif command == 'hist':
    x = ptypes[0]
    data = db.fetch_data_cols(dataset, [x], nulls=False)
    if data is None:
        no_data_exit()
    plothappy.show_hist(data[x], x)
