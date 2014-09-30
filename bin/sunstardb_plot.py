#!/usr/bin/env python

import json
import sys
import os
import os.path

from sunstardb.database import SunStarDB
from sunstardb import utils
import plothappy

(options, args, db) = SunStarDB.cli_connect()

def no_data_exit():
    print "No data for dataset '%s'" % dataset
    exit()

command = args[0]
if command == 'print':
    dataset, types = args[1], args[2:]
    data = db.fetch_data_table(dataset, types, nulls=True)
    if data is None:
        no_data_exit()
    print "\t".join( ['star'] + types )
    for row in data:
        print "\t".join( str(v) for v in row.values() )
elif command == 'scatter':
    dataset, types = args[1], args[2:]
    x, y = types[0:2]
    data = db.fetch_data_cols(dataset, [x, y], nulls=False)
    if data is None:
        no_data_exit()
    plothappy.show_scatter(data[x], data[y], "%s vs %s" % (x, y),
                          xlabel=x, ylabel=y, s=10)
elif command == 'hist':
    dataset, types = args[1], args[2:]
    x = types[0]
    data = db.fetch_data_cols(dataset, [x], nulls=False)
    if data is None:
        no_data_exit()
    plothappy.show_hist(data[x], x)
elif command == 'timeseries':
    type, star = args[1], args[2]
    datasets = args[3:] if len(args) > 3 else None
        
    times, data = db.fetch_timeseries(type, star, datasets)
    plothappy.show_scatter(times, data, "%s %s" % (star, type), xlabel='Time', ylabel=type, s=10)
