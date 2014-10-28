#!/usr/bin/env python

import json
import sys
import os
import os.path
import numpy

from sunstardb.database import SunStarDB
from sunstardb import utils
import plothappy

(options, args, db) = SunStarDB.cli_connect()

def no_data_exit(**input):
    input = ["%s='%s'" % (k, v) for k, v in input.items()]
    print "No data for", ", ".join(input)
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
    data = db.fetch_data_cols(dataset, [x, y], nulls=False, errors=True)
    if data is None:
        no_data_exit()
    # replace None in error columns with numpy nan
    # TODO: numpy conversion functions in database package? utils?
    for hilo in ['errhi_', 'errlo_']:
        for axis in [x, y]:
            col = hilo + axis
            data[col] = numpy.array(data[col], dtype=numpy.float)
    plothappy.show_plot(data[x], data[y], "%s vs %s" % (x, y),
                        xlabel=x, ylabel=y,
                        xerr=[data['errlo_'+x], data['errhi_'+x]],
                        yerr=[data['errlo_'+y], data['errhi_'+y]],
                        )
elif command == 'hist':
    dataset, types = args[1], args[2:]
    x = types[0]
    data = db.fetch_data_cols(dataset, [x], nulls=False)
    if data is None:
        no_data_exit()
    plothappy.show_hist(data[x], x)
elif command == 'timeseries':
    type, star = args[1], args[2]
    source = args[3] if len(args) > 3 else None
    result = db.fetch_timeseries(type, star, source)
    if result is None:
        no_data_exit(source=source)
    plothappy.show_plot(result['obs_time'], result[type],
                        "%s %s" % (star, type), xlabel='Time', ylabel=type,
                        yerr=[result['errlo'], result['errhi']]
                        )
