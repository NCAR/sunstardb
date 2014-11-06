#!/usr/bin/env python

import json
import sys
import os
import os.path
import numpy
import argparse

from sunstardb.database import SunStarDB
from sunstardb import utils
import plothappy

(args, db) = SunStarDB.cli_connect([dict(name='command', 
                                         choices=['print', 'scatter', 'hist', 'timeseries']
                                         ),
                                    dict(name='args',
                                         nargs=argparse.REMAINDER)
                                    ])

def no_data_exit(**input):
    input = ["%s='%s'" % (k, v) for k, v in input.items()]
    print "No data for", ", ".join(input)
    exit()

if args.command == 'print':
    dataset, types = args.args[0], args.args[1:]
    data = db.fetch_data_table(dataset, types, nulls=True)
    if data is None:
        no_data_exit()
    data.pprint(max_lines=-1, max_width=-1)
elif args.command == 'scatter':
    dataset, types = args.args[0], args.args[1:]
    x, y = types[0:2]
    data = db.fetch_data_table(dataset, [x, y], nulls=False, errors=True)
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
elif args.command == 'hist':
    dataset, types = args.args[0], args.args[1:]
    x = types[0]
    data = db.fetch_data_table(dataset, [x], nulls=False)
    if data is None:
        no_data_exit()
    plothappy.show_hist(data[x], x)
elif args.command == 'timeseries':
    type, star = args.args[0], args.args[1]
    source = args.args[2] if len(args.args) > 2 else None
    result = db.fetch_timeseries(type, star, source)
    if result is None:
        no_data_exit(source=source)
    plothappy.show_plot(result['obs_time'], result[type],
                        "%s %s" % (star, type), xlabel='Time', ylabel=type,
                        yerr=[result['errlo'], result['errhi']]
                        )
