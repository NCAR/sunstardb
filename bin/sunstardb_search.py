#!/usr/bin/env python

import sys
import os
import os.path
import numpy

from sunstardb.database import SunStarDB
from sunstardb import utils

(options, args, db) = SunStarDB.cli_connect()

command, args = args[0], args[1:]

if command == 'boxmatch':
    side = float(args[0])
    dataset = args[1]
    inputfile = args[2]
    for line in open(inputfile):
        line = line.strip()
        ra, dec = line.split('\t')
        skycoord = utils.parse_skycoord(ra, dec)
        result = db.fetch_boxmatch(dataset, skycoord, side, orient='center')
        print "%0.1f deg box search centered on ra=%0.3f deg, dec=%0.3f deg:" % \
            (side, skycoord.ra.degree, skycoord.dec.degree)
        print result
        print
else:
    print "Invalid command:", command
    exit(-1)
