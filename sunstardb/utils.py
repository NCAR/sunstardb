import os
import re
from datetime import datetime, timedelta
import astropy.coordinates
import astropy.time

def modification_date(filename):
    t = os.path.getmtime(filename)
    return datetime.fromtimestamp(t)

def jyear_utc(jyear):
    return astropy.time.Time(jyear, format='jyear', scale='utc')
    
def compress_space(s):
    return re.sub(r' +', ' ', s)

def wikiword(s):
    """Turn a string into a word suitable for using in a wiki URL"""
    return re.sub(r'[^\w-]', ' ', s).title().replace(' ', '')

FIRST_TIME = None
LAST_TIME = None
def time_reset():
    global FIRST_TIME
    global LAST_TIME
    FIRST_TIME = datetime.now()
    LAST_TIME = FIRST_TIME
time_reset() # set the globals on module load

def time_lap():
    global LAST_TIME
    t = datetime.now()
    elapsed = (t - LAST_TIME).total_seconds()
    LAST_TIME = t
    return elapsed

def time_total():
    t = datetime.now()
    elapsed = (t - FIRST_TIME).total_seconds()
    return elapsed

def parse_skycoord(ra, dec=None, frame='icrs'):
    # possible input formats:
    # 'hh:mm:ss', '+dd:mm:ss'
    # 'hh:mm:ss +dd:mm:ss'
    # 'hh mm ss +dd mm ss'
    if dec is None:
        a = ra.split(' ')
        if len(a) == 2:
            ra = a[0]
            dec = a[1]
        elif len(a) == 6:
            ra = ' '.join(a[0:3])
            dec = ' '.join(a[3:])

    skycoord = astropy.coordinates.SkyCoord(ra, dec, frame='icrs',
                                            unit=(astropy.units.hourangle,
                                                  astropy.units.degree))
    return skycoord
    
