import os
import re
import datetime

def modification_date(filename):
    t = os.path.getmtime(filename)
    return datetime.datetime.fromtimestamp(t)

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
    FIRST_TIME = datetime.datetime.now()
    LAST_TIME = FIRST_TIME
time_reset() # set the globals on module load

def time_lap():
    global LAST_TIME
    t = datetime.datetime.now()
    elapsed = (t - LAST_TIME).total_seconds()
    LAST_TIME = t
    return elapsed

def time_total():
    t = datetime.datetime.now()
    elapsed = (t - FIRST_TIME).total_seconds()
    return elapsed
