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
