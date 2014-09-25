import os.path

DIR = os.path.dirname(__file__)
def file(filename):
    return os.path.join(DIR, filename)
