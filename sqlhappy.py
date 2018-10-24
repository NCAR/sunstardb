"""
Base class for DB-interacting classes and scripts
"""

import psycopg2, psycopg2.extras
from datetime import datetime, timedelta
import sys
import os
import math
import numpy
from argparse import ArgumentParser
import configparser
import getpass

class Database():
    """ Base class for DB-interacting classes.

    This class contains helper functions for common interactions.  The
    idea here is to simplify common interactions in order to reduce
    the amount of database-level code needed in actual application
    code.

    This class simplifies the use of transactions by implicitely
    maintaining a transaction at all times.  Subclasses or users of
    this code therefore only need to remember to commit() or
    rollback() at the desired transaction boundary, and don't need to
    bother with remembering to start a transaction.

    It also handles all 'connection time' configuration options, so
    that the application can operate with a uniform environment.
    """

    def __init__(self,
                 drivername = "postgres",
                 host = "localhost", port = None, database = None,
                 username = None, password = None,
                 debug = False
                 ):
        """Connect to a database

        Attempts to make a database connection defined by the
        following order of precedence:

        1. if database or username are passed, use those parameters
           (drivername, host, port, database, username, password)
        2. lookup a databse connection from the [connection]
           section of the config file in the DBCONFIG environment var.

        Input:
         - host <str> : connection host name, default 'localhost'
         - port <int> : connection port
         - database <str> : database name
         - username <str> : database user name
         - password <str> : password for username
         - debug <bool> : enable debug mode
        """
        self.debug = debug

        # If the user provided a database or user build a dburl
        if (database is not None or username is not None):
            conn_params = { 'host':host, 'database':database, 'username':username, 'password':password }
        # If we have no connect info, look in the configuration
        elif os.environ.get('DBCONFIG'):
            config = configparser.ConfigParser()
            configfile = os.environ.get('DBCONFIG')
            config.readfp(open(configfile))
            conn_params = dict(config.items('connection'))            
        else:
            raise Exception("Connection info not provided.")

        conn_str = "host='%(host)s' dbname='%(database)s' user='%(username)s' password='%(password)s'" % \
            conn_params


        self.conn_params = conn_params
        self.conn_str = conn_str
        self.open()
        return

    def __del__(self):
        """Destructor that insures database connection is closed
        """
        if not self.connection.closed:
            self.close()

    def makebinds(self, data, names):
        """Build binds (list of maps) from a list of lists

        Input:
         - data  <list> : a list of lists containing data
         - names <list> : a list of strings to use as key names

        Output:
         - binds <list> : a list of dicts usable in execute()

        TODO: implement this
        """
        raise NotImplementedError()

    def clean_binds(self, binds):
        """Execute type casts necessary for psycopg2 to accept bind data
        """
        for key in binds:
            value = binds[key]
            if isinstance(value, numpy.floating):
                binds[key] = float(value)
            elif isinstance(value, numpy.integer):
                binds[key] = int(value)

    def execute(self, sql, binds = None):
        """execute sql with optional bind parameters, which can be arrays

        The given sql statement ought to have bind variable
        placeholders in the pyformat format, e.g. %(var_name)s.
        'var_name' then corresponds to a key in the 'binds'
        dictionary(ies)

        Input:
         - sql <str>    : the SQL statement to execute
         - binds <list> : a list of dicts containing bind data

        """
        cursor = self.connection.cursor()

        if self.debug:
            print("SQL:", sql)

        if binds is None:
            cursor.execute(sql)
        else:
            self.clean_binds(binds)
            cursor.execute(sql, binds)

        return cursor

    def commit(self):
        """Commit the current transaction"""
        self.connection.commit()

    def rollback(self):
        """Rollback the current transaction"""
        self.connection.rollback()

    def open(self):
        """Open a new connection and cursor"""
        # TODO: NamedTupleCursor is more powerful. Update?
        self.connection = psycopg2.connect(self.conn_str, cursor_factory=psycopg2.extras.DictCursor)

    def close(self):
        """Close the current connection"""
        self.connection.rollback()
        self.connection.close()

    def dict_(self, result, key = 0, val = 1):
        """Return a simple dictionary from a result set

        By default returns dict of { col[0] : col[1] }

        Inputs:
         - result <object> : a ResultProxy object containing DB results
         - key <str>       : key of 'result' to use as the output keys
         - val <str>       : key of 'result' to use as the output value

        Output:
         - <dict> : a dictionary of key -> val
        """
        result_dict = {}
        for row in result:
            result_dict[row[key]] = row[val]
        result.close()
        return result_dict

    def colnames(self, cursor):
        return tuple(desc[0] for desc in cursor.description)

    def fetchall(self, sql, binds = None, colnames = False):
        """Return all results of an SQL query, or None

        Input:
         - sql <string> : SELECT statement to execute
         - binds <dict> : optional bind parameters
         
        Output:
         - <list> : a list of rows
        """
        result = self.execute(sql, binds)
        if colnames is True:
            colnames = self.colnames(result)
        all = result.fetchall()
        result.close()
        if len(all) == 0:
            all = None

        if not colnames:
            return all
        else:
            return all, colnames

    def fetchall_dict(self, sql, binds = None, key = 0, val = 1):
        """Return sql query as a dictionary of { col[0] : col[1] }

        Input:
         - sql <string> : SELECT statement to execute
         - binds <dict> : optional bind parameters
         - key <string> : column name or index to define the key of the dict
         - val <string> : column name or index to define the value of the dict

        Output:
         - <dict> : a dictionary of key -> val
        """
        # TODO: if val = {}, values are dictionaries with the row data
        # TODO: if key is a list, keys are the concatemation of several cols
        result = self.execute(sql, binds)
        return self.dict_(result, key, val)

    def fetchall_tuples(self, sql, binds = None):
        """Return sql query as a list of tuples

        Input:
         - sql <string> : SELECT statement to execute
         - binds <dict> : optional bind parameters
         
        Output:
         - <list> : a list of tuples
        """
        
        tuple_list = []
        result = self.execute(sql, binds)
        for row in result:
            row_tuple = (row[:])
            tuple_list.append(row_tuple)
        result.close()
        return tuple_list

    def list_to_columns(self, result):
        """Turn an result list to a dict of lists (columns)
        Input:
         - results <list> : list of ResultProxy objects
         - names <list> : list of column names
        
        """
        if result is None:
            return None
        col_list = list(result[0].keys())
        cols = {}
        for c in col_list:
            cols[c] = []
        for row in result:
            for c in col_list:
                cols[c].append(row[c])
        return cols

    def fetchall_columns(self, sql, binds = None):
        """Return sql query as a dict of lists

        Input:
         - sql <string> : SELECT statement to execute
         - binds <dict> : optional bind parameters
         
        Output:
         - <dict> : a dict of columns identified by name
        """
        result = self.fetchall(sql, binds)
        return self.list_to_columns(result)

    def row(self, result):
        """Return the first row of a result set

        Input:
         - result <object> : a ResultProxy object

        Output:
         - <object> : RowProxy object, the first row of 'result'
        """

        row = result.fetchone()
        result.close()
        return row

    def fetch_row(self, sql, binds = None):
        """Get a single row from the database

        Inputs:
         - sql <str>    : an SQL query
         - binds <dict> : a dictionary of bind parameters

        Output:
         - <object> : RowProxy object, the first row of 'result'
        """
        result = self.execute(sql, binds)
        return self.row(result)

    def scalar(self, result, col = 0):
        """Returns a single value from a result set

        Inputs:
         - result <object> : a ResultProxy object
         - col <str>       : key for the column to use for the return value

        Output:
         - <?> : a value from the result, type depends on the schema
        """
        row = self.row(result)
        if row is None:
            return None
        else:
            return row[col]

    def fetch_scalar(self, sql, binds = None, col = 0):
        """Fetches a single value from a sql query

        Inputs:
         - sql <str>    : an SQL statement
         - binds <dict> : bind parametrs to use for the sql
         - col <str>    : column to use for the result value

        Output:
         - <?> : a value from the result, type depends on the schema
        """
        result = self.execute(sql, binds)
        return self.scalar(result, col)

    def column(self, result, col = 0):
        """Returns a single column from a result set

        Inputs:
         - result <object> : a ResultProxy object
         - col <str>       : column to use for the result

        Output:
         - <list> : a list of values from the column
        """
        column = [ row[col] for row in result ]
        result.close()
        return column

    def fetch_column(self, sql, binds = None, col = 0):
        """Fetches an entire column from an sql query

        Inputs:
         - sql <str>    : an SQL statement
         - binds <dict> : bind parametrs to use for the sql
         - col <str>    : column to use for the result value

        Output:
         - <list> : a list of values from the column
        """
        result = self.execute(sql, binds)
        return self.column(result, col)

    def insert_returning(self, sql, binds = None):
        """For sql of the type 'INSERT ... RETURINING *', returns first (only) row of output

        Inputs:
         - sql <str>    : an SQL statement
         - binds <dict> : bind parametrs to use for the sql

        Output:
         - <object> : RowProxy object, the first row of 'result'
        """
        result = self.execute(sql, binds)
        return self.row(result)

    def insert_returning_id(self, sql, binds = None):
        """For sql of the type 'INSERT ... RETURINING id', returns first (only) scalar of output

        Inputs:
         - sql <str>    : an SQL statement
         - binds <dict> : bind parametrs to use for the sql

        Output:
         - <object> : RowProxy object, the first row of 'result'
        """
        result = self.execute(sql, binds)
        return self.scalar(result)
        
    def build_filter(self):
        """TODO: implement function to help building a WHERE clause +
        bind params
        """
        pass

    def now(self):
        """Same as datetime.datetime.now()"""
        return datetime.now()

    def dbftime(self, time):
        """Format a datetime to a string suitable for inserting into the DB

        Input:
         - time <object> : a datetime object

        Output:
         - <str> : a datetime string formatted like YYYY-MM-DD hh:mm:ss
        """
        return time.strftime('%Y-%m-%d %H:%M:%S')

    def dbnow(self):
        """current timestamp suitable for inserting into the dataabase

        Output:
         - <str> : a datetime string formatted like YYYY-MM-DD hh:mm:ss
        """
        return self.dbftime(self.now())

    def dbfuture(self, days = 0, seconds = 0, microseconds = 0,
                 reftime = None):
        """Return a future timestamp suitable for the database

        Useful for generating expiration times.

        Input:
         - days <int>         : number of days into the future
         - seconds <int>      : number of seconds into the future
         - microseconds <int> : number of microseconds into the future
         - reftime <object>   : a datetime object to use as the reference

        Output:
         - <str> : a datetime string formatted like YYYY-MM-DD hh:mm:ss
        """
        if reftime is None:
            reftime = self.now()
        elif isinstance(reftime, str):
            reftime = datetime.strptime(reftime, '%Y-%m-%d %H:%M:%S')
        future = reftime + timedelta(days, seconds, microseconds)
        return self.dbftime(future)

    def rowsize(self, row):
        """Return the size in bytes of a row from the database

        This is only a rough estimate at the moment.
        Treats int, long, or float as 4 or 8 bytes, depending on arch.
        Treats strings as arrays of bytes
        Recurses for list columns.
        Everything else is counted as one byte.
        """
        # Note: this would be easier with python 3.0 getsizeof() builtin
        intsize = (math.log(sys.maxsize, 2) + 1)/8 # are we 64 or 32 bit?

        # Define a private function so that we can recurse
        def list_size(thelist):
            size = 0
            for v in thelist:
                if (isinstance(v, int) or
                    isinstance(v, int) or
                    isinstance(v, float)):
                    # Numbers are either 4 or 8 bytes
                    size += intsize
                elif isinstance(v, str):
                    # Strings are byte arrays
                    size += len(v)
                elif isinstance(v, list):
                    # Recurse if we have  list
                    size += list_size(v)
                else:
                    # If we don't know what it is, it's one byte...
                    size += 1
            return size

        return list_size(list(row.values()))

    def build_filter(self, binds, colmap, default_op='=', clause_op='AND', where=True):
        clauses = []
        for b in binds:
            if b in colmap:
                clause = colmap[b] + default_op + ('%%(%s)s' % b)
                clauses.append(clause)
        filter = (' '+clause_op+' ').join(clauses)
        if filter and where:
            filter = ' WHERE ' + filter
        return filter

### Package functions

def db_argparser(parser = ArgumentParser(add_help=False), arguments=None):
    """Get a optparse.OptionParser object with DB connection options added

    The option syntax is similar to the 'psql' command.

    The following options are defined in the parser:
    """
    parser.add_argument("-?", "--help", action="help")
    parser.add_argument("-h", "--host",
                      dest="host", default="localhost",
                      help="Database hostname.  Default 'localhost'")
    parser.add_argument("-p", "--port", dest="port",
                      help="Database port")
    parser.add_argument("-U", "--username", dest="user",
                      help="Database username")
    parser.add_argument("-W", "--password", dest="prompt_password",
                      action='store_true', default=False,
                      help="Prompt for password")
    parser.add_argument("-d", "--dbname", dest="dbname",
                      help="Database name")
    parser.add_argument("-D", "--debug", dest="debug",
                      action='store_true', default=False,
                      help="Debug mode. Default False.")
    # Add aditional arguments/options desired by the user
    if arguments is not None:
        for arg in arguments:
            name_or_flags = []
            if 'name' in arg:
                name_or_flags = [arg.pop('name')]
            elif 'flag' in arg:
                name_or_flags = [arg.pop('flag')]
            elif 'flags' in arg:
                name_or_flags = arg.pop('flags')
            parser.add_argument(*name_or_flags, **arg)
    return parser

def db_kwargs(args):
    """Build a dict of DB connection kwargs suitable for Database()

    Intended to work along with db_optparser() results.
    """
    password = None
    if args.prompt_password:
        password = getpass.getpass()

    db_kwargs = {'host'     : args.host,
                 'port'     : args.port,
                 'database' : args.dbname,
                 'username' : args.user,
                 'password' : password,
                 'debug'    : args.debug}
    return db_kwargs
