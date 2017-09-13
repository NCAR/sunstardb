# sunstardb - the solar-stellar database

sunstardb is a publicly accessible database of observations relevant
to the study of stellar magnetism. These observations form the basis
of the field known as the "solar-stellar connection," which seeks to
understand the mechanisms for the production of magnetic field in
stars and their dependence on fundamental properties such as mass,
luminosity, etc., as well as to understand the place of the Sun's
magnetism in this context. sunstardb aggregates and organizes results
published in the literature into one place in order to accelerate
research efforts in the solar-stellar connection.

sunstardb consists of the following components:

 * postgreSQL database
 * python APIs for interactive access
 * TODO: a web front-end for basic reporting of database contents

More information and announcements may be found at https://www2.hao.ucar.edu/sunstardb

An example can be seen in github by clicking the .ipynb files above,
or from [this
link](http://nbviewer.jupyter.org/github/NCAR/sunstardb/blob/master/sunstardb%20long%20example.ipynb).
(Note that the interactive tables do not work in these links, but do
work when running the jupyter notebook server locally by following the installation instructions below.)

## Requirements

sunstardb requires the following packages:

 * [psycopg2](http://initd.org/psycopg/)
 * [astropy](http://www.astropy.org)
 * [astroquery](https://astroquery.readthedocs.io)
 * [jupyter](http://jupyter.org)

## Installation

1. Install the requirements listed above and ensure they are in your python environment.
1. Clone or download sunstardb:
   * `git clone https://github.com/NCAR/sunstardb.git`

     OR
   * ```
     wget 'https://github.com/NCAR/sunstardb/archive/master.zip'
     unzip master.zip
     mv sunstardb-master sunstardb
     ```
1. `cd sunstardb`
1. `export PYTHONPATH=$PWD:$PYTHONPATH`
1. Fetch the configuration file using one of the following options:
   * Using `wget`:

   `wget --user-agent="Lynx/0 libwww-FM/0"  'https://goo.gl/h1WH2A' -O sunstardb.cfg`  
   * Using `curl`:

   `curl -L --user-agent "Lynx/0 libwww-FM/0" 'https://goo.gl/h1WH2A' > sunstardb.cfg`  
   * Or, if neither of the above utilities are installed, using your
   usual web browser.  Go to https://goo.gl/h1WH2A and save the output
   to a file named `sunstardb.cfg` in the top-level folder.
1. Run the example notebook:

   `jupyter notebook 'sunstardb long example.ipynb'`

   In the jupyter notebook menu, select "Cell -> Run All"
