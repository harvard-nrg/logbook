Quick start
===========
Logbook summarizes multiple data types throughout the PHOENIX filesystem.

Installation
------------
Just use ``pip`` ::

    pip install logbook-dptk

pipenv
~~~~~~
``pipenv`` is a great tool for bootstrapping a Python 
`virtualenv <https://docs.python-guide.org/dev/virtualenvs/>`_ and installing a package 
using the new ``Pipfile`` specification ::

    pip install --user pipenv
    export PATH=~/.local/bin:$PATH
    git clone https://github.com/harvard-nrg/logbook.git
    cd logbook
    pipenv install
    pipenv shell
    lb.py --help

Building
--------
To build Logbook into a Python package, head over to the 
`build documentation <building.html>`_.

Basic usage
-----------
To run Logbook across all data types for all studies and subjects ::

    lb.py \
      --phoenix-dir /PHOENIX \
      --consent-dir /PHOENIX/GENERAL \
      --log-dir /path/to/logs

specific data types
~~~~~~~~~~~~~~~~~~~
To run for one or more specific data types, use ``--data-type`` argument ::

    lb.py \
    --phoenix-dir /PHOENIX 
    --consent-dir /PHOENIX/GENERAL
    --log-dir /path/to/logs/logbook 
    --data-type {data type} {data type}

specific phone streams
~~~~~~~~~~~~~~~~~~~~~~
To run Logbook on the ``phone`` data type and derive only ``accelerometer`` 
and ``survey_answers`` results use the ``--phone-stream`` argument::

    lb.py \
    --phoenix-dir /PHOENIX \
    --consent-dir /PHOENIX/GENERAL \
    --log-dir /path/to/logs/logbook \
    --data-type phone \
    --phone-stream accelerometer survey_answers

specific day range
~~~~~~~~~~~~~~~~~~
To process all data types and get data from day 5 to 32 use the 
``--day-from`` and ``--day-to`` arguments ::

    lb.py --phoenix-dir /PHOENIX \
    --consent-dir /PHOENIX/GENERAL \
    --log-dir /path/to/logs \
    --day-from 5 \
    --day-to 32

specific studies
~~~~~~~~~~~~~~~~
To process all data types for specific PHOENIX studies, use the 
``--study`` argument ::

    lb.py \
    --phoenix-dir /PHOENIX \
    --consent-dir /PHOENIX/GENERAL \
    --log-dir /path/to/logs 
    --study A B

specific subject
~~~~~~~~~~~~~~~~
To process all data types for subject 123 in study A and B ::

    lb.py \
    --phoenix-dir /PHOENIX \
    --consent-dir /PHOENIX/GENERA
    --log-dir /path/to/logs \
    --study A B \
    --subject 123

output UTC
~~~~~~~~~~
To process all data types for all studies and subjects and output
in UTC time ::

    lb.py \
    --phoenix-dir /PHOENIX \
    --consent-dir /PHOENIX/GENERAL \
    --log-dir /path/to/logs \
    --output-tz UTC

debugging messages
~~~~~~~~~~~~~~~~~~
To process all data types and print debug messages, use the ``--debug`` 
argument ::

    lb.py \
    --phoenix-dir /PHOENIX \
    --consent-dir /PHOENIX/GENERAL \
    --log-dir /path/to/logs \
    --debug

