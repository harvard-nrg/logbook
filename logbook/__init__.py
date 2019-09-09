#!/usr/bin/env python

import sys
import os
import argparse as ap
import logging
import re
from dateutil import tz
from datetime import datetime
from importlib import import_module

from logbook import tools

logger = logging.getLogger(os.path.basename(__file__))

def parse_args():
    argparser = ap.ArgumentParser('LogBook Pipeline')

    # Input and Output Parameters
    argparser.add_argument('--read-dir',
        help='Path to the input directory',
        required=True)
    argparser.add_argument('--output-dir',
        help='Path to the output directory',
        required=True)

    argparser.add_argument('--study',
        help='Study name', required=True)
    argparser.add_argument('--subject',
        help='Subject ID', required=True)
    argparser.add_argument('--data-type',
        help='Data Type', required=True)
    argparser.add_argument('--phone-stream',
        help='Phone streams to process')

    argparser.add_argument('--date-from',
        help='Date info for the day 1 in the output. (Default: 1970-01-01; Format: YYYY-MM-DD)',
        default='1970-01-01')
    argparser.add_argument('--input-tz',
        help='Timezone info for the input. (Default: UTC)',
        default = 'UTC')
    argparser.add_argument('--output-tz',
        help='Timezone info for the output. (Default: America/New_York)',
        default = 'America/New_York')
    argparser.add_argument('--day-from',
        help='Output day from. Optional')
    argparser.add_argument('--day-to',
        help='Output day to. (optional; By default, process data for all days)')

    return argparser

def main(args):
    # Expand any ~/ in the directory paths
    read_dir = os.path.expanduser(args.read_dir)
    output_dir = os.path.expanduser(args.output_dir)

    # Perform sanity checks for inputs
    date_from = check_date(args.date_from, args.output_tz)
    if date_from is None: return

    read_dir = check_input(read_dir)
    output_dir = check_output(output_dir)
    if read_dir is None or output_dir is None: return

    day_from = int(args.day_from) if args.day_from != "None" else None
    day_to = int(args.day_to) if args.day_to != "None" else None

    # Process data
    data_type = args.data_type
    phone_stream = clean_phone_stream(args.phone_stream)

    mod = import_mod(data_type)
    if mod is None: return

    if data_type == 'phone':
        mod.process(data_type, args.study, args.subject, read_dir,
            date_from, args.output_tz, args.input_tz,
            day_from, day_to, args.output_dir, phone_stream)
    else:
        mod.process(data_type, args.study, args.subject, read_dir,
            date_from, args.output_tz, args.input_tz,
            day_from, day_to, args.output_dir)

# Parse stringified array to a list
def clean_phone_stream(phone_stream):
    if phone_stream == "None":
        return []

    phone_stream = phone_stream.split(",")
    if phone_stream[0].startswith('['):
        phone_stream[0] = phone_stream[0][1:]
    if phone_stream[-1].endswith(']'):
        phone_stream[-1] = phone_stream[-1][:-1]

    cleaned = [x.strip()[1:-1] for x in phone_stream]
    return cleaned

# import submodule
def import_mod(data_type):
    try:
        return import_module('logbook.{P}'.format(P=data_type), __name__)
    except Exception as e:
        logger.error(e)
        logger.error('Could not import the sub-module for %s' % data_type)
        return None

# Check if the input directory exists
def check_input(read_dir):
    if os.path.exists(read_dir):
        return read_dir
    else:
        logger.error('%s does not exist' % read_dir)
        return None

# Check if the output directory exists
def check_output(output_dir):
    if os.path.exists(output_dir):
        return output_dir
    else:
        logger.error('%s does not exist' % output_dir)
        return None

# Check if the input date-from is in correct format
def check_date(date_from, output_tz):
    if len(date_from) == 0 or date_from == '':
        logger.warn('Please check the subject consent date.')
        return None
    try:
        return datetime.strptime(date_from, '%Y-%m-%d').replace(tzinfo=tz.gettz(output_tz))
    except Exception as e:
        logger.error(e)
        logger.error('Error occurred while parsing the date-from parameter.')
        return None

if __name__ == '__main__':
    parser = parse_args()
    args = parser.parse_args()
    main(args)
