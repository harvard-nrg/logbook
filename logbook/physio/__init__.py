import os
import re
import gzip
import pandas as pd
import logging
from datetime import datetime
from dateutil import tz
from collections import Counter

from logbook import tools

logger = logging.getLogger(__name__)


FILE_TIMEZONE = 'America/New_York'
VALID_EXTENSIONS = ['.acq']
DEFAULT_COLS = ['$date_to', 'session_id', 'category']

def process(data_type, study, subject, read_dir, date_from,
        output_tz, input_tz, day_from, day_to, output_dir):

    ##################################
    # data_type specific adjustments #
    # Could be changed in the future #
    ##################################

    input_tz = FILE_TIMEZONE

    # Instantiate an empty dataframe
    df = pd.DataFrame.from_records([])

    for root_dir, dirs, files in os.walk(read_dir):
        files[:] = [ f for f in files if not f[0] == '.' ]
        dirs[:] = [ d for d in dirs if not d[0] == '.' ]

        for file_name in sorted(files):
            session_date, session_id, category = parse(file_name, subject, input_tz, output_tz)
            if category is None:
                continue

            data = get_data(session_date, session_id, category)
            df = df.append(data, ignore_index=True, sort=False)

    if df is None or len(df) == 0:
        logger.warn('Data not found. Skipping data export and exiting.')
        return

    # get headers to style digit #
    headers = df.columns.tolist()
    for col in DEFAULT_COLS:
        headers.remove(col)

    # Style df
    df = df.fillna(0)
    df[headers] = df[headers].astype(int).astype(str)

    # Divide into sub_categories
    for cat in df.category.unique():
        sub_df = df.loc[df['category'] == cat]

        # Export seconds bin
        seconds_df = get_seconds_df(sub_df, date_from)

        ## Drop timeofday. The actual timeofday will be acquired from mri
        seconds_df = seconds_df.drop(columns=['timeofday'])
        '''
        tools.clean_output_dir_seconds(study, subject, output_dir, data_type, cat)
        tools.export_data_seconds(seconds_df, study, subject,
            output_dir, day_from, day_to, data_type, cat)
        '''
        # Export daily bin
        daily_df = get_daily_df(seconds_df, date_from)
        tools.clean_output_dir_daily(study, subject, output_dir, data_type, cat)
        tools.export_data_daily(daily_df, study, subject,
            output_dir, day_from, day_to, data_type, cat)


def get_data(session_date, session_id, category):
    df = {}
    df['$date_to'] = session_date
    df['session_id'] = session_id
    df['category'] = category
    df['nFiles'] = 1

    return df

def process_daily(df):
    return df.groupby(['day', 'weekday', 'UTC_offset']).agg('sum').reset_index()

def sanitize_data(df):
    if '$date_to' in df.columns.tolist():
        df = df.drop(columns=['$date_to'])

    return df

# Process daily data
def get_daily_df(df, date_from):
    daily_data = process_daily(df)
    daily_data = sanitize_data(daily_data)
    daily_data = tools.sort_daily(daily_data.reset_index())

    return daily_data

# Process seconds data
def get_seconds_df(df, date_from):
    seconds_data = tools.bin_df_seconds(df)
    seconds_data = tools.parse_date_to(seconds_data, date_from)
    seconds_data = sanitize_data(seconds_data)
    seconds_data = tools.sort_seconds(seconds_data.reset_index())

    return seconds_data

def parse(file_name, subject, input_tz, output_tz):
    if not file_name.endswith(tuple(VALID_EXTENSIONS)):
        logger.debug('%s has an unsupported file extension.' % file_name)
        return None, None, None

    parsed_file_name = file_name.split('_')
    if len(parsed_file_name) >= 3 and file_name.endswith('acq'):
        session_date = process_datetime(parsed_file_name[1], input_tz, output_tz)
        session_id = parsed_file_name[2]

        if session_id.endswith('acq'):
            session_id = session_id[:-4]
        category = 'acq'
    else:
        logger.debug('%s is not supported' % file_name)
        return None, None, None

    return session_date, session_id, category

# Return timestamp in the timezone
def process_datetime(row_timestamp, input_tz, output_tz):
    temp = datetime.strptime(row_timestamp,
        '%y%m%d').replace(tzinfo=tz.gettz(input_tz))
    return temp.astimezone(tz.gettz(output_tz))
