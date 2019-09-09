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
DEFAULT_COLS = ['$date_to', 'category']

def process(data_type, study, subject, read_dir, date_from,
        output_tz, input_tz, day_from, day_to, output_dir):

    input_tz = FILE_TIMEZONE

    # Instantiate an empty dataframe
    df = pd.DataFrame.from_records([])

    print('read_dir=%s, subject=%s' % (read_dir, subject))
    paths, dates = get_session_paths(read_dir, subject, output_tz, input_tz)

    for p, d in list(zip(paths, dates)):
        data = parse(p, d)
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
        cols = ['nFiles']
        cols.extend(DEFAULT_COLS)
        sub_df = sub_df[cols]

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

# Transpose to categorize the data
def expand_df(df):
    expanded = []

    for key, value in df.items():
        elem = {}
        elem['category'] = str(key)

        elem_key = 'nFiles'
        elem[elem_key] = value

        expanded.append(elem)

    return pd.DataFrame.from_records(expanded)

# Create daily df
def parse(p, d):
    files = [f for f in os.listdir(p) if os.path.isfile(os.path.join(p, f))]

    df = get_extensions(files)
    df = expand_df(df)
    df['$date_to'] = d

    return df

# Parse filename to get the extension and count the number of files
def get_extensions(files):
    extensions = []

    for f in files:
        file_name = f.split('.')
        if file_name[-1] == 'lock' and len(file_name) > 2:
            extension = file_name[-2].lower()
            extensions.append(extension)

    return Counter(extensions)

# Get the session paths and dates
def get_session_paths(read_dir, subject, output_tz, input_tz):
    session_paths = []
    session_dates = []

    sessions = os.listdir(read_dir)

    for session in sessions:
        if session in [ subject, 'MGH Clinical Session Videos' ]:
            continue
            '''
            sub_session_dir = os.path.join(read_dir, session)
            sub_paths, sub_dates = get_session_paths(sub_session_dir, subject, output_tz, input_tz)
            session_paths.extend(sub_paths)
            session_dates.extend(sub_dates)
            '''
        session_path = os.path.join(read_dir, session)
        if not os.path.isdir(session_path):
            logger.debug('%s is not a directory' % session_path)
            continue

        session_date = get_session_date(session, subject, input_tz, output_tz)
        if session_date is not None:
            session_paths.append(session_path)
            session_dates.append(session_date)

    return session_paths, session_dates

# Get and parse session date
def get_session_date(session, subject, input_tz, output_tz):
    parsed_session_id = session.split('_')
    parsed_len = len(parsed_session_id)

    if parsed_len >= 2:
        if parsed_session_id[0] == subject:
            session_date = parsed_session_id[1]
            return process_datetime(session_date, input_tz, output_tz)
        elif parsed_session_id[-1].startswith('MS'):
            session_date = parsed_session_id[0]
            return process_datetime(session_date, input_tz, output_tz)
        elif session.endswith('_clinical_session'):
            session_date = parsed_session_id[1]
            return process_datetime_clinical(session_date, input_tz, output_tz)

    logger.debug('%s is not a valid session folder' % session)
    return None

# Return timestamp in the timezone
def process_datetime(row_timestamp, input_tz, output_tz):
    temp = datetime.strptime(row_timestamp,
        '%y%m%d').replace(tzinfo=tz.gettz(input_tz))
    return temp.astimezone(tz.gettz(output_tz))

def process_datetime_clinical(row_timestamp, input_tz, output_tz):
    row_timestamp = row_timestamp.title().strip()
    temp = datetime.strptime(row_timestamp,
        '%b%d%Y').replace(tzinfo=tz.gettz(input_tz))
    return temp.astimezone(tz.gettz(output_tz))
