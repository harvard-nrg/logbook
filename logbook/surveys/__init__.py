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
VALID_EXTENSIONS = ['.json']
DEFAULT_COLS = ['$date_to', 'category']

def process(data_type, study, subject, read_dir, date_from,
        output_tz, input_tz, day_from, day_to, output_dir):

    input_tz = FILE_TIMEZONE

    # Instantiate an empty dataframe
    df = pd.DataFrame.from_records([])

    for root_dir, dirs, files in os.walk(read_dir):
        files[:] = [ f for f in files if not f[0] == '.' ]
        dirs[:] = [ d for d in dirs if not d[0] == '.' ]

        for file_name in sorted(files):
            file_path = os.path.join(root_dir, file_name)
            data = parse(subject, file_name, file_path, input_tz, output_tz)
            df = df.append(data, ignore_index=True, sort=False)

    if df is None or len(df) == 0:
        logger.warn('Data not found. Skipping data export and exiting.')
        return

    # Divide into sub_categories
    for cat in df.category.unique():
        sub_df = df.loc[df['category'] == cat]

        # Export seconds bin
        seconds_df = get_seconds_df(sub_df, date_from)
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
    return df.groupby(['day', 'weekday', 'UTC_offset']).agg('mean').reset_index()

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
    df['frequency'] = df['frequency'].astype(int)
    df['hand'] = df['hand'].astype(int)

    seconds_data = tools.bin_df_seconds(df)
    seconds_data = tools.parse_date_to(seconds_data, date_from)
    seconds_data = sanitize_data(seconds_data)
    seconds_data = tools.sort_seconds(seconds_data.reset_index())

    return seconds_data

def parse(subject, file_name, file_path, input_tz, output_tz):
    if not file_name.endswith(tuple(VALID_EXTENSIONS)):
        logger.debug('%s has an unsupported file extension.' % file_name)
        return None

    try:
        parsed_file_name = file_name.split('.')
        assessment_name = parsed_file_name[1]

        data = pd.read_json(file_path)
        cols = data.columns.tolist()

        if 'date_watch' not in cols:
            logger.debug('This file does not have date information. Skipping.')
            return None
        elif 'watch_sampling_new' not in cols or 'watch_sampling_old' not in cols:
            logger.debug('This file does not have watch sampling rate information. Skipping.')
            return None
        elif 'time_watch_new' not in cols:
            logger.debug('This file does not have time information. Skipping.')
            return None
        elif 'hand_watch_new' not in cols:
            logger.debug('This file does not have watch handedness information. Skipping.')
            return None

        df = pd.DataFrame()
        df['$date_to'] = data.apply(lambda x: process_datetime(x['date_watch'],
            x['time_watch_new'],input_tz, output_tz), axis=1)
        df['category'] = 'watchSwap'
        df['frequency'] = data['watch_sampling_new'].apply(lambda x: process_frequency(x))
        df['hand'] = data['hand_watch_new'].apply(lambda x: process_hand(x))

        return df.dropna()
    except Exception as e:
        logger.error(e)
        return None

def process_hand(h):
    if h == "":
        return 0
    else:
        return int(h)

# Return frequency as int
def process_frequency(f):
    if f == "":
        return 0
    else:
        return int(f)

# Return timestamp in the timezone
def process_datetime(date_watch, time_watch, input_tz, output_tz):
    date_watch = str(date_watch)
    time_watch = str(time_watch)

    if len(date_watch) == 0:
        logger.error('No date provided')
        return None
    elif len(time_watch) == 0:
        logger.error('Time was not specified. Adding 00:00 manually.')
        time_watch = '00:00'

    row_timestamp = date_watch + '_' + time_watch

    temp = datetime.strptime(row_timestamp,
            '%Y-%m-%d_%H:%M').replace(tzinfo=tz.gettz(input_tz))
    return temp.astimezone(tz.gettz(output_tz))
