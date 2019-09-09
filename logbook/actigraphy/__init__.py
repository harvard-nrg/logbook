import os
import re
import gzip
import pandas as pd
import logging
from datetime import datetime
from dateutil import tz

from logbook import tools

logger = logging.getLogger(__name__)

FILE_TIMEZONE = 'America/New_York'
FILE_REGEX_1 = re.compile(r'(?P<subject>\w+)_(?P<hand>\w+)\s(?P<position>\w+)_(?P<serialnum>\w+)_(?P<year>[0-9]{4})-(?P<month>[0-9]{2})-(?P<day>[0-9]{2})\s(?P<hour>[0-9]{2})-(?P<minute>[0-9]{2})-(?P<second>[0-9]{2})(?P<extension>\..*)')
FILE_REGEX_2 = re.compile(r'(?P<subject>\w+)__(?P<serialnum>\w+)_(?P<year>[0-9]{4})-(?P<month>[0-9]{2})-(?P<day>[0-9]{2})\s(?P<hour>[0-9]{2})-(?P<minute>[0-9]{2})-(?P<second>[0-9]{2})(?P<extension>\..*)')
SKIP_TO_DATA_ROW_NUM = 100 #Num of lines to skip to get to the data
FILE_HEADERS = ['timestamp', 'x', 'y', 'z', 'lux', 'button', 'temp']
CHUNKSIZE = 10 ** 6

def process(data_type, study, subject, read_dir, date_from,
        output_tz, input_tz, day_from, day_to, output_dir):

    input_tz = FILE_TIMEZONE

    # Instantiate an empty dataframe
    df = pd.DataFrame.from_records([])

    for root_dir, dirs, files in os.walk(read_dir):
        files[:] = [ f for f in files if not f[0] == '.' ]
        dirs[:] = [ d for d in dirs if not d[0] == '.' ]

        for file_name in sorted(files):
            file_name, extension = verify(file_name)
            if file_name is not None:
                file_path = os.path.join(root_dir, file_name)
                for data in get_data(file_path, extension):
                    data_list = parse(data, date_from, output_tz,
                            input_tz, file_path, file_name)
                    df = df.append(data_list, ignore_index=True, sort=False)

    if df is None or len(df) == 0:
        logger.warn('Data not found. Skipping data export and exiting.')
        return

    # Export seconds bin
    seconds_df = get_seconds_df(df, date_from)
    '''
    tools.clean_output_dir_seconds(study, subject, output_dir, data_type, 'GENEActiv')
    tools.export_data_seconds(seconds_df, study, subject,
        output_dir, day_from, day_to, data_type, 'GENEActiv')
    '''
    # Export daily bin
    daily_df = get_daily_df(seconds_df, date_from)
    tools.clean_output_dir_daily(study, subject, output_dir, data_type, 'GENEActiv')
    tools.export_data_daily(daily_df, study, subject,
        output_dir, day_from, day_to, data_type, 'GENEActiv')


def process_daily(df):
    return df.groupby(['day', 'weekday', 'UTC_offset']).agg('sum').reset_index()

def process_seconds(df):
    df.index.name = None

    dfe = df.groupby(['day', 'weekday', 'timeofday', 'UTC_offset'])
    df = dfe.size().reset_index(name='data_points')

    # Format numbers for the visual
    df['data_points'] = df['data_points'].astype(int)
    df['weekday'] = df['weekday'].astype(int)
    df['day'] = df['day'].astype(int)

    df.columns.name = None

    return df

# Process seconds data
def get_seconds_df(df, date_from):
    seconds_data = tools.bin_df_seconds(df)
    seconds_data = tools.parse_date_to(seconds_data, date_from)
    seconds_data = process_seconds(seconds_data)
    seconds_data = tools.sort_seconds(seconds_data.reset_index())

    return seconds_data

# Process daily data
def get_daily_df(df, date_from):
    daily_data = process_daily(df)
    daily_data = tools.sort_daily(daily_data.reset_index())

    return daily_data

# Verify the file based on its filename
def verify(file_name):
    match_1 = FILE_REGEX_1.match(file_name)
    match_2 = FILE_REGEX_2.match(file_name)
    if match_1 and match_1.group('extension') in ['.csv.gz', '.csv']:
        return file_name, match_1.group('extension')
    elif match_2 and match_2.group('extension') in ['.csv.gz', '.csv']:
        return file_name, match_2.group('extension')
    else:
        return None, None

# Unzip the file and parse as csv
def gz_to_df(file_path):
    try:
        with gzip.open(file_path, 'rb') as f:
            if hasattr(f, 'read'):
                return csv_to_df(f)
            else:
                return None
    except Exception as e:
        logger.error(e)
        return

# Convert a comma-separated-string object to pandas csv dataframe
def csv_to_df(file_path):
    try:
        for chunk in pd.read_csv(file_path, keep_default_na=False, engine='c',
                skiprows= SKIP_TO_DATA_ROW_NUM, skipinitialspace=True,
                error_bad_lines=False, names=FILE_HEADERS, chunksize=CHUNKSIZE):
            yield chunk
    except Exception as e:
        logger.error(e)
        return

# Check the file extension, and act accordingly
def get_data(file_path, extension):
    if extension == '.csv':
        return csv_to_df(file_path)
    elif extension == '.csv.gz':
        return gz_to_df(file_path)
    else:
        logger.error('Incompatible file extension %s' % extension)
        return

# Return timestamp in the timezone
def process_datetime(row_timestamp, input_tz, output_tz):
    temp = datetime.strptime(row_timestamp,
        '%Y-%m-%d %H:%M:%S:%f').replace(tzinfo=tz.gettz(input_tz))
    return temp.astimezone(tz.gettz(output_tz))

# Parse and process data
def parse(df, date_from, output_tz, input_tz, file_path, file_name):
    if df is None or len(df) == 0:
        logger.error('Could not open file %s' % file_path)
        return
    df['$date_to'] = df['timestamp'].apply(lambda x: process_datetime(x,
        input_tz, output_tz))
    return df
