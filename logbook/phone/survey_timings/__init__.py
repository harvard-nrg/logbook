import os
import re
import gzip
import pandas as pd
import logging
from datetime import datetime
from dateutil import tz

logger = logging.getLogger(__name__)

FILE_REGEX = re.compile(r'(?P<year>[0-9]{4})-(?P<month>[0-9]{2})-(?P<day>[0-9]{2})\s(?P<hour>[0-9]{2})_(?P<minute>[0-9]{2})_(?P<second>[0-9]{2})(?P<extension>\..*)')
CHUNKSIZE = 10 ** 6

def process(study, subject, read_dir, date_from, output_tz, input_tz):
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

    return df

def process_seconds(df):
    df.index.name = None

    if 'event' in df.columns.tolist():
        dfe = df.groupby(['day', 'weekday','timeofday','UTC_offset', 'event'])
        df = dfe.size().reset_index(name='counts')
        df = df.pivot_table(index=['day','weekday','timeofday','UTC_offset'],
                columns='event', values='counts').fillna(0)
        df = df.reset_index()
    else:
        dfq = df.groupby(['day', 'weekday','timeofday','UTC_offset', 'question id'])
        df = dfq.size().reset_index(name='counts')
        df = df.pivot_table(index=['day','weekday','timeofday','UTC_offset'],
                columns='question id', values='counts').fillna(0)

        df = df[['Survey first rendered and displayed to user', 'User hit submit']]
        df = df.rename(columns={
            "Survey first rendered and displayed to user": "notified",
            "User hit submit": "submitted"
        })
        df = df.reset_index()

    # Format numbers for the visual
    df_columns = df.columns.tolist()
    df_columns.remove('timeofday')
    df[df_columns] = df[df_columns].fillna(0).astype(int)
    df['day'] = df['day'].astype(int)

    df.columns.name = None

    return df

# Verify the file based on its filename
def verify(file_name):
    match = FILE_REGEX.match(file_name)
    if match and match.group('extension') in ['.csv']:
        return file_name, match.group('extension')
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
            skipinitialspace=True, error_bad_lines=False, chunksize=CHUNKSIZE,index_col=False):
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
    timestamp = pd.Timestamp(row_timestamp, tz = input_tz)
    return timestamp.tz_convert(output_tz)

# Parse and process data
def parse(df, date_from, output_tz, input_tz, file_path, filename):
    if df is None or len(df) == 0:
        logger.error('Could not open file %s' % file_path)
        return

    df['$date_to'] = df['UTC time'].apply(lambda x: process_datetime(x, input_tz, output_tz))
    return df
