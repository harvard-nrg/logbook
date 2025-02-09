import os
import re
import gzip
import pandas as pd
import numpy as np
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
    dfg = df.groupby(['day', 'weekday','timeofday','UTC_offset', 'sent vs received'])

    # Get counts
    df_1 = dfg.size().reset_index(name='counts')
    df_1 = df_1.pivot_table(index=['day','weekday','timeofday','UTC_offset'], columns='sent vs received', values='counts').fillna(0)
    df_1 = df_1.rename(columns={
        "received SMS": "received_sms_counts",
        "sent SMS": "sent_sms_counts",
        "received MMS": "received_mms_counts",
        "sent MMS": "sent_mms_counts"
    })
    df_1 = df_1.reset_index()
    df_sent_total = df_1.filter(regex='sent').sum(axis=1).reset_index(name='counts')
    df_1['sent_total_counts'] = df_sent_total['counts']
    df_received_total = df_1.filter(regex='received').sum(axis=1).reset_index(name='counts')
    df_1['received_total_counts'] = df_received_total['counts']

    # Format numbers for the visual
    df_1_columns = df_1.columns.tolist()
    df_1_columns.remove('timeofday')
    df_1[df_1_columns] = df_1[df_1_columns].fillna(0).astype(int).astype(str)
    df_1['day'] = df_1['day'].astype(int)

    # Get unique numbers
    df_3 = dfg['hashed phone number'].nunique().reset_index(name='unique')
    df_unique = df_3.pivot_table(index=['day','weekday','timeofday','UTC_offset'],
            columns='sent vs received', values='unique').fillna(0)
    df_received_unique = df_unique.filter(regex='received').sum(axis=1).reset_index(name='counts')
    df_sent_unique = df_unique.filter(regex='received').sum(axis=1).reset_index(name='counts')
    df_1['received_total_unique_numbers'] = df_received_unique['counts'].astype(int).astype(str)
    df_1['sent_total_unique_numbers'] = df_sent_unique['counts'].astype(int).astype(str)

    # Get message length
    dfg_2 = df[df['message length'].apply(lambda x: type(x) in [int, float, np.int64, np.float64])]
    dfg_2['message length'] = dfg_2['message length'].astype(float)
    dfg_2 = dfg_2.groupby(['day', 'weekday','timeofday','UTC_offset', 'sent vs received'])
    df_2 = dfg_2['message length'].agg(['min','max','sum','mean']).reset_index()

    df_min = df_2.pivot_table(index=['day','weekday','timeofday','UTC_offset'],
            columns='sent vs received', values='min')
    df_1['received_total_message_length_min'] = df_min.filter(regex='received').sum(axis=1).reset_index(name='counts')['counts'].fillna(0)
    df_1['sent_total_message_length_min'] = df_min.filter(regex='sent').sum(axis=1).reset_index(name='counts')['counts'].fillna(0)

    df_max = df_2.pivot_table(index=['day','weekday','timeofday','UTC_offset'],columns='sent vs received', values='max')
    df_1['received_total_message_length_max'] = df_max.filter(regex='received').sum(axis=1).reset_index(name='counts')['counts'].fillna(0)
    df_1['sent_total_message_length_max'] = df_max.filter(regex='sent').sum(axis=1).reset_index(name='counts')['counts'].fillna(0)

    df_sum = df_2.pivot_table(index=['day','weekday','timeofday','UTC_offset'],columns='sent vs received', values='sum')
    df_1['received_total_message_length_sum'] = df_sum.filter(regex='received').sum(axis=1).reset_index(name='counts')['counts'].fillna(0)
    df_1['sent_total_message_length_sum'] = df_sum.filter(regex='sent').sum(axis=1).reset_index(name='counts')['counts'].fillna(0)

    df_mean = df_2.pivot_table(index=['day','weekday','timeofday','UTC_offset'],columns='sent vs received', values='mean')
    df_1['received_total_message_length_mean'] = df_mean.filter(regex='received').mean(axis=1).reset_index(name='counts')['counts'].fillna(0)
    df_1['sent_total_message_length_mean'] = df_mean.filter(regex='sent').mean(axis=1).reset_index(name='counts')['counts'].fillna(0)

    df.columns.name = None

    return df_1.round(3)

# Verify the file based on its filename
def verify(file_name):
    match = FILE_REGEX.match(file_name)
    if match:
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
