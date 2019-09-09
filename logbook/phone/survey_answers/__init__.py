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
                data_list = parse(date_from, output_tz,
                    input_tz, file_path, file_name)
                df = df.append(data_list, ignore_index=True, sort=False)

    return df

def process_seconds(df):
    df.index.name = None

    dfe = df.groupby(['day', 'weekday','timeofday','UTC_offset'])
    df = dfe.size().reset_index(name='surveys')

    # Format numbers for the visual
    df['surveys'] = df['surveys'].astype(int)
    df['weekday'] = df['weekday'].astype(int)
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

# Return timestamp in the timezone
def process_datetime(row_timestamp, input_tz, output_tz):
    match = FILE_REGEX.match(row_timestamp).groupdict()
    timestamp = pd.Timestamp(year = int(match['year']),
            month = int(match['month']),
            day = int(match['day']),
            hour = int(match['hour']),
            minute = int(match['minute']),
            second = int(match['second']),
            nanosecond = 0,
            tz = input_tz)
    return timestamp.tz_convert(output_tz)

# Parse and process data
def parse(date_from, output_tz, input_tz, file_path, filename):
    df = {}
    df['$date_to'] = process_datetime(filename, input_tz, output_tz)
    df['counts'] = 1
    return df
