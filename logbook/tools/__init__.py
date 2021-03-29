from __future__ import division
import os
import logging
from glob import glob
import pandas as pd
import numpy as np
from decimal import Decimal
from importlib import import_module

logger = logging.getLogger(os.path.basename(__file__))

DEFAULT_HEADERS = ['reftime', 'day', 'timeofday', 'weekday',
'study', 'subject', 'datatype', 'category', 'UTC_offset']

UNNECESSARY_HEADERS = ['$date_to', 'index', 'level_0']

# Get headers from the data
def get_headers(data):
     new_headers = data.columns.values
     default_headers = list(DEFAULT_HEADERS)

     if 'session_id' in new_headers:
         default_headers.append('session_id')

     for header in new_headers:
         if header not in default_headers:
             default_headers.append(header)

     return default_headers

def add_days(data):
    days = data['day'].unique()
    max_day = max(days)
    missing_days = [x for x in range(1, max_day + 1) if x not in days]
    missing_days_df = pd.DataFrame(missing_days, columns=['day'])
    data = data.append(missing_days_df, ignore_index=True).fillna('')
    return sort_daily(data)

# Add columns for study, subject, data_type, and category
def finalize_data(data, study, subject, data_type, category):
    data = add_days(data)

    data['study'] = study
    data['subject'] = subject
    data['datatype'] = data_type
    data['category'] = category

    for col in UNNECESSARY_HEADERS:
        if col in data.columns.values:
            data = data.drop([col], axis=1)
    return data

# Export the data as daily binned csv file
def export_data_daily(data, study, subject, output_dir, day_from, day_to, data_type, category):
    if len(data) == 0:
        logger.error('Data not found')
        return

    try:
        # Reformat datatype to fit bakerlab schema
        data_type = camel_case(data_type)
        category = camel_case(category)

        data = finalize_data(data, study, subject, data_type, category)

        default_headers = get_headers(data)

        day_from_data = data['day'].min()
        day_to_data = data['day'].max()

        # Target specific days
        day_from = day_from if day_from not in [None,'None'] else day_from_data
        day_to = day_to if day_to not in [None,'None'] else day_to_data

        file_name = get_filename_daily(study, subject, data_type, category, day_from, day_to)
        file_path = os.path.join(output_dir, file_name)

        logger.info('Writing %s' % file_path)
        query = '{DF} <= day <= {DT}'.format(DF=day_from, DT=day_to)

        print(file_path)

        data.query(query).to_csv(path_or_buf=file_path,
            index=False,
            columns=default_headers,
            na_rep='')

    except Exception as e:
        print(e)
        logger.error(e)

# Export the data as seconds binned csv file
def export_data_seconds(data, study, subject, output_dir, day_from, day_to, data_type, category):
    if len(data) == 0:
        logger.error('Data not found')
        return

    try:
        # Reformat datatype to fit bakerlab schema
        data_type = camel_case(data_type)
        category = camel_case(category)

        data = finalize_data(data, study, subject, data_type, category)

        default_headers = get_headers(data)

        day_from_data = data['day'].min()
        day_to_data = data['day'].max()

        # Target specific days
        day_from = day_from if day_from not in [None,'None'] else day_from_data
        day_to = day_to if day_to not in [None,'None'] else day_to_data

        file_name = get_filename_seconds(study, subject, data_type, category, day_from, day_to)
        file_path = os.path.join(output_dir, file_name)

        logger.info('Writing %s' % file_path)
        query = '{DF} <= day <= {DT}'.format(DF=day_from, DT=day_to)

        print(file_path)

        data.query(query).to_csv(path_or_buf=file_path,
            index=False,
            columns=default_headers,
            na_rep='')

    except Exception as e:
        print(e)
        logger.error(e)

# Export the data as csv file
def export_mri_data(data, study, subject, output_dir, day_from, day_to, data_type, category):
    if len(data) == 0:
        logger.error('Data not found')
        return

    try:
        # Reformat datatype to fit bakerlab schema
        data_type = camel_case(data_type)
        category = camel_case(category)

        data = finalize_data(data, study, subject, data_type, category)

        default_headers = get_headers(data)

        day_from_data = data['day'].min()
        day_to_data = data['day'].max()

        # Target specific days
        day_from = day_from if day_from not in [None,'None'] else day_from_data
        day_to = day_to if day_to not in [None,'None'] else day_to_data

        file_name = get_filename_daily(study, subject, data_type, category, day_from, day_to)
        file_path = os.path.join(output_dir, file_name)

        logger.info('Writing %s' % file_path)
        query = '{DF} <= day <= {DT}'.format(DF=day_from, DT=day_to)

        print(file_path)

        data.query(query).to_csv(path_or_buf=file_path,
            index=False,
            columns=default_headers,
            na_rep='')

    except Exception as e:
        print(e)
        logger.error(e)



# Export repetitive measures separately
def export_mri(data, study, subject, output_dir, day_from, day_to, data_type):
    series_df = data[[
        'day',
        'weekday',
        'timeofday',
        'XNAT_sessionID',
        'seriesDesc',
        'seriesType',
        'seriesNum',
        'tr',
        'te',
        'sliceThickness',
        'flipAngle',
        'frameNum',
        'seriesDurationSec'
    ]].copy()

    repeated_df = data[[
        'day',
        'weekday',
        'timeofday',
        'study',
        'subject',
        'XNAT_sessionID',
        'software',
        'weight',
        'age',
        'manufacturer',
        'manufacturerModel',
        'device',
        'fieldStrength'
    ]].copy()
    repeated_df.drop_duplicates(inplace=True)

    bold_df = data.loc[data['seriesDesc'].str.startswith('SMS')]
    bold_params = bold_df[[
        'day',
        'weekday',
        'timeofday',
        'XNAT_sessionID',
        'seriesNum',
        'seriesType',
        'seriesDesc',
        'frameNum',
        'seriesDurationSec'
    ]].copy()

    export_mri_data(series_df, study, subject, output_dir, day_from, day_to, 'mri', 'series_info')
    export_mri_data(repeated_df, study, subject, output_dir, day_from, day_to, 'mri', 'DeviceInfo_Demographics')
    export_mri_data(bold_params, study, subject, output_dir, day_from, day_to, 'mri', 'bold_params')

# Save the missingness data
def save_missingness(df_missed, study, subject, output_dir, frequencies, assessment):
    try:
        df_missed = pd.DataFrame([df_missed], columns=df_missed.keys())

        file_name = '{ST}-{SB}-{DATA}_logbook{EXT}'.format(ST=study,
            SB=subject,
            DATA=assessment.title(),
            EXT='.csv')

        file_path = os.path.join(output_dir, file_name)
        logger.info('Writing %s' % file_path)

        df_missed.to_csv(path_or_buf=file_path,
            columns=frequencies,
            na_rep='', index=False)
    except Exception as e:
        logger.error(e)


def camel_case(data_type):
    parse = data_type.find('_')
    if parse == -1:
        return data_type
    else:
        data_type = data_type.title()
        data_types = data_type.split('_')
        data_type = "".join(data_types)
        return data_type[0].lower() + data_type[1:]


def get_mri_filename(study, subject, data_type, day_from, day_to):
    return '{ST}-{SB}-mri_{DATA}_logbook_daily-day{DF}to{DT}{EXT}'.format(ST=study,
        SB=subject,
        DATA=data_type,
        DF=day_from,
        DT=day_to,
        EXT='.csv')

def get_filename_seconds(study, subject, data_type, category, day_from, day_to):
    return '{ST}-{SB}-{DATA}_{CATE}_logbook_seconds-day{DF}to{DT}{EXT}'.format(ST=study,
        SB=subject,
        DATA=data_type,
        CATE=category,
        DF=day_from,
        DT=day_to,
        EXT='.csv')

def get_filename_daily(study, subject, data_type, category, day_from, day_to):
    return '{ST}-{SB}-{DATA}_{CATE}_logbook_daily-day{DF}to{DT}{EXT}'.format(ST=study,
        SB=subject,
        DATA=data_type,
        CATE=category,
        DF=day_from,
        DT=day_to,
        EXT='.csv')

# Clean the output directory
def clean_output_dir_daily(study, subject, output_dir, data_type, category):
    file_pattern = '{STUDY}-{SUBJECT}-{DATA}_{CATE}_logbook_daily-day*'.format(STUDY=study,
        SUBJECT=subject,
        DATA=data_type,
        CATE=category)
    file_path = os.path.join(output_dir, file_pattern)
    for match in glob(file_path):
        logger.warn('Removing file %s' % match)
        os.remove(match)

# Clean the output directory
def clean_output_dir_seconds(study, subject, output_dir, data_type, category):
    file_pattern = '{STUDY}-{SUBJECT}-{DATA}_{CATE}_logbook_seconds-day*'.format(STUDY=study,
        SUBJECT=subject,
        DATA=data_type,
        CATE=category)
    file_path = os.path.join(output_dir, file_pattern)
    for match in glob(file_path):
        logger.warn('Removing file %s' % match)
        os.remove(match)

# Sort the data
def sort_data(df):
    return df.sort_values(['day', 'timeofday'])

# Sort data daily
def sort_daily(df):
    return df.sort_values(['day'])

# Sort data by time of day
def sort_seconds(df):
    return df.sort_values(['day', 'timeofday'])

# Check if a directory is valid, then return its child directories
def scan_dir(path):
    if os.path.isdir(path):
        try:
            return os.listdir(path)
        except Exception as e:
            logger.error(e)
            return []
    else:
        return []

# Count the number of days between two dates
def process_date(row_date, date_from):
    date_from = date_from.date()
    date_to = row_date.date()

    # The consent date should count as 1 not 0
    day = (date_to - date_from).days + 1
    return day

# Return NCF style weekday..
def process_weekday(row_date):
    weekday = row_date.isoweekday()

    return weekday

# Process UTC offset values
def process_utcoffset(row_date):
    utc_offset = str(row_date.strftime('%z')) if not pd.isnull(row_date) else ''
    return utc_offset

# Get day, weekday, and timeofday based on the row index
def parse_date_to(df, date_from):
    df['day'] = df.index.map(lambda x: process_date(x, date_from))
    df['weekday'] = df.index.map(process_weekday)
    df['timeofday'] = df.index.map(process_time)
    df['UTC_offset'] = df.index.map(process_utcoffset)

    return df[pd.notnull(df['day'])]

# Process time in NCF format
def process_time(row_date):
    hour = str(row_date.hour) if row_date.hour > 9 else '0' + str(row_date.hour)
    minute = str(row_date.minute) if row_date.minute > 9 else '0' + str(row_date.minute)
    second = str(row_date.second) if row_date.second > 9 else '0' + str(row_date.second)

    return hour + ':' + minute + ':' + second

# Bin data at each frequency
def bin_data(df, frequencies):
    for f in frequencies:

        tool = import_tool(f)
        if tool == None:
            yield None, None, None
            continue

        data = tool.bin_df(df)
        resampled = tool.resample_df(data)
        missed = missing_percent(resampled)

        yield data, missed, f

# Bin data at seconds
def bin_df_seconds(df):
    tool = import_tool('seconds')
    data = tool.bin_df(df)
    return df

# Import tool module for the frequency
def import_tool(frequency):
    try:
        return import_module('logbook.tools.timeseries.{P}'.format(P=frequency), __name__)
    except Exception as e:
        logger.error(e)
        logger.error('Could not import the tool module for %s' % frequency)
        return None

# Calculate missingness
def missing_percent(df):
    counts = np.count_nonzero(df['$date_to'])
    length = len(df)
    missing = 1 - (counts / length)
    pct = missing * 100
    pct = pct * 1000 / 1000

    return "%.3f" % pct
