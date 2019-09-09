import os
import logging
from importlib import import_module
import pandas as pd

from logbook import tools

logger = logging.getLogger(__name__)

def process(data_type, study, subject, read_dir, date_from,
        output_tz, input_tz, day_from, day_to, output_dir, phone_streams):
    # beiwe_ids
    beiwe_ids = tools.scan_dir(read_dir)
    beiwe_data_types = get_beiwe_types(read_dir, beiwe_ids)

    if phone_streams != []:
        beiwe_data_types = phone_streams

    # Aggregate data based on the bewie data type
    for bdt in beiwe_data_types:
        if bdt in ['identifiers', 'reachability']:
            continue
        df = pd.DataFrame.from_records([])

        mod = import_mod(bdt)
        if mod is None:
            continue

        for beiwe_id in beiwe_ids:
            beiwe_path = os.path.join(read_dir, beiwe_id, bdt)
            if not os.path.isdir(beiwe_path):
                continue

            logger.info('Processing %s' % bdt)
            data_list = mod.process(study, subject, beiwe_path,
                date_from, output_tz, input_tz)
            df = df.append(data_list, ignore_index=True, sort=False)

        if df is None or len(df) == 0:
            logger.warn('Data not found. Skipping data export and exiting.')
            continue

        # Export seconds bin
        seconds_df = get_seconds_df(df, date_from, mod)
        '''
        tools.clean_output_dir_seconds(study, subject, output_dir, data_type, bdt)
        tools.export_data_seconds(seconds_df, study, subject,
            output_dir, day_from, day_to, data_type, bdt)
        '''
        # Export daily bin
        daily_df = get_daily_df(seconds_df, date_from, bdt)
        tools.clean_output_dir_daily(study, subject, output_dir, data_type, bdt)
        tools.export_data_daily(daily_df, study, subject,
            output_dir, day_from, day_to, data_type, bdt)

def process_daily(df):
    df = df.groupby(['day', 'weekday', 'UTC_offset']).agg('sum').reset_index()
    return df.round(3)

# Process seconds data
def get_seconds_df(df, date_from, mod):
    seconds_data = tools.bin_df_seconds(df)
    seconds_data = tools.parse_date_to(seconds_data, date_from)
    seconds_data = seconds_data[pd.notnull(seconds_data['day'])]
    seconds_data = mod.process_seconds(seconds_data)
    seconds_data = tools.sort_seconds(seconds_data.reset_index())

    return seconds_data

# Process daily data
def get_daily_df(df, date_from, bdt):
    daily_data = process_daily(df) if bdt != 'identifiers' else df
    daily_data = daily_data.astype(str)
    daily_data['day'] = daily_data['day'].astype(int)
    daily_data = tools.sort_daily(daily_data.reset_index())

    return daily_data

# List beiwe data types
def get_beiwe_types(read_dir, beiwe_ids):
    beiwe_data_types = set()
    for i in beiwe_ids:
        if i[0] != '.':
            read_path = os.path.join(read_dir, i)
            types = os.listdir(read_path)
            for t in types:
                if t[0] != '.':
                    beiwe_data_types.add(t)

    return list(beiwe_data_types)

# Import module for the data type
def import_mod(data_type):
    try:
        return import_module('logbook.phone.{P}'.format(P=data_type), __name__)
    except Exception as e:
        logger.error(e)
        logger.error('Could not import the sub-module for %s' % data_type)
        return None
