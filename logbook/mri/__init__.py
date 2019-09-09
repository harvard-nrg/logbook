from __future__ import division
import os
import logging
from datetime import datetime
from dateutil import tz
import pandas as pd
import math
import pydicom as dicom

from logbook import tools

logger = logging.getLogger(__name__)

def process(data_type, study, subject, read_dir, date_from, output_tz, input_tz,
        day_from, day_to, output_dir):
    # Get all mri ids from the raw directory
    mri_ids = tools.scan_dir(read_dir)

    # Instantiate an empty dataframe
    df = pd.DataFrame.from_records([])

    for mri_id in sorted(mri_ids):
        # Instantiate an empty dataframe
        data = pd.DataFrame.from_records([])
        processed_series = set()

        logger.debug('Processing %s' % mri_id)
        temp = os.path.join(read_dir, mri_id)

        for root_dir, dirs, files in os.walk(temp):
            files[:] = [ f for f in files if not f[0] == '.' ]
            dirs[:] = [ d for d in dirs if not d[0] == '.' ]

            for file_name in sorted(files):
                if file_name.endswith('.dcm'):
                    file_path = os.path.join(root_dir, file_name)
                    dcm = get_data(file_path)
                    if dcm is not None and 'SeriesNumber' in dcm:
                        if dcm.SeriesDescription.startswith('SMS') or dcm.SeriesDescription.startswith('ASL') or (dcm.SeriesDescription.startswith('T1') and dcm.SeriesDescription.endswith('RMS')):
                            if not dcm.SeriesDescription.endswith('SBRef'):
                                if dcm.SeriesNumber in processed_series:
                                    data = add_slice_and_minutes(data, dcm.SeriesNumber,
                                            dcm.InstanceNumber, dcm.RepetitionTime)
                                    continue

                                processed_series.add(dcm.SeriesNumber)
                                data_list = parse(dcm, date_from, output_tz,
                                    input_tz, file_path, file_name, study, subject)
                                data = data.append(data_list, ignore_index=True, sort=False)
        df = df.append(data, ignore_index=True, sort=False)

    if df is None or len(df) == 0:
        logger.warn('Data not found. Skipping data export and exiting.')
    else:
        # Export
        tools.export_mri(clean_df(df), study, subject,
            output_dir, day_from, day_to, data_type)

# Add slice number
def add_slice_and_minutes(df, seriesNum, instanceNum, tr):
    series_row = df.loc[df['seriesNum'] == seriesNum]

    frame_num = df.loc[df['seriesNum'] == seriesNum]['frameNum'].iloc[0]
    new_frameNum = max(frame_num, instanceNum)

    seriesDurationSec = series_row['seriesDurationSec'].iloc[0]
    new_seriesDurationSec = max(tr * new_frameNum / 1000 , seriesDurationSec)

    df.loc[df['seriesNum'] == seriesNum, 'seriesDurationSec'] = new_seriesDurationSec
    df.loc[df['seriesNum'] == seriesNum, 'frameNum'] = new_frameNum

    return df

# Final cleaning
def clean_df(df):
    df['day'] = df['day'].astype(int)
    df['weekday'] = df['weekday'].astype(int)
    df['age'] = df['age'].astype(int).astype(str)
    df['frameNum'] = df['frameNum'].astype(int).astype(str)
    df['seriesNum'] = df['seriesNum'].astype(int).astype(str)
    df['sliceThickness'] = df['sliceThickness'].round(3).astype(str)
    df['weight'] = df['weight'].round(3).astype(str)
    return df

# Read file as a dicom file
def get_data(file_path):
    try:
        return dicom.read_file(file_path)
    except Exception as e:
        logger.error(e)
        return None

# Count the number of days between two dates
def process_date(row_date, date_from):
    date_from = date_from.date()
    date_to = row_date.date()

    # The consent date should count as 1 not 0
    day = (date_to - date_from).days + 1
    return day

# Return timestamp in the timezone
def process_datetime(row_timestamp, output_tz, input_tz):
    temp = datetime.strptime(row_timestamp,
        '%Y%m%d %H%M%S.%f')
    temp = pd.Timestamp(temp, tz=input_tz)
    return temp.tz_convert(output_tz)

# Return NCF style weekday..
def process_weekday(row_date):
    weekday = row_date.weekday()

    if weekday == 6:
        return 0
    else:
        return weekday + 1

def process_time(row_date):
    hour = str(row_date.hour) if row_date.hour > 9 else '0' + str(row_date.hour)
    minute = str(row_date.minute) if row_date.minute > 9 else '0' + str(row_date.minute)
    second = str(row_date.second) if row_date.second > 9 else '0' + str(row_date.second)

    return hour + ':' + minute + ':' + second

def get_session(data):
    if 'AccessionNumber' in data:
        return data.AccessionNumber
    elif 'PatientID' in data:
        return data.PatientID
    elif 'PatientName' in data:
        return data.PatientName
    else:
        return ''

def get_type(data):
    if data.SeriesDescription.startswith('SMS'):
        return 'bold'
    elif data.SeriesDescription.startswith('ASL'):
        return 'asl'
    elif (data.SeriesDescription.startswith('T1') and data.SeriesDescription.endswith('RMS')):
        return 't1'
    else:
        return ''

def get_series(data):
    if 'SeriesDescription' in data:
        return data.SeriesDescription
    elif 'ProtocolName' in data:
        return data.ProtocolName
    else:
        return ''

def get_fov(data):
    if 'Rows' not in data:
        return ''

    if 'PixelSpacing' not in data or len(data.PixelSpacing) < 2:
        return ''

    dim_1 = math.floor(float(data.PixelSpacing[0]) * data.Rows)
    dim_2 = math.floor(float(data.PixelSpacing[1]) * data.Rows)

    return '{} * {}'.format(int(dim_1), int(dim_2))

def get_age(data):
    if 'PatientAge' not in data:
        return ''
    age = data.PatientAge
    age = ''.join(i for i in age if i.isdigit())
    return int(age)

def get_bold_type(data):
    series_desc = data.SeriesDescription
    if series_desc.startswith('SMS'):
        try:
            desc = series_desc.split('_')
            bold_type = desc[-1]

            if bold_type.startswith('REST'):
                return 'REST'

            return bold_type
        except Exception as e:
            logger.error(e)
            logger.error('Unexpected series desc')
    return ''


# Parse and process data
def parse(data, date_from, output_tz, input_tz, file_path, file_name, study, subject):
    df = {}
    if 'StudyDate' not in data or 'StudyTime' not in data:
        return df

    date_str = data.StudyDate + ' ' + data.StudyTime
    date_to = process_datetime(date_str, output_tz, input_tz)

    df['$date_to'] = date_to
    df['day'] = process_date(date_to, date_from)
    df['weekday'] = process_weekday(date_to)
    df['timeofday'] = process_time(date_to)

    df['study'] = study
    df['subject'] = subject
    df['XNAT_sessionID'] = get_session(data)
    df['software'] = data.SoftwareVersions if 'SoftwareVersions' in data else ''
    df['weight'] = data.PatientWeight if 'PatientWeight' in data else ''
    df['age'] = get_age(data)
    df['manufacturer'] = data.Manufacturer if 'Manufacturer' in data else ''
    df['manufacturerModel'] = data.ManufacturerModelName if 'ManufacturerModelName' in data else ''
    df['device'] = data.DeviceSerialNumber if 'DeviceSerialNumber' in data else ''
    df['fieldStrength'] = data.MagneticFieldStrength if 'MagneticFieldStrength' in data else ''

    df['seriesDesc'] = get_series(data)
    df['seriesType'] = get_type(data)
    df['seriesNum'] = data.SeriesNumber
    df['tr'] = data.RepetitionTime if 'RepetitionTime' in data else ''
    df['te'] = data.EchoTime if 'EchoTime' in data else ''
    df['sliceThickness'] = data.SliceThickness if 'SliceThickness' in data else ''
    df['flipAngle'] = data.FlipAngle if 'FlipAngle' in data else ''
    #df['flipAngleFlag'] = data.VariableFlipAngleFlag if 'VariableFlipAngleFlag' in data else ''
    #df['fov'] = get_fov(data)
    df['frameNum'] = 1
    df['seriesDurationSec'] = ( data.InstanceNumber * data.RepetitionTime ) / 1000

    df['boldType'] = get_bold_type(data)

    return df
