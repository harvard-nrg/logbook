#!/usr/bin/env python
import os
import sys
import pandas as pd
import logging
import argparse as ap
from importlib import import_module
from datetime import datetime
from logbook import tools

logger = logging.getLogger(os.path.basename(__file__))

def main():
    argparser = ap.ArgumentParser('PHOENIX Metadata LogBook Pipeline')

    # Input and output parameters
    argparser.add_argument('--phoenix-dir',
         help='Phoenix directory')
    argparser.add_argument('--consent-dir',
         help='Consent directory')
    argparser.add_argument('--debug', action='store_true', help='Enable debug messages')

    argparser.add_argument('--data-type',
         help='Data type name (ex. "phone actigraphy" or "onsite_interview")',
         nargs='+')
    argparser.add_argument('--phone-stream',
         help='(ex. "survey_answers" or "accelerometer")',
         nargs='+')
    argparser.add_argument('--output-dir',
         help='Path to the output directory')

    argparser.add_argument('--study',
         nargs='+', help='Study name')
    argparser.add_argument('--subject',
         nargs='+', help='Subject ID')

    # Basic targeting parameters
    argparser.add_argument('--log-dir',
        help='Directory where the log is written')

    argparser.add_argument('--input-tz',
        help='Timezone info for the input. (Default: UTC)',
        default = 'UTC')
    argparser.add_argument('--output-tz',
        help='Timezone info for the output. (Default: America/New_York)',
        default = 'America/New_York')
    argparser.add_argument('--day-from',
        help='Output day from. (optional)',
        type=int)
    argparser.add_argument('--day-to',
        help='Output day to. (optional; By default, process data for all days)',
        type=int)

    args = argparser.parse_args()

    # Log file initialization
    log_date= datetime.today().strftime('%Y%m%d')
    DEFAULT_LOGFILE_LOCATION = os.path.join(str(args.log_dir),str(log_date)+'logbook.log')
    logging.basicConfig(level=logging.DEBUG if args.debug else logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        filename=str(DEFAULT_LOGFILE_LOCATION))

    # Gets all studies under each subdirectory
    studies = args.study if args.study else tools.scan_dir(args.consent_dir)

    for study in studies:
        study_path = os.path.join(args.consent_dir, study)
        consent_path = os.path.join(args.consent_dir, study, study + '_metadata.csv')
        consents = get_consents(consent_path)
        if consents is None: continue

        # Gets all subjects under the study directory
        subjects = args.subject if args.subject else tools.scan_dir(study_path)
        for subject in subjects:
            subject_path = os.path.join(study_path, subject)

            verified = verify_subject(subject, subject_path, consents)
            if not verified:
                continue

            logger.info('Processing {S} in {ST}'.format(S=subject, ST=study))
            date_from = consents[subject][0]

            # Loops through PHOENIX's subdirectories.
            directories = tools.scan_dir(args.phoenix_dir)
            for directory in sorted(directories):
                subject_path = os.path.join(args.phoenix_dir, directory, study, subject)

                # Scan each subject's directory to find available data types
                data_types = args.data_type if args.data_type else tools.scan_dir(subject_path)
                for data_type in data_types:
                    data_path = os.path.join(subject_path, data_type, 'raw')
                    output_path = args.output_dir if args.output_dir else os.path.join(subject_path,
                        data_type,
                        'processed')

                    mod = get_module()
                    mod_parser = mod.parse_args()

                    new_args, unknown = mod_parser.parse_known_args([
                        '--date-from', str(date_from),
                        '--read-dir', str(data_path),
                        '--phone-stream', str(args.phone_stream),
                        '--output-dir', str(output_path),
                        '--day-from', str(args.day_from),
                        '--day-to', str(args.day_to),
                        '--input-tz', str(args.input_tz),
                        '--output-tz', str(args.output_tz),
                        '--study', str(study),
                        '--subject', str(subject),
                        '--data-type', str(data_type)
                    ])
                    mod.main(new_args)
    return

# Import module based on user input
def get_module():
    try:
        return import_module('logbook', __name__)
    except Exception as e:
        logger.error(e)
        logger.error('Could not import the pipeline module. Exiting')
        sys.exit(1)

# Ensures data can be processed for the subject
def verify_subject(subject, path, consents):
    # Ensures the subject directory is not the consent directory
    if subject.endswith('.csv'):
        logger.debug('Subject {S} is not a valid subject.'.format(S=subject))
        return False

    if not os.path.isdir(path):
        logger.debug('Path {P} does not exist.'.format(P=path))
        return False

    if not subject in consents:
        logger.debug('Consent date does not exist for {S}.'.format(S=subject))
        return False

    return True

# Get consents for the study
def get_consents(path):
    try:
        df = pd.read_csv(path, keep_default_na=False, engine='c', skipinitialspace=True,index_col=False)
        df = df.pivot(
            index=None,
            columns='Subject ID',
            values='Consent'
            ).bfill().iloc[[0],:]
        return df
    except Exception as e:
        logger.error(e)
        logger.error('Unable to retrieve consents from {0}.'.format(path))
        return None

if __name__ == '__main__':
    main()
