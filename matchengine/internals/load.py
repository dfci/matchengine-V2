import csv
from datetime import datetime
import json
import logging
import os
from argparse import Namespace
from contextlib import ExitStack

import yaml
from bson import json_util

from matchengine.internals.database_connectivity.mongo_connection import MongoDBConnection

logging.basicConfig(level=logging.INFO)
log = logging.getLogger('matchengine')


def load(args: Namespace):
    """
    Load data into MongoDB for matching.

    Depending on the naming conventions used in your data, it may be necessary to either alter the data itself,
    or to define custom transformation functions in order for matching to work correctly.

    These transformations to data to prepare for matching can be made using the config.json file,
    and custom functions as described in match_criteria_transform.json.

    For more information and examples, see the README.
    """

    with ExitStack() as stack:
        db_rw = stack.enter_context(MongoDBConnection(read_only=False, db=args.db_name, async_init=False))
        db_ro = stack.enter_context(MongoDBConnection(read_only=True, db=args.db_name, async_init=False))
        log.info(f"Database: {args.db_name}")
        if args.trial:
            log.info('Adding trial(s) to mongo...')
            load_trials(db_rw, args)

        if args.clinical:
            log.info('Adding clinical data to mongo...')
            load_clinical(db_rw, args)

        if args.genomic:
            if len(list(db_ro.clinical.find({}))) == 0:
                log.warning("No clinical documents in db. Please load clinical documents before loading genomic.")

            log.info('Adding genomic data to mongo...')
            load_genomic(db_rw, db_ro, args)

        log.info('Done.')


#################
# trial loading
#################
def load_trials(db_rw, args: Namespace):
    if args.trial_format == 'json':
        load_trials_json(args, db_rw)
    elif args.trial_format == 'yaml':
        load_trials_yaml(args, db_rw)


def load_trials_yaml(args: Namespace, db_rw):
    if os.path.isdir(args.trial):
        load_dir(args, db_rw, "yaml", args.trial, 'trial')
    else:
        load_file(db_rw, 'yaml', args.trial, 'trial')


def load_trials_json(args: Namespace, db_rw):
    # load a directory of json files
    if os.path.isdir(args.trial):
        load_dir(args, db_rw, "json", args.trial, 'trial')
    else:
        # path leads to a single JSON file
        if is_valid_single_json(args.trial):
            load_file(db_rw, 'json', args.trial, 'trial')

        else:
            with open(args.trial) as file:
                json_raw = file.read()
                success = None
                try:
                    # mongoexport by default exports each object on a new line
                    json_array = json_raw.split('\n')
                    for doc in json_array:
                        data = json.loads(doc)
                        db_rw.trial.insert_one(data)
                    success = True
                except json.decoder.JSONDecodeError as e:
                    log.debug(f"{e}")
                if not success:
                    try:
                        # mongoexport also allows an export as a json array
                        json_array = json.loads(json_raw)
                        for doc in json_array:
                            db_rw.trial.insert_one(doc)
                        success = True
                    except json.decoder.JSONDecodeError as e:
                        log.debug(f"{e}")
                        if not success:
                            log.warning(
                                'Cannot read json format. JSON documents must be either newline separated, '
                                'in an array, or loaded as separate documents ')
                            raise Exception("Unknown JSON Format")


########################
# patient data loading
########################
def load_clinical(db_rw, args: Namespace):
    if args.patient_format == 'json':

        # load directory of clinical json files
        if os.path.isdir(args.clinical):
            load_dir(args, db_rw, 'json', args.clinical, 'clinical')
        else:
            load_file(db_rw, 'json', args.clinical, 'clinical')

    elif args.patient_format == 'csv':
        load_file(db_rw, 'csv', args.clinical, 'clinical')


def load_genomic(db_rw, db_ro, args: Namespace, ):
    if args.patient_format == 'json':
        # load directory of clinical json files
        if os.path.isdir(args.genomic):
            load_dir(args, db_rw, 'json', args.genomic, 'genomic')
        else:
            load_file(db_rw, 'json', args.genomic, 'genomic')
    elif args.patient_format == 'csv':
        load_file(db_rw, 'csv', args.genomic, 'genomic')

    map_clinical_to_genomic(db_rw, db_ro)


def map_clinical_to_genomic(db_rw, db_ro):
    """Ensure that all genomic docs are linked to their corresponding clinical docs by _id"""
    clinical_docs = list(db_ro.clinical.find({}, {"_id": 1, "SAMPLE_ID": 1}))
    clinical_dict = dict(zip([i['SAMPLE_ID'] for i in clinical_docs], [i['_id'] for i in clinical_docs]))

    genomic_docs = list(db_ro.genomic.find({}))
    for genomic_doc in genomic_docs:
        if genomic_doc['SAMPLE_ID'] in clinical_dict:
            genomic_doc["CLINICAL_ID"] = clinical_dict[genomic_doc['SAMPLE_ID']]
        else:
            genomic_doc["CLINICAL_ID"] = None
            log.warning(f"WARNING: No clinical document found for ObjectId({genomic_doc['_id']}")
        db_rw.genomic.update_one({'_id': genomic_doc['_id']}, {'$set': {'CLINICAL_ID': genomic_doc["CLINICAL_ID"]}})


##################
# util functions
##################
def load_dir(args: Namespace, db_rw, filetype: str, path: str, collection: str):
    for filename in os.listdir(path):
        if filename.endswith(f".{filetype}"):
            val = vars(args)[collection]
            full_path = val + filename if val[-1] == '/' else val + '/' + filename
            load_file(db_rw, filetype, full_path, collection)


def load_file(db_rw, filetype: str, path: str, collection: str):
    with open(path) as file_handle:
        if filetype == 'csv':
            file_handle = csv.DictReader(file_handle, delimiter=',')
            for row in file_handle:
                for key in file_handle.fieldnames:
                    if key == 'BIRTH_DATE':
                        row[key] = convert_birthdate(row[key])
                        row['BIRTH_DATE_INT'] = int(row[key].strftime('%Y%m%d'))
                db_rw[collection].insert_one(row)
        else:
            raw_file_data = file_handle.read()
            if filetype == 'yaml':
                data = yaml.safe_load_all(raw_file_data)
                db_rw[collection].insert_many(data)
            elif filetype == 'json':
                if is_valid_single_json(path):
                    data = json_util.loads(raw_file_data)
                    for key in list(data.keys()):
                        if key == 'BIRTH_DATE':
                            data[key] = convert_birthdate(data[key])
                            data['BIRTH_DATE_INT'] = int(data[key].strftime('%Y%m%d'))
                    db_rw[collection].insert_one(data)


def convert_birthdate(birth_date):
    """Convert a string birthday to to datetime object"""
    try:
        birth_date_dt = datetime.strptime(birth_date, "%Y-%m-%d")
    except Exception as e:
        log.warn("Unable to import clinical data due to malformed "
                 "patient birth date. \n\nBirthdates must be strings with "
                 "the following format \n %Y-%m-%d \n 2019-10-27 ")
        raise ImportError
    return birth_date_dt

def is_valid_single_json(path: str):
    """Check if a JSON file is a single object or an array of JSON objects"""
    try:
        with open(path) as f:
            json_file = json.load(f)
            if json_file.__class__ is list:
                return False
            return True
    except (FileNotFoundError, json.decoder.JSONDecodeError) as e:
        if e.__class__ is FileNotFoundError:
            log.error(f"{e}")
            raise e
        elif e.__class__ is json.decoder.JSONDecodeError:
            return False
