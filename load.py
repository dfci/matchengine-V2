import os
import csv
import json
import yaml
import logging
from bson import json_util
from argparse import Namespace
from matchengine.engine import MatchEngine

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

    me = MatchEngine(
        config={'trial_key_mappings': {},
                'match_criteria': {'clinical': [], 'genomic': [], 'trial': ["protocol_no", "status"]}, 'indices': {}},
        plugin_dir=args.plugin_dir,
        db_name=args.db_name,
    )

    if args.trial:
        log.info('Adding trial(s) to mongo...')
        load_trials(me, args)

    if args.clinical:
        log.info('Adding clinical data to mongo...')
        load_clinical(args, me)

    if args.genomic:
        if len(list(me.db_ro.clinical.find({}))) == 0:
            log.warning("No clinical documents in db. Please load clinical documents before loading genomic.")

        log.info('Adding genomic data to mongo...')
        load_genomic(args, me)

    log.info('Done.')


#################
# trial loading
#################
def load_trials(me: MatchEngine, args: Namespace):
    if args.trial_format == 'json':
        load_trials_json(args, me)
    elif args.trial_format == 'yaml':
        load_trials_yaml(args, me)


def load_trials_yaml(args: Namespace, me: MatchEngine):
    if os.path.isdir(args.trial):
        load_dir(args, me, "yaml", args.trial, 'trial')
    else:
        load_file(me, 'yaml', args.trial, 'trial')


def load_trials_json(args: Namespace, me: MatchEngine):
    # load a directory of json files
    if os.path.isdir(args.trial):
        load_dir(args, me, "json", args.trial, 'trial')
    else:
        # path leads to a single JSON file
        if is_valid_single_json(args.trial):
            load_file(me, 'json', args.trial, 'trial')

        else:
            file = open(args.trial)
            json_raw = file.read()
            try:
                # mongoexport by default exports each object on a new line
                json_array = json_raw.split('\n')
                for doc in json_array:
                    data = json.loads(doc)
                    me.db_rw.trial.insert(data)
            except Exception:
                # mongoexport also allows an export as a json array
                json_array = json.loads(json_raw)
                for doc in json_array:
                    me.db_rw.trial.insert(doc)
            except Exception("Unknown JSON format"):
                log.warning(
                    'Cannot read json format. JSON documents must be either newline separated, '
                    'in an array, or loaded as separate documents ')


########################
# patient data loading
########################
def load_clinical(args: Namespace, me: MatchEngine):
    if args.patient_format == 'json':

        # load directory of clinical json files
        if os.path.isdir(args.clinical):
            load_dir(args, me, 'json', args.clinical, 'clinical')
        else:
            load_file(me, 'json', args.clinical, 'clinical')

    elif args.patient_format == 'csv':
        load_file(me, 'csv', args.clinical, 'clinical')


def load_genomic(args: Namespace, me: MatchEngine):
    if args.patient_format == 'json':
        # load directory of clinical json files
        if os.path.isdir(args.genomic):
            load_dir(args, me, 'json', args.genomic, 'genomic')
        else:
            load_file(me, 'json', args.genomic, 'genomic')
    elif args.patient_format == 'csv':
        load_file(me, 'csv', args.genomic, 'genomic')

    map_clinical_to_genomic(me)


def map_clinical_to_genomic(me: MatchEngine):
    """Ensure that all genomic docs are linked to their corresponding clinical docs by _id"""
    clinical_docs = list(me.db_ro.clinical.find({}, {"_id": 1, "SAMPLE_ID": 1}))
    clinical_dict = dict(zip([i['SAMPLE_ID'] for i in clinical_docs], [i['_id'] for i in clinical_docs]))

    genomic_docs = list(me.db_ro.genomic.find({}))
    for genomic_doc in genomic_docs:
        if genomic_doc['SAMPLE_ID'] in clinical_dict:
            genomic_doc["CLINICAL_ID"] = clinical_dict[genomic_doc['SAMPLE_ID']]
        else:
            genomic_doc["CLINICAL_ID"] = None
            log.warning(f"WARNING: No clinical document found for ObjectId({genomic_doc['_id']}")
        me.db_rw.genomic.update({'_id': genomic_doc['_id']}, {'$set': {'CLINICAL_ID': genomic_doc["CLINICAL_ID"]}})


##################
# util functions
##################
def load_dir(args: Namespace, me: MatchEngine, filetype: str, path: str, collection: str):
    for filename in os.listdir(path):
        if filename.endswith(f".{filetype}"):
            val = vars(args)[collection]
            full_path = val + filename if val[-1] == '/' else val + '/' + filename
            load_file(me, filetype, full_path, collection)


def load_file(me: MatchEngine, filetype: str, path: str, collection: str):
    file = open(path)
    if filetype == 'yaml':
        data = yaml.safe_load_all(file.read())
        me.db_rw[collection].insert(data)
    elif filetype == 'json':
        file = open(path)
        if is_valid_single_json(path):
            data = json_util.loads(file.read())
            me.db_rw[collection].insert(data)
    elif filetype == 'csv':
        with open(path) as fh:
            file = csv.DictReader(fh, delimiter=',')
            for row in file:
                doc = {}
                for key in row:
                    doc[key] = row[key]
                me.db_rw[collection].insert(doc)


def is_valid_single_json(path: str):
    """Check if a JSON file is a single object or an array of JSON objects"""
    try:
        with open(path) as f:
            json_file = json.loads(f.read())
            if isinstance(json_file, list):
                return False
            return True
    except ValueError:
        return False
