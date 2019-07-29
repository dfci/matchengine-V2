import os
import sys
import json
import yaml
import logging
import subprocess
import pandas as pd
from bson import json_util
from pymongo import ASCENDING
from matchengine.utilities.mongo_connection import MongoDBConnection

logging.basicConfig(level=logging.INFO)
log = logging.getLogger('matchengine')


def load(args):
    """
    Load data into MongoDB for matching.

    Depending on the naming conventions used in your data, it may be necessary to either alter the data itself,
    or to define custom transformation functions in order for matching to work correctly.

    These transformations to data to prepare for matching can be made using the config.json file,
    and custom functions as described in match_criteria_transform.json.

    For more information and examples, see the README.

    :param args: clinical: Path to csv file containing clinical data. Required fields are:
        - MRN (Unique patient identifier)
        - SAMPLE_ID (Unique sample identifier)
        - BIRTH_DATE
            NOTE: Depending on the format of the birth date, a custom translation function may be required.
            This can be added to match_criteria_transform.py
        - ONCOTREE_PRIMARY_DIAGNOSIS_NAME (Disease diagnosis)
            NOTE: This field is required, but can be configured to work with other disease oncology's

    :param args: genomic: Path to csv file containing genomic data. The following fields are required:
        - SAMPLE_ID (Unique sample identifier)
        - CLINICAL_ID (reference to the clinical document associated with the genomic information)
        - TRUE_HUGO_SYMBOL (Gene name)
        - TRUE_PROTEIN_CHANGE (Specific variant)
        - TRUE_VARIANT_CLASSIFICATION (Variant type)
        - VARIANT_CATEGORY (CNV, MUTATION, or SV)
        - TRUE_TRANSCRIPT_EXON (Exon number <integer>
        - CNV_CALL (Heterozygous deletion, Homozygous deletion, Gain, High Level amplification, or null)
        - WILDTYPE (True or False)

    :param args: trials: Path to bson trial file or directory.
    """

    with MongoDBConnection(uri=args.mongo_uri, async_init=False) as db:
        log.info("Mongo URI: %s" % args.mongo_uri)
        t = Trial(args)
        p = Patient(args)

        # Add trials to mongo
        if args.trials:
            log.info('Adding trials to mongo...')
            t.load_dict[args.trial_format](args.trials)

        # Add patient data to mongo
        if args.clinical and args.genomic:
            log.info('Reading data into pandas...')
            is_bson_or_json = p.load_dict[args.patient_format](args.clinical, args.genomic)

            if not is_bson_or_json:
                # Add clinical data to mongo
                log.info('Adding clinical data to mongo...')
                if isinstance(p.clinical, dict):
                    db.clinical.insert(p.clinical)
                else:
                    clinical_json = json.loads(p.clinical.T.to_json()).values()
                    db.clinical.insert(clinical_json)

                # Get clinical ids from mongo
                log.info('Adding clinical ids to genomic data...')
                clinical_doc = list(db.clinical.find({}, {"_id": 1, "SAMPLE_ID": 1}))
                clinical_dict = dict(zip([i['SAMPLE_ID'] for i in clinical_doc], [i['_id'] for i in clinical_doc]))

                # pd -> json
                if args.trial_format == 'pkl':
                    genomic_json = json.loads(p.genomic_df.to_json(orient='records'))
                else:
                    genomic_json = json.loads(p.genomic_df.T.to_json()).values()

                # Map clinical ids to genomic data
                for genomic_doc in genomic_json:
                    if genomic_doc['SAMPLE_ID'] in clinical_dict:
                        genomic_doc["CLINICAL_ID"] = clinical_dict[genomic_doc['SAMPLE_ID']]
                    else:
                        genomic_doc["CLINICAL_ID"] = None

                # Add genomic data to mongo
                log.info('Adding genomic data to mongo...')
                db.genomic.insert(genomic_json)

            # Create indices
            log.info('Creating indices...')
            db.genomic.create_index([("TRUE_HUGO_SYMBOL", ASCENDING), ("WILDTYPE", ASCENDING)])
            db.genomic.create_index("SAMPLE_ID")
            db.genomic.create_index("CLINICAL_ID")
            db.genomic.create_index("WILDTYPE")
            db.genomic.create_index("VARIANT_CATEGORY")
            db.genomic.create_index("TRUE_PROTEIN_CHANGE")
            db.genomic.create_index("TRUE_HUGO_SYMBOL")

            db.clinical.create_index('SAMPLE_ID')
            db.clinical.create_index('MRN')
            db.clinical.create_index('VITAL_STATUS')
            db.clinical.create_index('ONCOTREE_PRIMARY_DIAGNOSIS_NAME')
            db.clinical.create_index('BIRTH_DATE')

        elif args.clinical and not args.genomic or args.genomic and not args.clinical:
            log.error('If loading patient information, please provide both clinical and genomic data.')
            sys.exit(1)

        log.info('Done.')


class Trial:
    def __init__(self, args):

        self.args = args
        self.load_dict = {
            'yml': self.yaml_to_mongo,
            'bson': self.bson_to_mongo,
            'json': self.json_to_mongo
        }

    def bson_to_mongo(self, path):
        """
        If you specify the path to a directory, all files with extension BSON will be added to MongoDB.
        If you specify the path to a specific BSON file, it will add that file to MongoDB.

        :param path: Path to BSON file or directory
        """

        if os.path.isdir(path):
            for filename in os.listdir(path):
                if filename.endswith(".bson"):
                    full_path = path + filename if path[-1] == '/' else path + '/' + filename
                    cmd = "mongorestore --uri %s %s" % (self.args.mongo_uri, full_path)
                    subprocess.call(cmd.split(' '))
        else:
            cmd = "mongorestore --uri %s %s" % (self.args.mongo_uri, path)
            subprocess.call(cmd.split(' '))

    def json_to_mongo(self, path):
        """
        If you specify the path to a directory, all files with extension JSON will be added to MongoDB.
        If you specify the path to a specific JSON file, it will add that file to MongoDB.
        If you specify the path to a single file which is an array of JSON documents, each will be added separately

        :param path: Path to JSON file or directory.
        """

        with MongoDBConnection(uri=self.args.mongo_uri, async_init=False) as db:
            # path is a directory of separate JSON documents
            if os.path.isdir(path):
                for filename in os.listdir(path):
                    if filename.endswith(".json"):
                        full_path = path + filename if path[-1] == '/' else path + '/' + filename
                        file = open(full_path)
                        data = json_util.loads(file.read())
                        db.trial.insert(data)
            else:
                # path leads to a single JSON file
                file = open(path)
                if self.is_valid_single_json(path):
                    data = json_util.loads(file.read())
                    db.trial.insert(data)

                # assume file is a list of json documents
                else:
                    cmd = 'mongoimport ' \
                          '--uri=%s ' \
                          '--collection=%s ' \
                          '--upsert ' \
                          '--upsertFields=[%s] ' \
                          '--file=%s ' \
                          '--jsonArray' % (self.args.mongo_uri,
                                           'trial',
                                           self.args.upsert_fields,
                                           path)
                    subprocess.call(cmd.split(' '))

    def yaml_to_mongo(self, yml):
        """
        If you specify the path to a directory, all files with extension YML will be added to MongoDB.
        If you specify the path to a specific YML file, it will add that file to MongoDB.

        :param yml: Path to YML file.
        """

        # search directory for ymls
        if os.path.isdir(yml):
            for y in os.listdir(yml):
                ymlpath = os.path.join(yml, y)

                # only add files of extension ".yml"
                if ymlpath.split('.')[-1] != 'yml':
                    continue

                # convert yml to json format
                self.add_trial(ymlpath)
        else:
            self.add_trial(yml)

    def add_trial(self, yml):
        """
        Adds file in YAML format to MongoDB

        :param yml: Path to file
        """

        with open(yml) as f:
            t = yaml.safe_load(f.read())

            with MongoDBConnection(uri=self.args.mongo_uri, async_init=False) as db:
                db.trial.insert_one(t)


class Patient:

    def __init__(self, args):
        self.args = args
        self.load_dict = {
            'csv': self.load_csv,
            'pkl': self.load_pkl,
            'bson': self.load_bson,
            'json': self.load_json
        }
        self.clinical = None
        self.genomic_df = None

    def load_csv(self, clinical, genomic):
        """Load CSV file into a Pandas dataframe"""
        self.clinical = pd.read_csv(clinical)
        self.genomic_df = pd.read_csv(genomic, low_memory=False)

    def load_pkl(self, clinical, genomic):
        """Load PKL file into a Pandas dataframe"""
        self.clinical = pd.read_pickle(clinical)
        self.genomic_df = pd.read_pickle(genomic)

    def load_json(self, clinical_path, genomic_path):
        """
        Load clinical and genomic json documents.
        If file is not valid JSON, assume it is an array of valid JSON documents.
        If path is a directory, add all documents with extension JSON
        :param clinical_path:
        :param genomic_path:
        :return:
        """
        if os.path.isdir(clinical_path):
            for file in os.listdir(clinical_path):
                c_file = os.path.join(clinical_path, file)
                self.add_json(c_file, "clinical")
        else:
            self.add_json(clinical_path, "clinical")

        if os.path.isdir(genomic_path):
            for file in os.listdir(genomic_path):
                g_file = os.path.join(genomic_path, file)
                self.add_json(g_file, "genomic")
        else:
            self.add_json(genomic_path, "genomic")

        return True

    def load_bson(self, clinical, genomic):
        """Load bson file into MongoDB"""
        cmd1 = "mongorestore --uri=%s %s" % (self.args.mongo_uri, clinical)
        cmd2 = "mongorestore --uri=%s %s" % (self.args.mongo_uri, genomic)
        subprocess.call(cmd1.split(' '))
        subprocess.call(cmd2.split(' '))
        return True

    def add_json(self, path, collection):
        if path.split('.')[-1] != 'json':
            return

        cmd = "mongoimport " \
              "--uri=%s " \
              "--collection=%s " \
              "--mode=upsert " \
              "--upsertFields=%s " \
              "--file=%s" % (self.args.mongo_uri,
                             collection,
                             self.args.upsert_fields,
                             path)

        if not self.is_valid_single_json(path):
            cmd += ' --jsonArray'

        log.info("Loading %s... %s" % (collection, path))
        subprocess.call(cmd.split(' '))

    @staticmethod
    def is_valid_single_json(file):
        """Check if a JSON file is a single object or an array of JSON objects"""
        try:
            with open(file) as f:
                json_file = json.loads(f.read())
                if isinstance(json_file, list):
                    return False
                return True
        except ValueError:
            return False
