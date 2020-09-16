from __future__ import annotations

from argparse import Namespace
from collections import defaultdict
from unittest import TestCase
import datetime

from matchengine.tests.timetravel_and_override import _scope_handler
from matchengine.internals.database_connectivity.mongo_connection import MongoDBConnection
from matchengine.internals.load import load


class IntegrationTestMatchengineLoading(TestCase):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def _db_exit(self):
        for attr in ['_db_rw', '_db_ro']:
            if hasattr(self, attr):
                getattr(self, attr).__exit__(None, None, None)

    def _reset(self, **kwargs):
        self._db_exit()
        self._db_rw = MongoDBConnection(read_only=False, db='integration_load', async_init=False)
        self._db_ro = MongoDBConnection(read_only=False, db='integration_load', async_init=False)
        self.db_rw = self._db_rw.__enter__()
        self.db_ro = self._db_ro.__enter__()
        if kwargs.get('do_reset_trials', False):
            self.db_rw.trial.drop()

        if kwargs.get('do_reset_patient', False):
            self.db_rw.clinical.drop()
            self.db_rw.genomic.drop()

    def setUp(self) -> None:
        # instantiate matchengine to get access to db helper functions
        self._reset()

        # check to make sure not accidentally connected to production db since tests will drop collections
        assert self.db_rw.name == 'integration_load'

    def test__load_trial_single_json(self):
        self._reset(do_reset_trials=True)
        args = Namespace(clinical=None,
                         genomic=None,
                         db_name='integration_load',
                         plugin_dir='plugins',
                         trial_format='json',
                         trial='matchengine/tests/data/trials/11-111.json',
                         upsert_fields='')
        load(args)
        assert len(list(self.db_ro.trial.find({}))) == 1

    def test__load_trials_single_json_multiple_trials(self):
        """mongoexport by default creates 'json' objects separated by new lines."""
        self._reset(do_reset_trials=True)
        args = Namespace(clinical=None,
                         genomic=None,
                         db_name='integration_load',
                         plugin_dir='plugins',
                         trial_format='json',
                         trial='matchengine/tests/data/trials/two_trials_one_doc.json',
                         upsert_fields='')
        load(args)
        assert len(list(self.db_ro.trial.find({}))) == 2

    def test__load_trials_json_array(self):
        """mongoexport also allows exporting of trials as an array of json objects."""
        self._reset(do_reset_trials=True)
        args = Namespace(
            clinical=None,
            genomic=None,
            db_name='integration_load',
            plugin_dir='plugins',
            trial_format='json',
            trial='matchengine/tests/data/trials/trials_json_array.json',
            upsert_fields='')
        load(args)
        assert len(list(self.db_ro.trial.find({}))) == 2

    def test__load_trials_json_dir(self):
        self._reset(do_reset_trials=True)
        args = Namespace(clinical=None,
                         genomic=None,
                         db_name='integration_load',
                         plugin_dir='plugins',
                         trial_format='json',
                         trial='matchengine/tests/data/integration_trials/',
                         upsert_fields='')
        load(args)
        assert len(list(self.db_ro.trial.find({}))) == 18

    def test__load_trial_single_yaml(self):
        self._reset(do_reset_trials=True)
        args = Namespace(clinical=None,
                         genomic=None, db_name='integration_load',
                         plugin_dir='plugins',
                         trial_format='yaml',
                         trial='matchengine/tests/data/yaml/11-111.yaml',
                         upsert_fields='')
        load(args)
        assert len(list(self.db_ro.trial.find({}))) == 1

    def test__load_trial_yaml_dir(self):
        self._reset(do_reset_trials=True)
        args = Namespace(clinical=None,
                         genomic=None,
                         db_name='integration_load',
                         plugin_dir='plugins',
                         trial_format='yaml',
                         trial='matchengine/tests/data/yaml/',
                         upsert_fields='')
        load(args)
        assert len(list(self.db_ro.trial.find({}))) == 2

    def test__load_clinical_single_json_file(self):
        self._reset(do_reset_patient=True)
        args = Namespace(genomic=None,
                         trial=None,
                         db_name='integration_load',
                         plugin_dir='plugins',
                         patient_format='json',
                         clinical='matchengine/tests/data/clinical_json/test_patient_1.json',
                         upsert_fields='')
        load(args)
        assert len(list(self.db_ro.clinical.find({}))) == 1
        date_class = list(self.db_ro.clinical.find({}))[0]['BIRTH_DATE'].__class__
        assert date_class is _scope_handler['datetime'] or date_class is _scope_handler['old_datetime']

    def test__load_clinical_json_dir(self):
        self._reset(do_reset_patient=True)
        args = Namespace(genomic=None,
                         trial=None,
                         db_name='integration_load',
                         plugin_dir='plugins',
                         patient_format='json',
                         clinical='matchengine/tests/data/clinical_json/',
                         upsert_fields='')
        load(args)
        assert len(list(self.db_ro.clinical.find({}))) == 2
        date_class = list(self.db_ro.clinical.find({}))[0]['BIRTH_DATE'].__class__
        assert date_class is _scope_handler['datetime'] or date_class is _scope_handler['old_datetime']

    def test__load_clinical_single_csv_file(self):
        self._reset(do_reset_patient=True)
        args = Namespace(genomic=None,
                         trial=None,
                         db_name='integration_load',
                         plugin_dir='plugins',
                         patient_format='csv',
                         clinical='matchengine/tests/data/clinical_csv/test_patients.csv',
                         upsert_fields='')
        load(args)
        assert len(list(self.db_ro.clinical.find({}))) == 2
        date_class = list(self.db_ro.clinical.find({}))[0]['BIRTH_DATE'].__class__
        assert date_class is _scope_handler['datetime'] or date_class is _scope_handler['old_datetime']

    def test__load_genomic_single_json_file(self):
        self._reset(do_reset_patient=True)

        # load clinical doc
        args = Namespace(genomic=None,
                         trial=None,
                         db_name='integration_load',
                         plugin_dir='plugins',
                         patient_format='json',
                         clinical='matchengine/tests/data/clinical_json/test_patient_1.json',
                         upsert_fields='')
        load(args)

        # load genomic doc
        args = Namespace(genomic='matchengine/tests/data/genomic_json/test_patient_1.json',
                         trial=None,
                         db_name='integration_load',
                         plugin_dir='plugins',
                         patient_format='json',
                         clinical=None,
                         upsert_fields='')
        load(args)
        clinical = list(self.db_ro.clinical.find({}))
        genomic = list(self.db_ro.genomic.find({}))
        assert len(clinical) == 1
        assert len(genomic) == 1
        assert genomic[0]['CLINICAL_ID'] == clinical[0]['_id']

    def test__load_genomic_json_dir(self):
        self._reset(do_reset_patient=True)
        # load clinical docs
        args = Namespace(genomic=None,
                         trial=None,
                         db_name='integration_load',
                         plugin_dir='plugins',
                         patient_format='json',
                         clinical='matchengine/tests/data/clinical_json/',
                         upsert_fields='')
        load(args)

        # load genomic docs
        args = Namespace(genomic='matchengine/tests/data/genomic_json/',
                         trial=None,
                         db_name='integration_load',
                         plugin_dir='plugins',
                         patient_format='json',
                         clinical=None,
                         upsert_fields='')
        load(args)
        clinical = list(self.db_ro.clinical.find({}))
        genomic = list(self.db_ro.genomic.find({}))
        assert len(clinical) == 2
        assert len(genomic) == 2
        assert genomic[0]['CLINICAL_ID'] == clinical[0]['_id']
        assert genomic[1]['CLINICAL_ID'] == clinical[1]['_id']

    def test__load_genomic_csv(self):
        self._reset(do_reset_patient=True)

        # load clinical doc
        args = Namespace(genomic=None,
                         trial=None,
                         db_name='integration_load',
                         plugin_dir='plugins',
                         patient_format='json',
                         clinical='matchengine/tests/data/clinical_json/',
                         upsert_fields='')
        load(args)

        # load genomic doc
        args = Namespace(genomic='matchengine/tests/data/genomic_csv/test_patients.csv',
                         trial=None,
                         db_name='integration_load',
                         plugin_dir='plugins',
                         patient_format='csv',
                         clinical=None,
                         upsert_fields='')
        load(args)
        clinical = list(self.db_ro.clinical.find({}))
        genomic = list(self.db_ro.genomic.find({}))
        assert len(clinical) == 2
        assert len(genomic) == 2

        track = defaultdict(lambda: 1)
        for clinical_doc in clinical:
            track[clinical_doc['_id']] -= 1
        for genomic_doc in genomic:
            assert track[genomic_doc['CLINICAL_ID']] == 0

    def tearDown(self) -> None:
        self._db_exit()
