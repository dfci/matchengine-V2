import csv
import datetime
import json
import os
from shutil import which
from collections import defaultdict
from contextlib import redirect_stderr
from unittest import TestCase

from bson import ObjectId

from matchengine.internals.database_connectivity.mongo_connection import MongoDBConnection
from matchengine.internals.engine import MatchEngine
from matchengine.tests.timetravel_and_override import set_static_date_time, perform_override, unoverride_datetime


class IntegrationTestMatchengine(TestCase):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.first_run_done = False

    def _reset(self, **kwargs):
        if not self.first_run_done:
            if kwargs.get('do_reset_time', True):
                set_static_date_time()
            self.first_run_done = True
        with MongoDBConnection(read_only=False, db='integration', async_init=False) as setup_db:
            assert setup_db.name == 'integration'

            if not kwargs.get("skip_sample_id_reset", False):
                setup_db.clinical.update({"SAMPLE_ID": "5d2799d86756630d8dd065b8"},
                                         {"$set": {"ONCOTREE_PRIMARY_DIAGNOSIS_NAME": "Non-Small Cell Lung Cancer",
                                                   "_updated": datetime.datetime(2001, 1, 1, 1, 1, 1, 1)}})

            if kwargs.get('do_reset_trial_matches', False):
                setup_db.trial_match.drop()

            if kwargs.get('reset_run_log', False):
                setup_db.run_log_trial_match.drop()

            if kwargs.get('do_reset_trials', False):
                setup_db.trial.drop()
                trials_to_load = map(lambda x: os.path.join('matchengine',
                                                            'tests',
                                                            'data',
                                                            'integration_trials',
                                                            x + '.json'),
                                     kwargs.get('trials_to_load', list()))
                for trial_path in trials_to_load:
                    with open(trial_path) as trial_file_handle:
                        trial = json.load(trial_file_handle)
                    setup_db.trial.insert(trial)
            if kwargs.get('do_rm_clinical_run_history', False):
                setup_db.clinical_run_history_trial_match.drop()

        if hasattr(self, 'me'):
            self.me.__exit__(None, None, None)

        self.me = MatchEngine(
            match_on_deceased=kwargs.get('match_on_deceased', True),
            match_on_closed=kwargs.get('match_on_closed', True),
            num_workers=kwargs.get('num_workers', 1),
            visualize_match_paths=kwargs.get('visualize_match_paths', False),
            config=kwargs.get('config', 'matchengine/config/dfci_config.json'),
            plugin_dir=kwargs.get('plugin_dir', 'matchengine/plugins/'),
            match_document_creator_class=kwargs.get('match_document_creator_class', "DFCITrialMatchDocumentCreator"),
            fig_dir=kwargs.get('fig_dir', '/tmp/'),
            protocol_nos=kwargs.get('protocol_nos', None),
            sample_ids=kwargs.get('sample_ids', None),
            report_all_clinical_reasons=kwargs.get("report_all_clinical", True)
        )

        assert self.me.db_rw.name == 'integration'
        # Because ages are relative (people get older with the passage of time :/) the test data will stop working
        # to negate this, we need datetime.datetime.now() and datetime.date.today() to always return the same value
        # To accomplish this, there are overridden classes for datetime.datetime and datetime.date, implementing
        # static versions of now() and today(), respectively

        # The logic for overriding classes is generified here for future extensibility.

        # To perform the override, we first iterate over each of the override classes (at the time of writing,
        # this is just StaticDatetime and StaticDate
        if kwargs.get("do_reset_time", True):
            if kwargs.get('date_args', False):
                set_static_date_time(**kwargs['date_args'])
            else:
                set_static_date_time()
        if kwargs.get("unreplace_dt", False):
            unoverride_datetime()

    def test__match_on_deceased_match_on_closed(self):
        self._reset(do_reset_trials=True,
                    trials_to_load=['all_closed', 'all_open', 'closed_dose', 'closed_step_arm'])
        assert self.me.db_rw.name == 'integration'
        self.me.get_matches_for_all_trials()
        assert len(set(self.me._matches.keys()).intersection({'10-001', '10-002', '10-003', '10-004'})) == 4
        assert len(self.me._matches['10-001']) == 5
        assert len(self.me._matches['10-002']) == 5
        assert len(self.me._matches['10-003']) == 5
        assert len(self.me._matches['10-004']) == 5

    def test__match_on_deceased(self):
        self._reset(match_on_deceased=True, match_on_closed=False,
                    do_reset_trials=True,
                    trials_to_load=['all_closed', 'all_open', 'closed_dose', 'closed_step_arm'])
        assert self.me.db_rw.name == 'integration'
        self.me.get_matches_for_all_trials()
        assert len(set(self.me._matches.keys()).intersection({'10-002', '10-003', '10-004'})) == 3
        assert len(self.me._matches['10-002']) == 5
        assert len(self.me._matches['10-003']) == 5

    def test__match_on_closed(self):
        self._reset(match_on_deceased=False, match_on_closed=True,
                    do_reset_trials=True,
                    trials_to_load=['all_closed', 'all_open', 'closed_dose', 'closed_step_arm'])
        assert self.me.db_rw.name == 'integration'
        self.me.get_matches_for_all_trials()
        assert len(set(self.me._matches.keys()).intersection({'10-001', '10-002', '10-003', '10-004'})) == 4
        assert len(self.me._matches['10-001']) == 4
        assert len(self.me._matches['10-002']) == 4
        assert len(self.me._matches['10-003']) == 4
        assert len(self.me._matches['10-004']) == 4

    def test_update_trial_matches(self):
        self._reset(do_reset_trial_matches=True,
                    do_reset_trials=True,
                    trials_to_load=['all_closed', 'all_open', 'closed_dose', 'closed_step_arm'],
                    report_all_clinical=False)
        assert self.me.db_rw.name == 'integration'
        self.me.get_matches_for_all_trials()
        for protocol_no in self.me.trials.keys():
            self.me.update_matches_for_protocol_number(protocol_no)
        assert self.me.db_ro.trial_match.count() == 48

    def test_wildcard_protein_change(self):
        self._reset(do_reset_trial_matches=True,
                    do_reset_trials=True,
                    trials_to_load=['wildcard_protein_found', 'wildcard_protein_not_found'])
        assert self.me.db_rw.name == 'integration'
        self.me.get_matches_for_all_trials()
        assert len(self.me._matches['10-005']) == 64

    def test_match_on_individual_protocol_no(self):
        self._reset(do_reset_trial_matches=True,
                    do_reset_trials=True,
                    trials_to_load=['wildcard_protein_not_found'],
                    protocol_nos={'10-006'})
        assert self.me.db_rw.name == 'integration'
        self.me.get_matches_for_all_trials()
        assert len(self.me._matches.keys()) == 1

    def test_match_on_individual_sample(self):
        self._reset(
            do_reset_trial_matches=True,
            do_reset_trials=True,
            trials_to_load=['all_closed', 'all_open', 'closed_dose', 'closed_step_arm'],
            sample_ids={'5d2799cb6756630d8dd0621d'}
        )
        assert self.me.db_rw.name == 'integration'
        self.me.get_matches_for_all_trials()
        assert len(self.me._matches['10-001']) == 1
        assert len(self.me._matches['10-002']) == 1
        assert len(self.me._matches['10-003']) == 1
        assert len(self.me._matches['10-004']) == 1

    def test_output_csv(self):
        self._reset(do_reset_trial_matches=True,
                    do_reset_trials=True,
                    trials_to_load=['all_closed', 'all_open', 'closed_dose', 'closed_step_arm'],
                    report_all_clinical=False)
        assert self.me.db_rw.name == 'integration'
        self.me.get_matches_for_all_trials()
        filename = f'trial_matches_{datetime.datetime.now().strftime("%b_%d_%Y_%H:%M")}.csv'
        try:
            from matchengine.internals.utilities.output import create_output_csv
            create_output_csv(self.me)
            assert os.path.exists(filename)
            assert os.path.isfile(filename)
            with open(filename) as csv_file_handle:
                csv_reader = csv.DictReader(csv_file_handle)
                fieldnames = set(csv_reader.fieldnames)
                rows = list(csv_reader)
            from matchengine.internals.utilities.output import get_all_match_fieldnames
            assert len(fieldnames.intersection(get_all_match_fieldnames(self.me))) == len(fieldnames)
            assert sum([1
                        for protocol_matches in self.me._matches.values()
                        for sample_matches in protocol_matches.values()
                        for _ in sample_matches]) == 48
            assert len(rows) == 48
            os.unlink(filename)
        except Exception as e:
            if os.path.exists(filename):
                os.unlink(filename)
            raise e

    def test_run_log_updated_sample_leads_to_new_trial_match_and_existing_sample_not_updated_does_not_cause_new_trial_matches(self):

        # run 1 - create matches and run log row
        self._reset(
            do_reset_trial_matches=True,
            do_reset_trials=True,
            trials_to_load=['run_log_arm_closed'],
            reset_run_log=True,
            match_on_closed=True,
            match_on_deceased=False,
            do_rm_clinical_run_history=True,
            report_all_clinical=False
        )
        assert self.me.db_rw.name == 'integration'
        self.me.db_rw.clinical.update({"SAMPLE_ID": "5d2799d86756630d8dd065b8"},
                                      {"$set": {"ONCOTREE_PRIMARY_DIAGNOSIS_NAME": "Gibberish",
                                                "_updated": datetime.datetime.now()}})
        self.me.get_matches_for_all_trials()
        self.me.update_all_matches()
        trial_matches = list(self.me.db_ro.trial_match.find())
        run_log_trial_match = list(self.me.db_ro.run_log_trial_match.find())
        clinical_run_history_trial_match = list(self.me.db_ro.clinical_run_history_trial_match.find())
        assert len(trial_matches) == 2
        assert len(run_log_trial_match) == 1
        assert len(clinical_run_history_trial_match) == 1392

        self._reset(
            do_reset_trial_matches=False,
            do_reset_trials=False,
            reset_run_log=False,
            match_on_closed=True,
            match_on_deceased=False,
            do_rm_clinical_run_history=False,
            do_reset_time=False,
            report_all_clinical=False,
            skip_sample_id_reset=False
        )

        # check that run log has 2 rows, and number of trial matches has not changed
        self.me.get_matches_for_all_trials()
        self.me.update_all_matches()
        trial_matches = list(self.me.db_ro.trial_match.find())
        run_log_trial_match = list(self.me.db_ro.run_log_trial_match.find({}))
        clinical_run_history_trial_match = list(
            self.me.db_ro.clinical_run_history_trial_match.find({'clinical_id': ObjectId("5d2799d86756630d8dd065b8")})
        )[0]
        assert len(trial_matches) == 3
        assert len(run_log_trial_match) == 2
        assert len(clinical_run_history_trial_match['run_history']) == 2

        self._reset(
            do_reset_trial_matches=False,
            do_reset_trials=False,
            reset_run_log=False,
            match_on_closed=True,
            match_on_deceased=False,
            do_rm_clinical_run_history=False,
            do_reset_time=False,
            report_all_clinical=False,
            skip_sample_id_reset=False
        )

        # check that run log has 2 rows, and number of trial matches has not changed
        self.me.get_matches_for_all_trials()
        self.me.update_all_matches()
        trial_matches = list(self.me.db_ro.trial_match.find())
        run_log_trial_match = list(self.me.db_ro.run_log_trial_match.find({}))
        clinical_run_history_trial_match = list(
            self.me.db_ro.clinical_run_history_trial_match.find({'clinical_id': ObjectId("5d2799d86756630d8dd065b8")})
        )[0]
        assert len(trial_matches) == 3
        assert len(run_log_trial_match) == 3
        assert len(clinical_run_history_trial_match['run_history']) == 3


    def test_visualize_match_paths(self):
        # pygraphviz doesn't install easily on macOS so skip in that case.
        try:
            __import__('pygraphviz')
        except ImportError:
            print('WARNING: pygraphviz is not installed, skipping this test')
            return
        try:
            __import__('matplotlib')
        except ImportError:
            print('WARNING: matplotlib is not installed, skipping this test')
            return
        if not which('dot'):
            print('WARNING: executable "dot" not found, skipping this test')
            return

        fig_dir = f"/tmp/{os.urandom(10).hex()}"
        os.makedirs(fig_dir, exist_ok=True)
        unoverride_datetime()
        self._reset(
            do_reset_trial_matches=True,
            do_reset_trials=True,
            trials_to_load=['all_closed'],
            sample_ids={'5d2799cb6756630d8dd0621d'},
            visualize_match_paths=True,
            fig_dir=fig_dir,
            do_reset_time=False
        )
        assert self.me.db_rw.name == 'integration'
        self.me.get_matches_for_trial('10-001')
        for file_name in ['10-001-arm-212.png', '10-001-arm-222.png', '10-001-dose-312.png', '10-001-step-112.png']:
            assert os.path.exists(os.path.join(fig_dir, file_name))
            assert os.path.isfile(os.path.join(fig_dir, file_name))
            os.unlink(os.path.join(fig_dir, file_name))
        os.rmdir(fig_dir)

    def test_massive_match_clause(self):
        self._reset(do_reset_trials=True,
                    trials_to_load=['massive_match_clause'],
                    match_on_deceased=True,
                    match_on_closed=True,
                    num_workers=1)
        assert self.me.db_rw.name == 'integration'
        self.me.get_matches_for_all_trials()
        print(len(self.me._matches["11-113"]))

    def test_context_handler(self):
        self._reset(
            do_reset_trial_matches=True,
            do_reset_trials=True,
            trials_to_load=['all_closed']
        )
        assert self.me.db_rw.name == 'integration'
        with MatchEngine(sample_ids={'5d2799cb6756630d8dd0621d'},
                         protocol_nos={'10-001'},
                         match_on_closed=True,
                         match_on_deceased=True,
                         config='matchengine/config/dfci_config.json',
                         plugin_dir='matchengine/plugins/',
                         match_document_creator_class='DFCITrialMatchDocumentCreator',
                         num_workers=1) as me:
            me.get_matches_for_trial('10-001')
            assert not me._loop.is_closed()
        assert me._loop.is_closed()
        with open(os.devnull, 'w') as _f, redirect_stderr(_f):
            try:
                me.get_matches_for_trial('10-001')
                raise AssertionError("MatchEngine should have failed")
            except RuntimeError as e:
                print(f"Found expected RuntimeError {e}")

    def test_signatures(self):
        self._reset(do_reset_trials=True,
                    trials_to_load=['signatures'])
        assert self.me.db_rw.name == 'integration'
        self.me.get_matches_for_all_trials()
        assert len(self.me._matches['99-9999']['5d2799df6756630d8dd068ca']) == 5

    def test_tmb(self):
        self._reset(do_reset_trials=True,
                    trials_to_load=['tmb'],
                    report_all_clinical=False)
        assert self.me.db_rw.name == 'integration'
        self.me.get_matches_for_all_trials()
        assert len(self.me._matches['99-9999']['1d2799df4446699a8ddeeee']) == 4
        assert len(self.me._matches['99-9999']['4d2799df4446630a8dd068dd']) == 3
        assert len(self.me._matches['99-9999']['1d2799df4446699a8dd068ee']) == 4

    def test_unstructured_sv(self):
        self._reset(do_reset_trials=True,
                    trials_to_load=['unstructured_sv'])
        assert self.me.db_rw.name == 'integration'
        self.me.get_matches_for_all_trials()
        matches = self.me._matches['10-005']['1d2799df4446699a8ddeeee']
        assert matches[0]['genomic_alteration'] == 'EGFR Structural Variation'
        assert len(matches) == 1

    def test_structured_sv(self):
        self._reset(do_reset_trials=True,
                    trials_to_load=['structured_sv'],
                    report_all_clinical=False)
        assert self.me.db_rw.name == 'integration'
        self.me.get_matches_for_all_trials()
        assert '5d2799df6756630d8dd068c6' in self.me.matches['99-9999']
        assert len(self.me.matches['99-9999']['5d2799df6756630d8dd068c6']) == 44
        caught_matches = defaultdict(int)
        for match in self.me.matches['99-9999']['5d2799df6756630d8dd068c6']:
            alteration = match.get('genomic_alteration')
            if match['reason_type'] == 'genomic':
                if match['internal_id'] == 1234566:
                    assert (alteration
                            not in
                            {
                                "CLIP4-ALK Structural Variation",
                                "ALK-CLIP4 Structural Variation",
                                "EML4-EML4 Structural Variation"
                            })
                else:
                    caught_matches[alteration] += 1
        check_against = {
            '!TP53 Structural Variation': 12,
            'TFG-ALK Structural Variation': 2,
            'ALK-TFG Structural Variation': 2,
            'STRN-intergenic Structural Variation': 2,
            'RANDB2-ALK Structural Variation': 2,
            'ALK-RANDB2 Structural Variation': 2,
            'NPM1-intergenic Structural Variation': 6,
            'KIF5B-ALK Structural Variation': 2,
            'ALK-KIF5B Structural Variation': 2,
            'CLIP4-ALK Structural Variation': 1,
            'this should only match to any_gene-KRAS Structural Variation': 3,
            'KRAS-this should only match to any_gene Structural Variation': 3,
            'EML4-EML4 Structural Variation': 3,
            'this should only match to any_gene-this should only match to any gene Structural Variation': 1,
            'ALK-CLIP4 Structural Variation': 1
        }
        for alteration, count in caught_matches.items():
            assert check_against[alteration] == count

    def tearDown(self) -> None:
        if hasattr(self, 'me'):
            self.me.__exit__(None, None, None)
