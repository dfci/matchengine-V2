from unittest import TestCase
import csv
import os
import json

from matchengine import MatchEngine
from timetravel_and_override import set_static_date_time

import datetime


class IntegrationTestMatchengine(TestCase):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.first_run_done = False

    def _reset(self, **kwargs):
        if not self.first_run_done:
            set_static_date_time()
            self.me = MatchEngine(
                config={'trial_key_mappings': {},
                        'match_criteria': {'clinical': [],
                                           'genomic': [],
                                           'trial': ["protocol_no", "status"]},
                        'indices': {}},
                plugin_dir='tests/plugins'
            )
            assert self.me.db_rw.name == 'integration'
            self.first_run_done = True

        if kwargs.get('do_reset_trial_matches', False):
            self.me.db_rw.trial_match.drop()
            self.me.check_indices()

        if kwargs.get('reset_run_log', False):
            self.me.db_rw.run_log.drop()

        if kwargs.get('do_reset_trials', False):
            self.me.db_rw.trial.drop()
            trials_to_load = map(lambda x: os.path.join('tests', 'data', 'integration_trials', x + '.json'),
                                 kwargs.get('trials_to_load', list()))
            for trial_path in trials_to_load:
                with open(trial_path) as trial_file_handle:
                    trial = json.load(trial_file_handle)
                self.me.db_rw.trial.insert(trial)
        if kwargs.get('do_rm_clinical_run_history', False):
            self.me.db_rw.clinical.update({}, {"$unset": {"run_history": 1}}, multi=True)

        self.me.__exit__(None, None, None)

        self.me = MatchEngine(
            match_on_deceased=kwargs.get('match_on_deceased', True),
            match_on_closed=kwargs.get('match_on_closed', True),
            num_workers=kwargs.get('num_workers', 1),
            visualize_match_paths=kwargs.get('visualize_match_paths', False),
            config=kwargs.get('config', 'config/dfci_config.json'),
            plugin_dir=kwargs.get('plugin_dir', 'plugins/'),
            match_document_creator_class=kwargs.get('match_document_creator_class', "DFCITrialMatchDocumentCreator"),
            fig_dir=kwargs.get('fig_dir', '/tmp/'),
            protocol_nos=kwargs.get('protocol_nos', None),
            sample_ids=kwargs.get('sample_ids', None)
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

    def setUp(self) -> None:
        self._reset(do_reset_trials=True)

    def test__match_on_deceased_match_on_closed(self):
        assert self.me.db_rw.name == 'integration'
        self._reset(do_reset_trials=True,
                    trials_to_load=['all_closed', 'all_open', 'closed_dose', 'closed_step_arm'])
        self.me.get_matches_for_all_trials()
        assert len(set(self.me.matches.keys()).intersection({'10-001', '10-002', '10-003', '10-004'})) == 4
        assert len(self.me.matches['10-001']) == 5
        assert len(self.me.matches['10-002']) == 5
        assert len(self.me.matches['10-003']) == 5
        assert len(self.me.matches['10-004']) == 5

    def test__match_on_deceased(self):
        self._reset(match_on_deceased=True, match_on_closed=False,
                    do_reset_trials=True,
                    trials_to_load=['all_closed', 'all_open', 'closed_dose', 'closed_step_arm'])
        self.me.get_matches_for_all_trials()
        assert len(set(self.me.matches.keys()).intersection({'10-002', '10-003', '10-004'})) == 3
        assert len(self.me.matches['10-002']) == 5
        assert len(self.me.matches['10-003']) == 5
        assert len(self.me.matches['10-004']) == 0

    def test__match_on_closed(self):
        self._reset(match_on_deceased=False, match_on_closed=True,
                    do_reset_trials=True,
                    trials_to_load=['all_closed', 'all_open', 'closed_dose', 'closed_step_arm'])
        self.me.get_matches_for_all_trials()
        assert len(set(self.me.matches.keys()).intersection({'10-001', '10-002', '10-003', '10-004'})) == 4
        assert len(self.me.matches['10-001']) == 4
        assert len(self.me.matches['10-002']) == 4
        assert len(self.me.matches['10-003']) == 4
        assert len(self.me.matches['10-004']) == 4

    def test_update_trial_matches(self):
        self._reset(do_reset_trial_matches=True,
                    do_reset_trials=True,
                    trials_to_load=['all_closed', 'all_open', 'closed_dose', 'closed_step_arm'])
        self.me.get_matches_for_all_trials()
        for protocol_no in self.me.trials.keys():
            self.me.update_matches_for_protocol_number(protocol_no)
        assert self.me.db_ro.trial_match.count() == 80

    def test_wildcard_protein_change(self):
        self._reset(do_reset_trial_matches=True,
                    do_reset_trials=True,
                    trials_to_load=['wildcard_protein_found', 'wildcard_protein_not_found'])
        self.me.get_matches_for_all_trials()
        assert len(self.me.matches['10-005']) == 64
        assert len(self.me.matches['10-006']) == 0

    def test_match_on_individual_protocol_no(self):
        self._reset(do_reset_trial_matches=True,
                    do_reset_trials=True,
                    trials_to_load=['wildcard_protein_not_found'],
                    protocol_nos={'10-006'})
        self.me.get_matches_for_all_trials()
        assert len(self.me.matches.keys()) == 1
        assert len(self.me.matches['10-006']) == 0

    def test_match_on_individual_sample(self):
        self._reset(
            do_reset_trial_matches=True,
            do_reset_trials=True,
            trials_to_load=['all_closed', 'all_open', 'closed_dose', 'closed_step_arm'],
            sample_ids={'5d2799cb6756630d8dd0621d'}
        )
        self.me.get_matches_for_all_trials()
        assert len(self.me.matches['10-001']) == 1
        assert len(self.me.matches['10-002']) == 1
        assert len(self.me.matches['10-003']) == 1
        assert len(self.me.matches['10-004']) == 1

    def test_output_csv(self):
        self._reset(do_reset_trial_matches=True,
                    do_reset_trials=True,
                    trials_to_load=['all_closed', 'all_open', 'closed_dose', 'closed_step_arm'])
        self.me.get_matches_for_all_trials()
        filename = f'trial_matches_{datetime.datetime.now().strftime("%b_%d_%Y_%H:%M")}.csv'
        try:
            self.me.create_output_csv()
            assert os.path.exists(filename)
            assert os.path.isfile(filename)
            with open(filename) as csv_file_handle:
                csv_reader = csv.DictReader(csv_file_handle)
                fieldnames = set(csv_reader.fieldnames)
                rows = list(csv_reader)
            assert len(fieldnames.intersection(self.me._get_all_match_fieldnames())) == len(fieldnames)
            assert sum([1
                        for protocol_matches in self.me.matches.values()
                        for sample_matches in protocol_matches.values()
                        for _ in sample_matches]) == 80
            assert len(rows) == 80
            os.unlink(filename)
        except Exception as e:
            if os.path.exists(filename):
                os.unlink(filename)
            raise e

    def test_run_log(self):
        # run 1 - create matches and run log row
        self._reset(
            do_reset_trial_matches=True,
            do_reset_trials=True,
            trials_to_load=['run_log_arm_closed'],
            reset_run_log=True,
            match_on_closed=True,
            match_on_deceased=False,
            do_rm_clinical_run_history=True
        )
        self.me.get_matches_for_all_trials()
        self.me.update_all_matches()
        assert len(list(self.me.db_ro.trial_match.find())) == 5
        assert len(list(self.me.db_ro.run_log.find())) == 1

        # time travel to the future, of the current past. to the delorean we go...
        set_static_date_time(2001, 12, 9, 21)

        # simulate patients being updated
        sample_ids = ["5d2799df6756630d8dd068c9", "5d2799df6756630d8dd068ca"]
        self.me.db_rw.clinical.update({"SAMPLE_ID": {"$in": sample_ids}},
                                      {'$set': {"_updated": datetime.datetime(2001, 10, 10, 10)}}, multi=True)

        # run 2
        self._reset(
            do_reset_trial_matches=False,
            sample_ids=sample_ids,
            do_reset_trials=False,
            reset_run_log=False,
            match_on_closed=True,
            match_on_deceased=False,
            do_rm_clinical_run_history=False,
            do_reset_time=False
        )

        # check that run log has 2 rows, and number of trial matches has not changed
        self.me.get_matches_for_all_trials()
        self.me.update_all_matches()
        assert len(list(self.me.db_ro.run_log.find())) == 2
        assert len(list(self.me.db_ro.trial_match.find())) == 5

        # simulate a trial update
        self.me.db_rw.trial.update({"protocol_no": "10-002"}, {'$set': {"last_updated": "December 20, 2001"}})

        # move time forward a little
        set_static_date_time(2001, 12, 21, 21)

        # run 3
        self._reset(
            do_reset_trial_matches=False,
            do_reset_trials=False,
            reset_run_log=False,
            match_on_closed=True,
            match_on_deceased=False,
            do_rm_clinical_run_history=False,
            do_reset_time=False
        )

        self.me.get_matches_for_all_trials()
        self.me.update_all_matches()
        assert len(list(self.me.db_ro.run_log.find())) == 3
        the_chosen = list(self.me.db_ro.clinical.find({"SAMPLE_ID": {'$in': sample_ids}}))

        # these two sample ids should have 3 runs and most everyone else should have 2
        assert len(the_chosen) == 2
        assert len(the_chosen[0]['run_history']) == 3
        assert len(the_chosen[1]['run_history']) == 3

        the_others = list(
            self.me.db_rw.clinical.find({"SAMPLE_ID": {'$nin': sample_ids}, 'VITAL_STATUS': 'alive'}).limit(3))
        assert len(the_others[0]['run_history']) == 2
        assert len(the_others[1]['run_history']) == 2
        assert len(the_others[2]['run_history']) == 2

        # run 4
        self._reset(
            do_reset_trial_matches=False,
            do_reset_trials=False,
            reset_run_log=False,
            match_on_closed=True,
            match_on_deceased=False,
            do_rm_clinical_run_history=False,
            do_reset_time=False
        )

        self.me.get_matches_for_all_trials()
        self.me.update_all_matches()
        assert len(list(self.me.db_ro.run_log.find())) == 4

        # now the chose should have 3, and the others should also have 3
        the_chosen = list(self.me.db_ro.clinical.find({"SAMPLE_ID": {'$in': sample_ids}}))
        assert len(the_chosen[0]['run_history']) == 3
        assert len(the_chosen[1]['run_history']) == 3

        the_others = list(
            self.me.db_rw.clinical.find({"SAMPLE_ID": {'$nin': sample_ids}, 'VITAL_STATUS': 'alive'}).limit(3))
        assert len(the_others[0]['run_history']) == 3
        assert len(the_others[1]['run_history']) == 3

    def test_visualize_match_paths(self):
        fig_dir = f"/tmp/{os.urandom(10).hex()}"
        os.makedirs(fig_dir, exist_ok=True)
        self._reset(
            do_reset_trial_matches=True,
            do_reset_trials=True,
            trials_to_load=['all_closed'],
            sample_ids={'5d2799cb6756630d8dd0621d'},
            visualize_match_paths=True,
            fig_dir=fig_dir
        )
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
        self.me.get_matches_for_all_trials()
        print(len(self.me.matches["11-113"]))

    def test_context_handler(self):
        self._reset(
            do_reset_trial_matches=True,
            do_reset_trials=True,
            trials_to_load=['all_closed']
        )
        with MatchEngine(sample_ids={'5d2799cb6756630d8dd0621d'},
                         protocol_nos={'10-001'},
                         match_on_closed=True,
                         match_on_deceased=True,
                         config='config/dfci_config.json',
                         plugin_dir='plugins/',
                         match_document_creator_class='DFCITrialMatchDocumentCreator',
                         num_workers=1) as me:
            me.get_matches_for_trial('10-001')
            assert not me._loop.is_closed()
        assert me._loop.is_closed()
        try:
            me.get_matches_for_trial('10-001')
            raise AssertionError("MatchEngine should have failed")
        except RuntimeError as e:
            print(f"Found expected RuntimeError {e}")

    def test_signatures(self):
        assert self.me.db_rw.name == 'integration'
        self._reset(do_reset_trials=True,
                    trials_to_load=['signatures'])
        self.me.get_matches_for_all_trials()
        assert len(self.me.matches['99-9999']['5d2799df6756630d8dd068ca']) == 5

    def tearDown(self) -> None:
        self.me.__exit__(None, None, None)
