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
            self.me = MatchEngine(
                config={'trial_key_mappings': {},
                        'match_criteria': {'clinical': [],
                                           'genomic': [],
                                           'trial': ["protocol_no", "status"]}},
                plugin_dir='tests/plugins'
            )
            assert self.me.db_rw.name == 'integration'
            self.first_run_done = True

        if kwargs.get('do_reset_trial_matches', False):
            self.me.db_rw.trial_match.drop()
            self.me.check_indices()

        if kwargs.get('do_reset_trials', False):
            self.me.db_rw.trial.drop()
            trials_to_load = map(lambda x: os.path.join('tests', 'data', 'integration_trials', x + '.json'),
                                 kwargs.get('trials_to_load', list()))
            for trial_path in trials_to_load:
                with open(trial_path) as trial_file_handle:
                    trial = json.load(trial_file_handle)
                self.me.db_rw.trial.insert(trial)

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
        if kwargs.get('date_args', False):
            set_static_date_time(**kwargs['date_args'])
        else:
            set_static_date_time()

    def setUp(self) -> None:
        self._reset(do_reset_trials=True)

    def test__match_on_deceased_match_on_closed(self):
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

    def tearDown(self) -> None:
        self.me.__exit__(None, None, None)
