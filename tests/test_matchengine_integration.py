from unittest import TestCase
import gc
import csv
import os
import json

from matchengine import MatchEngine, main

import datetime


class StaticDatetime(datetime.datetime):
    @classmethod
    def now(cls, **kwargs):
        return datetime.datetime(2000, 7, 12, 9, 47, 40, 303620)


class StaticDate(datetime.date):
    @classmethod
    def today(cls):
        return datetime.date(2000, 7, 12)


# Exception raised when a GC reference for a base class being overridden is of a type where override logic is not known
class UnknownReferenceTypeForOverrideException(Exception):
    pass


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
        for override_class in [StaticDate, StaticDatetime]:
            # Then we iterate over each base class for the override class - at the time of writing this is just
            # datetime.datetime for StaticDatetime and StaticDate for datetime.date
            for base_class in override_class.__bases__:
                # For each base class, we get all objects referring to it, via the garbage collector
                for referrer in gc.get_referrers(base_class):
                    # Check to see if the referrer is mutable (otherwise performing an override won't do anything -
                    # any immutable object with a reference will not be overridden.
                    # TODO: and recursive override logic to handle referrers nested in immutable objects
                    if getattr(referrer, '__hash__', None) is None:
                        # If the referrer is a dict, then the reference is present as a value in the dict
                        if isinstance(referrer, dict):
                            # iterate over each key in the referrer
                            for k in list(referrer.keys()):
                                # check to see if the value associated with that key is the base class
                                if referrer[k] is base_class:
                                    # if it is, then re-associate the key with the the override class
                                    referrer[k] = override_class
                        # All other mutable types not caught above have not had the overrides implemented,
                        # so raise an Exception to alert of this fact
                        else:
                            raise UnknownReferenceTypeForOverrideException(
                                (f"ERROR: Found a hashable object of type {type(referrer)} "
                                 f"referring to {base_class} "
                                 f"while performing overrides for {override_class} "
                                 f"please implement logic for handling overriding references from this type.")
                            )

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
