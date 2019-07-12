from unittest import TestCase
import gc
import glob
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

    def _reset(self, **kwargs):
        if hasattr(self, 'me'):
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
            use_run_log=kwargs.get('use_run_log', False)
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

        if kwargs.get('do_reset_trials', False):
            self.me.db_rw.trial.drop()

            trials_to_load = glob.glob(os.path.join("tests", "data", "integration_trials", "*.json"))
            for trial_path in trials_to_load:
                with open(trial_path) as trial_file_handle:
                    trial = json.load(trial_file_handle)
                self.me.db_rw.trial.insert(trial)
            self.first_run_done = True

    def setUp(self) -> None:
        self._reset(do_reset_trials=True)

    def test__match_on_deceased_match_on_closed(self):
        self._reset()
        self.me.get_matches_for_all_trials()
        assert len(set(self.me.matches.keys()).intersection({'10-001', '10-002', '10-003', '10-004'})) == 4
        assert len(self.me.matches['10-001']) == 5
        assert len(self.me.matches['10-002']) == 5
        assert len(self.me.matches['10-003']) == 5
        assert len(self.me.matches['10-004']) == 5

    def test__match_on_deceased(self):
        self._reset(match_on_deceased=True, match_on_closed=False)
        self.me.get_matches_for_all_trials()
        assert len(set(self.me.matches.keys()).intersection({'10-002', '10-003', '10-004'})) == 3
        assert len(self.me.matches['10-002']) == 5
        assert len(self.me.matches['10-003']) == 5
        assert len(self.me.matches['10-004']) == 0

    def test__match_on_closed(self):
        self._reset(match_on_deceased=False, match_on_closed=True)
        self.me.get_matches_for_all_trials()
        assert len(set(self.me.matches.keys()).intersection({'10-001', '10-002', '10-003', '10-004'})) == 4
        assert len(self.me.matches['10-001']) == 4
        assert len(self.me.matches['10-002']) == 4
        assert len(self.me.matches['10-003']) == 4
        assert len(self.me.matches['10-004']) == 4

    def test_update_trial_matches(self):
        self._reset()
        self.me.get_matches_for_all_trials()
        for protocol_no in self.me.trials.keys():
            self.me.update_matches_for_protocol_number(protocol_no)
        assert self.me.db_ro.trial_match.count() == 80

    def tearDown(self) -> None:
        self.me.__exit__(None, None, None)
