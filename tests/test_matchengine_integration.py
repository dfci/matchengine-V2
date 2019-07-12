from unittest import TestCase
import gc

from matchengine import MatchEngine

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
    def setUp(self) -> None:
        self.me = MatchEngine(
            match_on_deceased=True,
            match_on_closed=True,
            num_workers=1,
            visualize_match_paths=True,
            config='config/dfci_config.json',
            plugin_dir='plugins',
            use_run_log=False
        )

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
        pass

    def test_basic(self):
        assert True

    pass
