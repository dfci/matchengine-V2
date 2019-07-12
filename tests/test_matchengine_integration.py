from unittest import TestCase
import gc

from matchengine import MatchEngine

import datetime


# old_datetime_datetime_now = datetime.datetime.now
class StaticDatetime(datetime.datetime):
    @classmethod
    def now(cls, **kwargs):
        return datetime.datetime(2000, 7, 12, 9, 47, 40, 303620)


# old_datetime_datetime_now = datetime.datetime.now
class StaticDate(datetime.date):
    @classmethod
    def today(cls):
        return datetime.date(2000, 7, 12)


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
        self.me.match_criteria_transform.transform.static_date = StaticDate
        for override_class in [StaticDate, StaticDatetime]:
            for base_class in override_class.__bases__:
                for referrer in gc.get_referrers(base_class):
                    if getattr(referrer, '__hash__', None) is None:
                        if isinstance(referrer, dict):
                            for k in list(referrer.keys()):
                                if referrer[k] is base_class:
                                    referrer[k] = override_class
        pass

    def test_basic(self):
        assert True

    pass
