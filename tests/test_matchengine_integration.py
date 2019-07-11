from unittest import TestCase

from matchengine import MatchEngine


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
        pass

    def test_basic(self):
        assert True

    pass
