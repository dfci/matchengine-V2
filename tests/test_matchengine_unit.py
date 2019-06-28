import json
from unittest import TestCase

from TrialMatchDocumentCreator import TrialMatchDocumentCreator
from match_criteria_transform import MatchCriteriaTransform
from matchengine import MatchEngine


class TestMatchEngine(TestCase):

    def setUp(self) -> None:
        # init matchengine without any args since tests themselves are testing the args
        self.me = MatchEngine.__new__(MatchEngine)

        self.me.plugin_dir = 'tests/plugins'
        self.me.match_document_creator_class = 'TestTrialMatchDocumentCreator'
        with open('tests/config.json') as config_file_handle:
            self.config = json.load(config_file_handle)

        self.me.match_criteria_transform = MatchCriteriaTransform(self.config)
        pass

    def test__find_plugins(self):
        old_create_trial_matches = self.me.create_trial_matches
        self.me._find_plugins()
        assert hasattr(self.me, 'create_trial_matches')
        assert id(self.me.create_trial_matches) != old_create_trial_matches


    def test__async_init(self):
        self.fail()

    def test__execute_clinical_queries(self):
        self.fail()

    def test__execute_genomic_queries(self):
        self.fail()

    def test__run_query(self):
        self.fail()

    def test__queue_worker(self):
        self.fail()

    def test_extract_match_clauses_from_trial(self):
        self.fail()

    def test_create_match_tree(self):
        self.fail()

    def test_get_match_paths(self):
        self.fail()

    def test_translate_match_path(self):
        self.fail()

    def test_update_matches_for_protocol_number(self):
        self.fail()

    def test_update_all_matches(self):
        self.fail()

    def test__async_update_matches_by_protocol_no(self):
        self.fail()

    def test_get_matches_for_all_trials(self):
        self.fail()

    def test_get_matches_for_trial(self):
        self.fail()

    def test__async_get_matches_for_trial(self):
        self.fail()

    def test_get_clinical_ids_from_sample_ids(self):
        self.fail()

    def test_get_trials(self):
        self.fail()

    def test__perform_db_call(self):
        self.fail()

    def test_get_sort_order(self):
        self.fail()

    def test_create_trial_matches(self):
        self.fail()
