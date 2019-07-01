import glob
import json
from unittest import TestCase
from match_criteria_transform import MatchCriteriaTransform
from matchengine import MatchEngine, log
from matchengine_types import MatchClauseData, ParentPath, MatchClauseLevel


class TestMatchEngine(TestCase):

    def setUp(self) -> None:
        """init matchengine without running __init__ since tests will need to instantiate various values individually"""
        self.me = MatchEngine.__new__(MatchEngine)

        self.me.plugin_dir = 'tests/plugins'
        self.me.match_document_creator_class = 'TestTrialMatchDocumentCreator'
        with open('tests/config.json') as config_file_handle:
            self.config = json.load(config_file_handle)

        self.me.match_criteria_transform = MatchCriteriaTransform(self.config)
        pass

    def test__find_plugins(self):
        """Verify functions inside external config files are reachable within the Matchengine class"""
        old_create_trial_matches = self.me.create_trial_matches
        self.me._find_plugins()
        assert hasattr(self.me, 'create_trial_matches')
        assert id(self.me.create_trial_matches) != old_create_trial_matches

    def test_extract_match_clauses_from_trial(self):
        self.me.trials = dict()
        self.me.match_on_closed = False
        with open('./tests/data/trials/11-111.json') as f:
            data = json.load(f)
            match_clause = data['treatment_list']['step'][0]['arm'][0]['match'][0]
            self.me.trials['11-111'] = data

        extracted = next(self.me.extract_match_clauses_from_trial('11-111'))
        assert extracted.match_clause[0]['and'] == match_clause['and']
        assert extracted.parent_path == ('treatment_list', 'step', 0, 'arm', 0, 'match')
        assert extracted.match_clause_level == 'arm'
        assert extracted.code == 'EXP 3'

    def test_create_match_tree(self):
        self.me.trials = dict()
        self.me.visualize_match_paths = False
        for file in glob.glob('./tests/data/ctml_boolean_cases/*.json'):
            with open(file) as f:
                data = json.load(f)
                trial = [data]
                self.me.trials[file] = trial

        for trial in self.me.trials:
            me_trial = self.me.trials[trial]
            match_tree = self.me.create_match_tree(MatchClauseData(match_clause=me_trial,
                                                                   internal_id='123',
                                                                   code='456',
                                                                   coordinating_center='The Death Star',
                                                                   status='Open to Accrual',
                                                                   parent_path=ParentPath(()),
                                                                   match_clause_level=MatchClauseLevel('arm'),
                                                                   match_clause_additional_attributes={},
                                                                   protocol_no='12-345'))
            pass

    def test_get_match_paths(self):
        self.fail()

    def test_translate_match_path(self):
        self.fail()
