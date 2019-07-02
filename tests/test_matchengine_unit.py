import glob
import json
import os
from unittest import TestCase

import networkx as nx

from match_criteria_transform import MatchCriteriaTransform
from matchengine import MatchEngine, log, ComparableDict
from matchengine_types import MatchClauseData, ParentPath, MatchClauseLevel, MatchClause, MatchCriteria, MatchCriterion


class TestMatchEngine(TestCase):

    def setUp(self) -> None:
        """init matchengine without running __init__ since tests will need to instantiate various values individually"""
        self.me = MatchEngine.__new__(MatchEngine)

        assert isinstance(self.me.create_trial_matches({}), dict)
        self.me.plugin_dir = 'tests/plugins'
        self.me.match_document_creator_class = 'TestTrialMatchDocumentCreator'
        self.me.visualize_match_paths = False
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
        for file in glob.glob('./tests/data/ctml_boolean_cases/*.json'):
            with open(file) as f:
                data = json.load(f)
                trial = [data]
                self.me.trials[file] = trial

        with open('./tests/data/create_match_tree_expected.json') as f:
            test_cases = json.load(f)

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
            test_case = test_cases[os.path.basename(trial)]
            assert len(test_case["nodes"]) == len(match_tree.nodes)
            for test_case_key in test_case.keys():
                if test_case_key == "nodes":
                    for node_id, node_attrs in test_case[test_case_key].items():
                        graph_node = match_tree.nodes[int(node_id)]
                        assert len(node_attrs) == len(graph_node)
                        assert ComparableDict(node_attrs).hash() == ComparableDict(graph_node).hash()
                else:
                    for test_item, graph_item in zip(test_case[test_case_key], getattr(match_tree, test_case_key)):
                        for idx, test_item_part in enumerate(test_item):
                            assert test_item_part == graph_item[idx]

    def test_get_match_paths(self):
        self.me.trials = dict()
        for file in glob.glob('./tests/data/ctml_boolean_cases/*.json'):
            with open(file) as f:
                data = json.load(f)
                trial = [data]
                self.me.trials[file] = trial
        with open("./tests/data/get_match_paths_expected.json") as f:
            test_cases = json.load(f)
        for trial in self.me.trials:
            filename = os.path.basename(trial)
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
            match_paths = list(self.me.get_match_paths(match_tree))
            for test_case, match_path in zip(test_cases[filename], match_paths):
                assert match_path.hash() == test_case["hash"]
                for test_case_criteria_idx, test_case_criteria in enumerate(test_case["criteria_list"]):
                    match_path_criteria = match_path.criteria_list[test_case_criteria_idx]
                    assert test_case_criteria["depth"] == match_path_criteria.depth
                    for inner_test_case_criteria, inner_match_path_criteria in zip(test_case_criteria["criteria"],
                                                                                   match_path_criteria.criteria):
                        assert ComparableDict(inner_test_case_criteria).hash() == ComparableDict(
                            inner_match_path_criteria).hash()

    def test_translate_match_path(self):
        self.me.trials = dict()
        self.me._find_plugins()
        match_clause_data = MatchClauseData(match_clause=MatchClause([{}]),
                                            internal_id='123',
                                            code='456',
                                            coordinating_center='The Death Star',
                                            status='Open to Accrual',
                                            parent_path=ParentPath(()),
                                            match_clause_level=MatchClauseLevel('arm'),
                                            match_clause_additional_attributes={},
                                            protocol_no='12-345')
        match_path = self.me.translate_match_path(match_clause_data=match_clause_data,
                                                  match_criterion=MatchCriterion([MatchCriteria({},0)]))
        assert len(match_path.clinical) == 0
        assert len(match_path.genomic) == 0


