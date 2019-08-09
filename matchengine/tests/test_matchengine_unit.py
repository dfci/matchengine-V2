import glob
import json
import os
from unittest import TestCase

from matchengine.internals.engine import MatchEngine
from matchengine.internals.match_criteria_transform import MatchCriteriaTransform
from matchengine.internals.match_translator import create_match_tree, get_match_paths, extract_match_clauses_from_trial, \
    translate_match_path
from matchengine.internals.typing.matchengine_types import MatchClause, MatchCriteria, MatchCriterion
from matchengine.internals.typing.matchengine_types import MatchClauseData, ParentPath, MatchClauseLevel
from matchengine.internals.utilities.object_comparison import nested_object_hash
from matchengine.internals.utilities.utilities import find_plugins


class TestMatchEngine(TestCase):

    def setUp(self) -> None:
        """init matchengine without running __init__ since tests will need to instantiate various values individually"""
        self.me = MatchEngine.__new__(MatchEngine)

        assert self.me.create_trial_matches({}).__class__ is dict
        self.me.plugin_dir = 'matchengine/tests/plugins'
        self.me.match_document_creator_class = 'TestTrialMatchDocumentCreator'
        self.me.visualize_match_paths = False
        with open('matchengine/tests/config.json') as config_file_handle:
            self.config = json.load(config_file_handle)

        self.me.match_criteria_transform = MatchCriteriaTransform(self.config)

    def test_find_plugins(self):
        """Verify functions inside external config files are reachable within the Matchengine class"""
        old_create_trial_matches = self.me.create_trial_matches
        find_plugins(self.me)
        assert hasattr(self.me, 'create_trial_matches')
        assert id(self.me.create_trial_matches) != old_create_trial_matches
        blank_trial_match = self.me.create_trial_matches({})
        assert blank_trial_match.__class__ is dict and not blank_trial_match

    def test_query_transform(self):
        find_plugins(self.me)

        assert hasattr(self.me.match_criteria_transform.transform, 'is_negate')
        assert getattr(self.me.match_criteria_transform.transform, 'is_negate')('this') == ('this', False)
        assert getattr(self.me.match_criteria_transform.transform, 'is_negate')('!this') == ('this', True)
        assert getattr(self.me.match_criteria_transform.transform, 'is_negate')('!') == (str(), True)
        assert getattr(self.me.match_criteria_transform.transform, 'is_negate')('') == (str(), False)

        transform_args = {
            'trial_path': 'test',
            'trial_key': 'test',
            'trial_value': 'test',
            'sample_key': 'test',
            'file': 'matchengine/tests/data/external_file_mapping_test.json'
        }

        assert hasattr(self.me.match_criteria_transform.query_transformers, 'nomap')
        query_transform_result = getattr(self.me.match_criteria_transform.query_transformers,
                                         'nomap')(**transform_args).results[0]
        nomap_ret, nomap_no_negate = query_transform_result.query, query_transform_result.negate
        assert len(nomap_ret) == 1 and nomap_ret['test'] == 'test' and not nomap_no_negate

        assert hasattr(self.me.match_criteria_transform.query_transformers, 'external_file_mapping')
        query_transform_result = getattr(self.me.match_criteria_transform.query_transformers,
                                         'external_file_mapping')(**transform_args).results[0]
        ext_f_map_ret, ext_f_map_no_negate = query_transform_result.query, query_transform_result.negate
        assert len(ext_f_map_ret) == 1 and not ext_f_map_no_negate
        assert 'test' in ext_f_map_ret and '$in' in ext_f_map_ret['test']
        assert all(map(lambda x: x[0] == x[1],
                       zip(ext_f_map_ret['test']['$in'],
                           ['option_1', 'option_2', 'option_3'])))
        query_transform_result = getattr(
            self.me.match_criteria_transform.query_transformers,
            'external_file_mapping')(**dict(transform_args,
                                            **{'trial_value': '!test2'})).results[0]
        ext_f_map_ret_single, ext_f_map_no_negate_single = query_transform_result.query, query_transform_result.negate
        assert len(ext_f_map_ret) == 1 and ext_f_map_no_negate_single
        assert 'test' in ext_f_map_ret_single and ext_f_map_ret_single['test'].__class__ is str
        assert ext_f_map_ret_single['test'] == 'option_4'

        assert hasattr(self.me.match_criteria_transform.query_transformers, 'to_upper')
        query_transform_result = getattr(self.me.match_criteria_transform.query_transformers,
                                         'to_upper')(**transform_args).results[0]
        to_upper_ret, to_upper_no_negate = query_transform_result.query, query_transform_result.negate
        assert len(to_upper_ret) == 1 and not to_upper_no_negate
        assert 'test' in ext_f_map_ret and to_upper_ret['test'] == 'TEST'

    def test_extract_match_clauses_from_trial(self):
        self.me.trials = dict()
        self.me.match_on_closed = False
        with open('./matchengine/tests/data/trials/11-111.json') as f:
            data = json.load(f)
            match_clause = data['treatment_list']['step'][0]['arm'][0]['match'][0]
            self.me.trials['11-111'] = data

        extracted = next(extract_match_clauses_from_trial(self.me, '11-111'))
        assert extracted.match_clause[0]['and'] == match_clause['and']
        assert extracted.parent_path == ('treatment_list', 'step', 0, 'arm', 0, 'match')
        assert extracted.match_clause_level == 'arm'
        assert extracted.code == 'EXP 3'

    def test_create_match_tree(self):
        self.me.trials = dict()
        for file in glob.glob('./matchengine/tests/data/ctml_boolean_cases/*.json'):
            with open(file) as f:
                data = json.load(f)
                trial = [data]
                self.me.trials[file] = trial

        with open('./matchengine/tests/data/create_match_tree_expected.json') as f:
            test_cases = json.load(f)

        for trial in self.me.trials:
            me_trial = self.me.trials[trial]
            match_tree = create_match_tree(self.me, MatchClauseData(match_clause=me_trial,
                                                                    internal_id='123',
                                                                    code='456',
                                                                    coordinating_center='The Death Star',
                                                                    status='Open to Accrual',
                                                                    parent_path=ParentPath(()),
                                                                    match_clause_level=MatchClauseLevel('arm'),
                                                                    match_clause_additional_attributes={},
                                                                    protocol_no='12-345',
                                                                    is_suspended=True))
            test_case = test_cases[os.path.basename(trial)]
            assert len(test_case["nodes"]) == len(match_tree.nodes)
            for test_case_key in test_case.keys():
                if test_case_key == "nodes":
                    for node_id, node_attrs in test_case[test_case_key].items():
                        graph_node = match_tree.nodes[int(node_id)]
                        assert len(node_attrs) == len(graph_node)
                        assert nested_object_hash(node_attrs)== nested_object_hash(graph_node)
                else:
                    for test_item, graph_item in zip(test_case[test_case_key], getattr(match_tree, test_case_key)):
                        for idx, test_item_part in enumerate(test_item):
                            assert test_item_part == graph_item[idx]

    def test_get_match_paths(self):
        self.me.trials = dict()
        for file in glob.glob('./matchengine/tests/data/ctml_boolean_cases/*.json'):
            with open(file) as f:
                data = json.load(f)
                trial = [data]
                self.me.trials[file] = trial
        with open("./matchengine/tests/data/get_match_paths_expected.json") as f:
            test_cases = json.load(f)
        for trial in self.me.trials:
            filename = os.path.basename(trial)
            me_trial = self.me.trials[trial]
            match_tree = create_match_tree(self.me, MatchClauseData(match_clause=me_trial,
                                                                    internal_id='123',
                                                                    code='456',
                                                                    coordinating_center='The Death Star',
                                                                    status='Open to Accrual',
                                                                    parent_path=ParentPath(()),
                                                                    match_clause_level=MatchClauseLevel('arm'),
                                                                    match_clause_additional_attributes={},
                                                                    is_suspended=True,
                                                                    protocol_no='12-345'))
            match_paths = list(get_match_paths(match_tree))
            for test_case, match_path in zip(test_cases[filename], match_paths):
                for test_case_criteria_idx, test_case_criteria in enumerate(test_case["criteria_list"]):
                    match_path_criteria = match_path.criteria_list[test_case_criteria_idx]
                    assert test_case_criteria["depth"] == match_path_criteria.depth
                    for inner_test_case_criteria, inner_match_path_criteria in zip(test_case_criteria["criteria"],
                                                                                   match_path_criteria.criteria):
                        assert nested_object_hash(inner_test_case_criteria)== nested_object_hash(
                            inner_match_path_criteria)

    def test_translate_match_path(self):
        self.me.trials = dict()
        find_plugins(self.me)
        match_clause_data = MatchClauseData(match_clause=MatchClause([{}]),
                                            internal_id='123',
                                            code='456',
                                            coordinating_center='The Death Star',
                                            status='Open to Accrual',
                                            parent_path=ParentPath(()),
                                            match_clause_level=MatchClauseLevel('arm'),
                                            match_clause_additional_attributes={},
                                            protocol_no='12-345',
                                            is_suspended=True)
        match_paths = translate_match_path(self.me, match_clause_data=match_clause_data,
                                           match_criterion=MatchCriterion([MatchCriteria({}, 0, 0)]))
        assert len(match_paths.clinical) == 0
        assert len(match_paths.genomic) == 0

    def test_comparable_dict(self):
        assert nested_object_hash({})== nested_object_hash({})
        assert nested_object_hash({"1": "1",
                               "2": "2"})== nested_object_hash({"2": "2",
                                                                    "1": "1"})
        assert nested_object_hash({"1": [{}, {2: 3}],
                               "2": "2"})== nested_object_hash({"2": "2",
                                                                    "1": [{2: 3}, {}]})
        assert nested_object_hash({"1": [{'set': {1, 2, 3}}, {2: 3}],
                               "2": "2"})== nested_object_hash({"2": "2",
                                                                    "1": [{2: 3}, {'set': {3, 1, 2}}]})
        assert nested_object_hash({
            1: {
                2: [
                    {
                        3: 4,
                        5: {6, 7}
                    }
                ]
            },
            "4": [9, 8]
        }) != nested_object_hash({
            1: {
                2: [
                    {
                        3: 4,
                        9: {6, 7}
                    }
                ]
            },
            "4": [9, 8]
        })
