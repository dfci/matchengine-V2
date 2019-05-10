from collections import deque
from typing import Any, Tuple, Union, NewType, List, Dict
from MatchCriteriaTransform import MatchCriteriaTransform

import networkx as nx
import pymongo
import json

Trial = NewType("Trial", dict)
ParentPath = NewType("ParentPath", Tuple[Union[str, int]])
MatchClause = NewType("MatchClause", List[Dict[str, Any]])
MatchTree = NewType("MatchTree", nx.DiGraph)
MatchCriterion = NewType("MatchPath", List[Dict[str, Any]])
MongoQuery = NewType("MongoQuery", dict)
NodeID = NewType("NodeID", int)


class MongoDBConnection(object):
    SECRETS = {
        "MONGO_HOST": "immuno5.dfci.harvard.edu",
        "MONGO_PORT": 27019,
        "MONGO_DBNAME": "matchminer",
        "MONGO_AUTH_SOURCE": "admin",
        "MONGO_RO_USERNAME": "mmReadOnlyUser",
        "MONGO_RO_PASSWORD": "awifbv4ouwnvkjsdbff"
    }
    uri = "mongodb://{username}:{password}@{hostname}:{port}/{db}?authSource=admin&replicaSet=rs0"
    read_only = None
    db = None
    client = None

    def __init__(self, read_only=True, uri=None, db=None):
        self.read_only = read_only
        self.db = db if db is not None else self.SECRETS['MONGO_DBNAME']
        if uri is not None:
            self.uri = uri

    def __enter__(self):
        self.client = pymongo.MongoClient(
            self.uri.format(username=self.SECRETS["MONGO_RO_USERNAME"],
                            password=self.SECRETS["MONGO_RO_PASSWORD"],
                            hostname=self.SECRETS["MONGO_HOST"],
                            port=self.SECRETS["MONGO_PORT"],
                            db=self.db))
        return self.client[self.db]

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.client.close()


def find_matches(sample_ids: list = None, protocol_nos: list = None) -> dict:
    trials: List[Trial] = list()
    trial_find_query = dict()
    with open("config/config.json") as config_file_handle:
        config = json.load(config_file_handle)
    match_criteria_transform = MatchCriteriaTransform(config)
    if protocol_nos is not None:
        trial_find_query['$in'] = [protocol_no for protocol_no in protocol_nos]
    with MongoDBConnection(read_only=True) as db:
        trials = [result for result in db.trial.find(trial_find_query)]
    for trial in trials:
        match_clauses = extract_match_clauses_from_trial(trial)
        for parent_path, match_clause in match_clauses:
            match_tree = create_match_tree(match_clause)
            match_paths = get_match_paths(match_tree)
            mongo_queries = [translate_match_path(parent_path, match_path, match_criteria_transform) for match_path in
                             match_paths]
            print(mongo_queries)


def extract_match_clauses_from_trial(trial: Trial) -> List[Tuple[ParentPath, MatchClause]]:
    process_q = deque()
    match_clauses = list()
    for k, v in trial.items():
        if k == 'match':
            parent_path = ParentPath(tuple())
            match_clauses.append((parent_path, v))
        else:
            process_q.append((tuple(), k, v))
    while process_q:
        path, parent_key, parent_value = process_q.pop()
        if isinstance(parent_value, dict):
            for inner_key, inner_value in parent_value.items():
                if inner_key == 'match':
                    parent_path = ParentPath(path + (parent_key, inner_key))
                    match_clauses.append((parent_path, inner_value))
                else:
                    process_q.append((path + (parent_key,), inner_key, inner_value))
        elif isinstance(parent_value, list):
            for index, item in enumerate(parent_value):
                process_q.append((path + (parent_key,), index, item))
    return match_clauses


def create_match_tree(match_clause: MatchClause) -> MatchTree:
    process_q: deque[Tuple[NodeID, Dict[str, Any]]] = deque()
    graph = nx.DiGraph()
    node_id: NodeID = 1
    graph.add_node(0)  # root node is 0
    graph.nodes[0]['criteria_list'] = list()
    for item in match_clause:
        process_q.append((NodeID(0), item))
    while process_q:
        parent_id, values = process_q.pop()
        parent_is_or = True if graph.nodes[parent_id].setdefault('is_or', False) else False
        for label, value in values.items():  # label is 'and', 'or', 'genomic' or 'clinical'
            if label == 'and':
                for item in value:
                    process_q.append((parent_id, item))
            elif label == "or":
                graph.add_edges_from([(parent_id, node_id)])
                graph.nodes[node_id]['criteria_list'] = list()
                graph.nodes[node_id]['is_or'] = True
                for item in value:
                    process_q.append((node_id, item))
                node_id += 1
            elif parent_is_or:
                graph.add_edges_from([(parent_id, node_id)])
                graph.nodes[node_id]['criteria_list'] = [values]
                node_id += 1
            else:
                graph.nodes[parent_id]['criteria_list'].append({label: value})
    return MatchTree(graph)


def get_match_paths(match_tree: MatchTree) -> List[MatchCriterion]:
    leaves = list()
    match_paths: List[MatchCriterion] = list()
    for node in match_tree.nodes:
        if match_tree.out_degree(node) == 0:
            leaves.append(node)
    for leaf in leaves:
        path = nx.shortest_path(match_tree, 0, leaf) if leaf != 0 else [leaf]
        match_path = MatchCriterion(list())
        for node in path:
            match_path.extend(match_tree.nodes[node]['criteria_list'])
        match_paths.append(match_path)
    return match_paths


def translate_match_path(path: ParentPath,
                         match_criterion: MatchCriterion,
                         match_criteria_transformer: MatchCriteriaTransform) -> MongoQuery:
    and_list = list()
    for criteria in match_criterion:
        for genomic_or_clinical, values in criteria.items():
            for trial_key, trial_value in values.items():
                trial_key_settings = match_criteria_transformer.trial_key_mappings[genomic_or_clinical].setdefault(
                    trial_key.upper(),
                    dict())
                sample_value_function_name = trial_key_settings.setdefault('sample_value', 'nomap')
                sample_function = MatchCriteriaTransform.__dict__[sample_value_function_name]
                args = dict(sample_key=trial_key.upper(),
                            trial_value=trial_value,
                            parent_path=path,
                            trial_path=genomic_or_clinical,
                            trial_key=trial_key)
                args.update(trial_key_settings)
                and_query = sample_function(match_criteria_transformer, **args)
                and_list.append(and_query)
    return MongoQuery({"$and": and_list})


if __name__ == "__main__":
    find_matches()
