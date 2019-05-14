from collections import deque, defaultdict
from typing import Any, Tuple, Union, NewType, List, Dict, AsyncGenerator, Generator

from bson import ObjectId

from MatchCriteriaTransform import MatchCriteriaTransform

import networkx as nx
import pymongo
import pymongo.database
import json
# import asyncio
import logging

# import motor.motor_asyncio

logging.basicConfig(level=logging.INFO)
log = logging.getLogger('matchengine')

Trial = NewType("Trial", dict)
ParentPath = NewType("ParentPath", Tuple[Union[str, int]])
MatchClause = NewType("MatchClause", List[Dict[str, Any]])
MatchTree = NewType("MatchTree", nx.DiGraph)
MatchCriterion = NewType("MatchPath", List[Dict[str, Any]])
MultiCollectionQuery = NewType("MongoQuery", dict)
NodeID = NewType("NodeID", int)


class MongoDBConnection(object):
    SECRETS = {
        "MONGO_HOST": "***REMOVED***",
        "MONGO_PORT": 27019,
        "MONGO_DBNAME": "matchminer",
        "MONGO_AUTH_SOURCE": "admin",
        "MONGO_RO_USERNAME": "***REMOVED***",
        "MONGO_RO_PASSWORD": "***REMOVED***"
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
    with open("config/config.json") as config_file_handle:
        config = json.load(config_file_handle)
    match_criteria_transform = MatchCriteriaTransform(config)
    with MongoDBConnection(read_only=True) as db:
        for trial in get_trials(db, protocol_nos):
            log.info("Protocol No: {}".format(trial["protocol_no"]))
            for parent_path, match_clause in extract_match_clauses_from_trial(trial):
                for match_path in get_match_paths(create_match_tree(match_clause)):
                    try:
                        query = add_sample_ids_to_query(translate_match_path(parent_path,
                                                                             match_path,
                                                                             match_criteria_transform),
                                                        sample_ids,
                                                        match_criteria_transform)
                        results = run_query(db, match_criteria_transform, query)
                        # log.info(
                        #     "query: {}".format(query))
                        if results:
                            log.info(
                                "Protocol No: {}, parent_path: {}, match_path: {}, query: {}, len(results): {}".format(
                                    trial['protocol_no'], parent_path, match_path, query, len(results)))
                    except Exception as e:
                        logging.error("ERROR: {}".format(e))
                        raise e


def get_trials(db: pymongo.database.Database,
               protocol_nos: list = None) -> Generator[Trial, None, None]:
    trial_find_query = dict()
    if protocol_nos is not None:
        trial_find_query['protocol_no'] = {"$in": [protocol_no for protocol_no in protocol_nos]}
    for trial in db.trial.find(trial_find_query):
        yield Trial(trial)


def extract_match_clauses_from_trial(trial: Trial) -> Generator[List[Tuple[ParentPath, MatchClause]], None, None]:
    process_q = deque()
    for k, v in trial.items():
        if k == 'match':
            parent_path = ParentPath(tuple())
            yield parent_path, v
        else:
            process_q.append((tuple(), k, v))
    while process_q:
        path, parent_key, parent_value = process_q.pop()
        if isinstance(parent_value, dict):
            for inner_key, inner_value in parent_value.items():
                if inner_key == 'match':
                    parent_path = ParentPath(path + (parent_key, inner_key))
                    yield parent_path, inner_value
                else:
                    process_q.append((path + (parent_key,), inner_key, inner_value))
        elif isinstance(parent_value, list):
            for index, item in enumerate(parent_value):
                process_q.append((path + (parent_key,), index, item))


def create_match_tree(match_clause: MatchClause) -> MatchTree:
    process_q: deque[Tuple[NodeID, Dict[str, Any]]] = deque()
    graph = nx.DiGraph()
    node_id: NodeID = NodeID(1)
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


def get_match_paths(match_tree: MatchTree) -> Generator[MatchCriterion, None, None]:
    leaves = list()
    for node in match_tree.nodes:
        if match_tree.out_degree(node) == 0:
            leaves.append(node)
    for leaf in leaves:
        path = nx.shortest_path(match_tree, 0, leaf) if leaf != 0 else [leaf]
        match_path = MatchCriterion(list())
        for node in path:
            match_path.extend(match_tree.nodes[node]['criteria_list'])
        yield match_path


def translate_match_path(path: ParentPath,
                         match_criterion: MatchCriterion,
                         match_criteria_transformer: MatchCriteriaTransform) -> MultiCollectionQuery:
    categories = defaultdict(list)
    for criteria in match_criterion:
        for genomic_or_clinical, values in criteria.items():
            and_query = dict()
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
                and_query.update(sample_function(match_criteria_transformer, **args))
            categories[genomic_or_clinical].append(and_query)
    return MultiCollectionQuery(categories)


def add_sample_ids_to_query(query: MultiCollectionQuery,
                            sample_ids: List[str],
                            match_criteria_transformer: MatchCriteriaTransform) -> MultiCollectionQuery:
    if sample_ids is not None:
        query[match_criteria_transformer.clinical_collection].append({
            "SAMPLE_ID": {
                "$in": sample_ids
            }
        })
    return query


def run_query(db: pymongo.database.Database,
              match_criteria_transformer: MatchCriteriaTransform,
              multi_collection_query: MultiCollectionQuery) -> set:
    primary_ids = set()
    all_results = defaultdict(lambda: defaultdict(list))
    if match_criteria_transformer.clinical_collection in multi_collection_query:
        collection = match_criteria_transformer.clinical_collection
        join_field = match_criteria_transformer.primary_collection_unique_field
        projection = {join_field: 1}
        query = {"$and": multi_collection_query[collection]}
        primary_ids.update([result[join_field] for result in db[collection].find(query, projection)])
        if not primary_ids:
            return set()
    for items in multi_collection_query.items():
        category, queries = items
        if category == match_criteria_transformer.clinical_collection:
            continue
        join_field = match_criteria_transformer.collection_mappings[category]['join_field']
        projection = {join_field: 1}
        if category == 'genomic':
            projection.update({
                "TIER": 1,
                "VARIANT_CATEGORY": 1,
                "WILDTYPE": 1,
                "TRUE_HUGO_SYMBOL": 1,
                "TRUE_PROTEIN_CHANGE": 1,
                "CNV_CALL": 1,
                "TRUE_VARIANT_CLASSIFICATION": 1,
                "MMR_STATUS": 1
            })
        for query in queries:
            if primary_ids:
                query.update({join_field: {
                    "$in": [  # ensure all clinical IDs are valid MongoDB ObjectIDs
                        primary_id if isinstance(primary_id, ObjectId) else ObjectId(primary_id)
                        for primary_id in primary_ids
                    ]
                }})
            results = db[category].find(query, projection)
            ids = {  # ensure all returned clinical IDs are valid ObjectIDs
                result[join_field] if isinstance(result[join_field], ObjectId) else ObjectId(result[join_field])
                for result in results
            }
            if not ids:
                return set()
            # if this is the first run of the loop, save the IDs to the primary_ids set
            if not primary_ids:
                primary_ids.update(ids)
            else:  # otherwise, intersect the set
                results_to_remove = ids - primary_ids
                for result_to_remove in results_to_remove:
                    del all_results[category][result_to_remove["_id"]]
                primary_ids.intersection_update(ids)
                if not primary_ids:
                    return set()
                else:
                    for result in results:
                        if result[join_field] in primary_ids:
                            all_results[category][result['_id']].append(result)
    return primary_ids


if __name__ == "__main__":
    find_matches(protocol_nos=['***REMOVED***'])
    # find_matches(sample_ids=["***REMOVED***"], protocol_nos=None)
