from typing import Any, Tuple, Union, NewType, List, Dict, AsyncGenerator, Generator
from MatchCriteriaTransform import MatchCriteriaTransform
from MongoDBConnection import MongoDBConnection
from collections import deque, defaultdict
from bson import ObjectId

import pymongo.database
import networkx as nx
import json
import logging

logging.basicConfig(level=logging.INFO)
log = logging.getLogger('matchengine')

Trial = NewType("Trial", dict)
ParentPath = NewType("ParentPath", Tuple[Union[str, int]])
MatchClause = NewType("MatchClause", List[Dict[str, Any]])
MatchTree = NewType("MatchTree", nx.DiGraph)
MatchCriterion = NewType("MatchPath", List[Dict[str, Any]])
MultiCollectionQuery = NewType("MongoQuery", dict)
NodeID = NewType("NodeID", int)
MongoQueryResult = NewType("MongoQueryResult", Dict[str, Any])
MongoQuery = NewType("MongoQuery", Dict[str, Any])
GenomicID = NewType("GenomicID", ObjectId)
ClinicalID = NewType("ClinicalID", ObjectId)
RawQueryResult = NewType("RawQueryResult",
                         Tuple[ClinicalID, Dict[GenomicID, Dict[str, Union[MongoQuery, MongoQueryResult]]]])
TrialMatch = NewType("TrialMatch", Dict[str, Any])


def find_matches(sample_ids: list = None, protocol_nos: list = None, debug=False) -> dict:
    """
    Take a list of sample ids and trial protocol numbers, return a dict of trial matches
    :param sample_ids:
    :param protocol_nos:
    :param debug:
    :return:
    """
    log.info('Beginning trial matching. %s on closed trials. %s deceased patients.')

    with open("config/config.json") as config_file_handle:
        config = json.load(config_file_handle)
    match_criteria_transform = MatchCriteriaTransform(config)

    with MongoDBConnection(read_only=True) as db:
        for trial in get_trials(db, protocol_nos):
            log.info("Protocol No: {}".format(trial["protocol_no"]))
            for parent_path, match_clause in extract_match_clauses_from_trial(trial):
                for match_path in get_match_paths(create_match_tree(match_clause)):
                    try:
                        protocol_no = trial['protocol_no']
                        translated_match_path = translate_match_path(parent_path, match_path, match_criteria_transform)
                        query = add_sample_ids_to_query(translated_match_path, sample_ids, match_criteria_transform)
                        results = [result for result in run_query(db, match_criteria_transform, query)]

                        if len(results) > 0:
                            log.info(f'Protocol No: {protocol_no}')
                            log.info(f'Parent_path: {parent_path}')
                            log.info(f'Match_path: {match_path}')
                            log.info(f'Query: {query}')
                            log.info(f'len(results): {len(results)}')
                            log.info('')

                            create_trial_match(results, parent_path, match_clause)
                    except Exception as e:
                        logging.error("ERROR: {}".format(e))
                        raise e


def get_trials(db: pymongo.database.Database, protocol_nos: list = None) -> Generator[Trial, None, None]:
    trial_find_query = dict()
    if protocol_nos is not None:
        trial_find_query['protocol_no'] = {"$in": [protocol_no for protocol_no in protocol_nos]}

    for trial in db.trial.find(trial_find_query):
        # TODO toggle with flag
        if trial['status'].lower().strip() not in {"open to accrual"}:
            yield Trial(trial)
        else:
            logging.info('Trial %s is closed, skipping' % trial['protocol_no'])


def extract_match_clauses_from_trial(trial: Trial) -> Generator[List[Tuple[ParentPath, MatchClause]], None, None]:
    """
    Pull out all of the matches from a trial curation.
    Return the parent path and the values of that match clause
    :param trial:
    :return:
    """

    # find all match clauses. place everything else (nested dicts/lists) on a queue
    process_q = deque()
    for key, val in trial.items():

        # skip top level match clauses
        if key == 'match':
            continue
            # if val[0]['clinical']['disease_status']:
            #     continue
            # else:
            #     parent_path = ParentPath(tuple())
            #     yield parent_path, val
        else:
            process_q.append((tuple(), key, val))

    # process nested dicts to find more match clauses
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
    """
    Translate the keys/values from the trial curation into keys/values used in a genomic/clinical document.
    Uses an external config file ./config/config.json
    :param path:
    :param match_criterion:
    :param match_criteria_transformer:
    :return:
    """
    categories = defaultdict(list)
    for criteria in match_criterion:
        for genomic_or_clinical, values in criteria.items():
            and_query = dict()
            for trial_key, trial_value in values.items():
                trial_key_settings = match_criteria_transformer.trial_key_mappings[genomic_or_clinical].setdefault(
                    trial_key.upper(),
                    dict())

                if 'ignore' in trial_key_settings and trial_key_settings['ignore']:
                    continue

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
        query[match_criteria_transformer.CLINICAL].append({
            "SAMPLE_ID": {
                "$in": sample_ids
            }
        })
    return query


def run_query(db: pymongo.database.Database,
              match_criteria_transformer: MatchCriteriaTransform,
              multi_collection_query: MultiCollectionQuery) -> Generator[RawQueryResult, None, RawQueryResult]:
    """
    Execute mongo query
    :param db:
    :param match_criteria_transformer:
    :param multi_collection_query:
    :return:
    """
    all_results = defaultdict(lambda: defaultdict(dict))

    # get clinical docs
    clinical_docs, clinical_ids = execute_clinical_query(db, match_criteria_transformer, multi_collection_query)

    # set on all_results to return later
    for doc in clinical_docs:
        clinical_id = doc['_id']
        all_results[clinical_id][match_criteria_transformer.CLINICAL][clinical_id] = doc

    # If no clinical docs are returned, skip executing genomic portion of the query
    if not clinical_docs:
        return RawQueryResult(tuple())

    for items in multi_collection_query.items():
        category, queries = items

        # skip clinical queries as they've already been executed
        if category == match_criteria_transformer.CLINICAL and clinical_docs:
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
            if clinical_docs:
                query.update({join_field: {"$in": list(clinical_ids)}})

            results = [result for result in db[category].find(query, projection)]

            # ensure all returned clinical IDs are valid ObjectIDs
            ids = {result[join_field] for result in results}

            # short circuit if no values are returned
            if not ids:
                return RawQueryResult(tuple())

            results_to_remove = clinical_ids - ids
            for result_to_remove in results_to_remove:
                if result_to_remove in all_results:
                    del all_results[result_to_remove]
            clinical_ids.intersection_update(ids)
            if not clinical_docs:
                return RawQueryResult(tuple())
            else:
                for doc in results:
                    if doc[join_field] in clinical_docs:
                        all_results[doc[join_field]][category][doc['_id']] = {
                            "result": doc,
                            "query": query
                        }

    for clinical_id, doc in all_results.items():
        yield RawQueryResult((clinical_id, doc))


def execute_clinical_query(db, match_criteria_transformer: MatchCriteriaTransform,
                           multi_collection_query: MultiCollectionQuery):
    clinical_docs = dict()
    clinical_ids = set()
    if match_criteria_transformer.CLINICAL in multi_collection_query:
        collection = match_criteria_transformer.CLINICAL
        join_field = match_criteria_transformer.primary_collection_unique_field
        projection = {join_field: 1}
        projection.update({
            "SAMPLE_ID": 1,
            'MRN': 1,
            'ORD_PHYSICIAN_NAME': 1,
            'ORD_PHYSICIAN_EMAIL': 1,
            'ONCOTREE_PRIMARY_DIAGNOSIS_NAME': 1,
            'REPORT_DATE': 1,
            'VITAL_STATUS': 1,
            'FIRST_LAST': 1,
            'GENDER': 1

        })
        query = {"$and": multi_collection_query[collection]}
        clinical_docs = [doc for doc in db[collection].find(query, projection)]
        clinical_ids = set([doc['_id'] for doc in clinical_docs])

    return clinical_docs, clinical_ids


def execute_genomic_query():
    pass


def create_trial_match(raw_query_result: RawQueryResult, parent_path, match_clause) -> TrialMatch:
    for query in raw_query_result:
        trial_match = dict()
        trial_match['clinical_id'] = query[0]
        trial_match['mrn'] = query[1]['clinical'][query[0]]['MRN']
        trial_match['gender'] = query[1]['clinical'][query[0]]['GENDER']
        trial_match['ord_physician_name'] = query[1]['clinical'][query[0]]['ORD_PHYSICIAN_NAME']
        trial_match['ord_physician_email'] = query[1]['clinical'][query[0]]['ORD_PHYSICIAN_EMAIL']
        trial_match['first_last'] = query[1]['clinical'][query[0]]['FIRST_LAST']
        trial_match['report_date'] = query[1]['clinical'][query[0]]['REPORT_DATE']
        trial_match['oncotree_primary_diagnosis_name'] = query[1]['clinical'][query[0]][
            'ONCOTREE_PRIMARY_DIAGNOSIS_NAME']

        if 'genomic' in query[1]:
            match_reason = 'GENE'
            for genomic_id, initial_query in query[1]['genomic'].items():
                result = initial_query['result']
                query = initial_query['query']
                if result['TRUE_PROTEIN_CHANGE'] is not None and result['TRUE_PROTEIN_CHANGE'] in query:
                    match_reason = 'VARIANT'
                    print(match_reason)


if __name__ == "__main__":
    # find_matches(protocol_nos=['17-251'])
    # find_matches(sample_ids=["BL-17-W40535"], protocol_nos=None)
    find_matches(sample_ids=["BL-17-J08441"], protocol_nos=['13-237'], debug=True)
    # find_matches(sample_ids=None, protocol_nos=None)
