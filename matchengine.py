import time

from pymongo import UpdateMany, InsertOne
from pymongo.errors import AutoReconnect

from match_criteria_transform import MatchCriteriaTransform
from mongo_connection import MongoDBConnection
from collections import deque, defaultdict
from typing import Generator, Set, Text, AsyncGenerator
from frozendict import frozendict
from multiprocessing import cpu_count

import pymongo.database
import networkx as nx
import logging
import json
import argparse
import asyncio

from matchengine_types import *
from trial_match_utils import *
from load import load

logging.basicConfig(level=logging.INFO)
log = logging.getLogger('matchengine')

count = 0
count2 = 0


async def queue_worker(q, matches, config, worker_id) -> None:
    global count
    global count2
    if not q.empty():
        match_criteria_transform = MatchCriteriaTransform(config)
        match_task_count = 0
        with MongoDBConnection(read_only=True) as ro_db, MongoDBConnection(read_only=False) as rw_db:
            while not q.empty():
                task: Union[QueryTask, MatchTask] = await q.get()
                try:
                    # logging.info("Worker: {}, query: {}".format(worker_id, task.query))
                    if isinstance(task, QueryTask):
                        log.info(
                            "Worker: {}, protocol_no: {} got new QueryTask".format(worker_id,
                                                                                   task.trial['protocol_no']))
                        async for result in run_query(task.cache,
                                                      ro_db,
                                                      match_criteria_transform,
                                                      task.queries,
                                                      task.clinical_ids):
                            trial_match = TrialMatch(task.trial,
                                                     task.match_clause_data,
                                                     task.match_path,
                                                     task.queries,
                                                     result)
                            count += 1
                            if count % 100 == 1:
                                log.info("count: {}".format(count))
                            match_document = create_trial_matches(trial_match)
                            matches.append(match_document)
                            # await q.put(MatchTask(task, result))
                    elif isinstance(task, MatchTask):
                        match_task_count += 1
                        if match_task_count % 100 == 0:
                            log.info(
                                "Worker: {}, protocol_no: {} MatchTask {}".format(worker_id,
                                                                                  task.query_task.trial[
                                                                                      'protocol_no'],
                                                                                  match_task_count))
                        trial_match = TrialMatch(task.query_task.trial,
                                                 task.query_task.match_clause_data,
                                                 task.query_task.match_path,
                                                 task.query_task.queries,
                                                 task.raw_result)
                        for match_document in create_trial_matches(trial_match):
                            await matches.put(match_document)
                    q.task_done()
                except Exception as e:
                    log.error("ERROR: Worker: {}, error: {}".format(worker_id, e))
                    if isinstance(e, AutoReconnect):
                        q.task_done()
                        await q.put(task)
                    else:
                        raise e


async def find_matches(sample_ids: list = None,
                       protocol_nos: list = None,
                       debug: bool = False,
                       num_workers: int = 25,
                       match_on_closed: bool = False,
                       match_on_deceased: bool = False,
                       cache: Cache = None) -> AsyncGenerator:  # Generator[Tuple[str, List[str], List[Dict]]]:
    """
    Take a list of sample ids and trial protocol numbers, return a dict of trial matches
    :param cache:
    :param match_on_closed:
    :param match_on_deceased:
    :param sample_ids:
    :param protocol_nos:
    :param debug:
    :param num_workers
    :return:
    """
    log.info('Beginning trial matching.')

    with open("config/config.json") as config_file_handle:
        config = json.load(config_file_handle)

    # init
    match_criteria_transform = MatchCriteriaTransform(config)

    if cache is None:
        cache = Cache(int(), int(), int(), int(), dict(), dict(), dict())

    with MongoDBConnection(read_only=True) as db:
        trials = [trial async for trial in get_trials(db, match_criteria_transform, protocol_nos, match_on_closed)]
        _ids = await get_clinical_ids_from_sample_ids(db, sample_ids, match_on_deceased)

    for trial in trials:
        q = asyncio.queues.Queue()
        matches = list()
        log.info("Begin Protocol No: {}".format(trial["protocol_no"]))
        for match_clause in extract_match_clauses_from_trial(match_criteria_transform, trial, match_on_closed):
            for match_path in get_match_paths(create_match_tree(match_clause.match_clause)):
                query = translate_match_path(match_clause,
                                             match_path,
                                             match_criteria_transform)
                if debug:
                    log.info("Query: {}".format(query))
                if query:  # and match_clause.internal_id == 1999999:
                    await q.put(QueryTask(match_criteria_transform,
                                          trial,
                                          match_clause,
                                          match_path,
                                          query,
                                          _ids,
                                          cache))
        workers = [asyncio.create_task(queue_worker(q, matches, config, i))
                   for i in range(0, min(q.qsize(), num_workers))]
        await asyncio.gather(*workers)
        await q.join()
        logging.info("Total results: {}".format(matches.__len__()))
        yield trial['protocol_no'], sample_ids, matches


async def get_trials(db: pymongo.database.Database,
                     match_criteria_transform: MatchCriteriaTransform,
                     protocol_nos: list = None,
                     match_on_closed: bool = False) -> Generator[Trial, None, None]:
    trial_find_query = dict()

    # the minimum criteria needed in a trial projection. add extra values in config.json
    projection = {'protocol_no': 1, 'nct_id': 1, 'treatment_list': 1, 'status': 1}
    projection.update(match_criteria_transform.trial_projection)

    if protocol_nos is not None:
        trial_find_query['protocol_no'] = {"$in": [protocol_no for protocol_no in protocol_nos]}

    async for trial in db.trial.find(trial_find_query, projection):
        if trial['status'].lower().strip() not in {"open to accrual"} and not match_on_closed:
            logging.info('Trial %s is closed, skipping' % trial['protocol_no'])
        else:
            yield Trial(trial)


async def get_clinical_ids_from_sample_ids(db, sample_ids: List[str],
                                           match_on_deceased: bool = False) -> List[ClinicalID]:
    # if no sample ids are passed in as args, get all clinical documents
    if sample_ids is None:
        query = {} if match_on_deceased else {"VITAL_STATUS": 'alive'}
        return [result['_id']
                for result in await db.clinical.find(query, {"_id": 1}).to_list(None)]
    else:
        return [result['_id']
                for result in await db.clinical.find({"SAMPLE_ID": {"$in": sample_ids}}, {"_id": 1}).to_list(None)]


def extract_match_clauses_from_trial(match_criteria_transform: MatchCriteriaTransform,
                                     trial: Trial,
                                     match_on_closed: bool = False) -> Generator[MatchClauseData, None, None]:
    """
    Pull out all of the matches from a trial curation.
    Return the parent path and the values of that match clause.

    Default to only extracting match clauses on steps, arms or dose levels which are open to accrual unless otherwise
    specified

    :param match_on_closed:
    :param trial:
    :return:
    """

    # find all match clauses. place everything else (nested dicts/lists) on a queue
    process_q = deque()
    for key, val in trial.items():

        # include top level match clauses
        if key == 'match':
            # TODO uncomment, for now don't match on top level match clauses
            continue
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
                    if path[-1] == 'arm':
                        if not match_on_closed and \
                                parent_value.setdefault('arm_suspended', 'n').lower().strip() == 'y':
                            continue
                    elif path[-1] == 'dose':
                        if not match_on_closed and \
                                parent_value.setdefault('level_suspended', 'n').lower().strip() == 'y':
                            continue
                    elif path[-1] == 'step':
                        if not match_on_closed and \
                                all([arm.setdefault('arm_suspended', 'n').lower().strip() == 'y'
                                     for arm in parent_value.setdefault('arm', list({'arm_suspended': 'y'}))]):
                            continue

                    parent_path = ParentPath(path + (parent_key, inner_key))
                    level = MatchClauseLevel(
                        match_criteria_transform.level_mapping[
                            [item for item in parent_path[::-1] if not isinstance(item, int) and item != 'match'][0]])

                    internal_id = parent_value[match_criteria_transform.internal_id_mapping[level]]
                    yield MatchClauseData(inner_value, internal_id, parent_path, level, parent_value)
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
                graph.add_edges_from([(parent_id, node_id)])
                graph.nodes[node_id]['criteria_list'] = list()
                for item in value:
                    process_q.append((node_id, item))
                node_id += 1
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
            match_path.append(match_tree.nodes[node]['criteria_list'])
        yield match_path


def translate_match_path(match_clause_data: MatchClauseData,
                         match_criterion: MatchCriterion,
                         match_criteria_transformer: MatchCriteriaTransform) -> List[MultiCollectionQuery]:
    """
    Translate the keys/values from the trial curation into keys/values used in a genomic/clinical document.
    Uses an external config file ./config/config.json

    :param match_clause_data:
    :param match_criterion:
    :param match_criteria_transformer:
    :return:
    """
    output = list()
    query_cache = set()
    for node in match_criterion:
        categories = MultiCollectionQuery(defaultdict(list))
        for criteria in node:
            for genomic_or_clinical, values in criteria.items():
                and_query = dict()
                any_negate = False
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
                                parent_path=match_clause_data.parent_path,
                                trial_path=genomic_or_clinical,
                                trial_key=trial_key)
                    args.update(trial_key_settings)
                    sample_value, negate = sample_function(match_criteria_transformer, **args)
                    if not any_negate and negate:
                        any_negate = True
                    and_query.update(sample_value)
                if and_query:
                    if comparable_dict(and_query).hash() not in query_cache:
                        categories[genomic_or_clinical].append((any_negate, and_query))
                        query_cache.add(comparable_dict(and_query).hash())
        if categories:
            output.append(categories)
    return output


async def execute_clinical_query(db: pymongo.database.Database,
                                 match_criteria_transformer: MatchCriteriaTransform,
                                 multi_collection_query: MultiCollectionQuery,
                                 initial_clinical_ids: Set[ClinicalID]) -> Set[ObjectId]:
    if match_criteria_transformer.CLINICAL in multi_collection_query:
        collection = match_criteria_transformer.CLINICAL
        not_query = list()
        query = list()
        for negate, inner_query in multi_collection_query[collection]:
            for k, v in inner_query.items():
                if negate:
                    not_query.append({k: v})
                else:
                    query.append({k: v})
        if initial_clinical_ids:
            if not_query:
                not_query.insert(0, {"_id": {"$in": list(initial_clinical_ids)}})
            if query:
                query.insert(0, {"_id": {"$in": list(initial_clinical_ids)}})
        if query:
            positive_results = {result["_id"]
                                for result in await db[collection].find({"$and": query}, {"_id": 1}).to_list(None)}
        else:
            positive_results = initial_clinical_ids
        if not_query:
            negative_results = {result["_id"]
                                for result in await db[collection].find({"$and": not_query}, {"_id": 1}).to_list(None)}
        else:
            negative_results = set()

        clinical_ids = positive_results - negative_results
        return clinical_ids
    else:
        return initial_clinical_ids


async def perform_db_call(db, collection, query, projection):
    return await db[collection].find(query, projection).to_list(None)


async def run_query(cache: Cache,
                    db: pymongo.database.Database,
                    match_criteria_transformer: MatchCriteriaTransform,
                    multi_collection_queries: List[MultiCollectionQuery],
                    initial_clinical_ids: List[ClinicalID]) -> Generator[RawQueryResult,
                                                                         None,
                                                                         RawQueryResult]:
    """
    Execute a mongo query on the clinical and genomic collections to find trial matches.
    First execute the clinical query. If no records are returned short-circuit and return.

    :param db:
    :param match_criteria_transformer:
    :param multi_collection_queries:
    :return:
    """
    # TODO refactor into smaller functions
    all_results: Dict[ObjectId, Set[ObjectId]] = defaultdict(set)
    reasons = list()

    clinical_ids = set()
    for multi_collection_query in multi_collection_queries:
        # get clinical docs first
        new_clinical_ids = await execute_clinical_query(db,
                                                        match_criteria_transformer,
                                                        multi_collection_query,
                                                        clinical_ids if clinical_ids else set(initial_clinical_ids))

        # If no clinical docs are returned, skip executing genomic portion of the query
        if not new_clinical_ids:
            return
        for key in new_clinical_ids:
            clinical_ids.add(key)

        # iterate over all queries
        for items in multi_collection_query.items():
            genomic_or_clinical, queries = items

            # skip clinical queries as they've already been executed
            if genomic_or_clinical == match_criteria_transformer.CLINICAL and clinical_ids:
                continue

            join_field = match_criteria_transformer.collection_mappings[genomic_or_clinical]['join_field']

            for negate, query in queries:
                # cache hit or miss should be here
                uniq = comparable_dict(query).hash()
                query_clinical_ids = clinical_ids if clinical_ids else set(initial_clinical_ids)
                if uniq not in cache.queries:
                    cache.queries[uniq] = dict()
                need_new = query_clinical_ids - set(cache.queries[uniq].keys())
                if need_new:
                    new_query = {"$and": list()}
                    for k, v in query.items():
                        new_query['$and'].append({k: v})
                    new_query['$and'].insert(0,
                                             {join_field:
                                                  {'$in': list(need_new)}})
                    cursor = await db[genomic_or_clinical].find(new_query, {"_id": 1, "CLINICAL_ID": 1}).to_list(None)
                    for result in cursor:
                        cache.queries[uniq][result["CLINICAL_ID"]] = result["_id"]

                clinical_result_ids = set()

                for query_clinical_id in query_clinical_ids:
                    if query_clinical_id in cache.queries[uniq]:
                        genomic_id = cache.queries[uniq][query_clinical_id]
                        clinical_result_ids.add(query_clinical_id)
                        if not negate:
                            all_results[query_clinical_id].add(genomic_id)
                            reasons.append((negate, query, query_clinical_id, genomic_id))
                        elif negate and query_clinical_id in all_results:
                            del all_results[query_clinical_id]
                    elif query_clinical_id not in cache.queries[uniq] and negate:
                        if query_clinical_id not in all_results:
                            all_results[query_clinical_id] = set()
                        reasons.append((negate, query, query_clinical_id, None))

                if not negate:
                    clinical_ids.intersection_update(clinical_result_ids)
                else:
                    clinical_ids.difference_update(clinical_result_ids)

                if not clinical_ids:
                    return
                else:
                    for id_to_remove in set(all_results.keys()) - clinical_ids:
                        del all_results[id_to_remove]

    needed_clinical = list()
    needed_genomic = list()
    for clinical_id, genomic_ids in all_results.items():
        if clinical_id not in cache.docs:
            needed_clinical.append(clinical_id)
        for genomic_id in genomic_ids:
            if genomic_id not in cache.docs:
                needed_genomic.append(genomic_id)

    # minimum fields required to execute matching. Extra matching fields can be added in config.json
    genomic_projection = {
        "SAMPLE_ID": 1,
        "CLINICAL_ID": 1,
        "VARIANT_CATEGORY": 1,
        "WILDTYPE": 1,
        "TRUE_TRANSCRIPT_EXON": 1,
        "TIER": 1,
        "TRUE_HUGO_SYMBOL": 1,
        "TRUE_PROTEIN_CHANGE": 1,
        "CNV_CALL": 1,
        "TRUE_VARIANT_CLASSIFICATION": 1,
        "MMR_STATUS": 1
    }
    genomic_projection.update(match_criteria_transformer.genomic_projection)

    # minimum projection necessary for matching. Append extra values from config if desired
    clinical_projection = {
        "SAMPLE_ID": 1,
        "MRN": 1,
        "ONCOTREE_PRIMARY_DIAGNOSIS_NAME": 1,
        "VITAL_STATUS": 1,
        "FIRST_LAST": 1
    }
    clinical_projection.update(match_criteria_transformer.clinical_projection)

    results = await asyncio.gather(perform_db_call(db,
                                                   "clinical",
                                                   {"_id": {"$in": list(needed_clinical)}},
                                                   clinical_projection),
                                   perform_db_call(db,
                                                   "genomic",
                                                   {"_id": {"$in": list(needed_genomic)}},
                                                   genomic_projection))
    for outer_result in results:
        for result in outer_result:
            cache.docs[result["_id"]] = result
    for negate, query, query_clinical_id, genomic_id in reasons:
        if query_clinical_id in all_results:
            if negate:
                yield RawQueryResult(query, ClinicalID(query_clinical_id), cache.docs[query_clinical_id], None)
            elif genomic_id in all_results[query_clinical_id]:
                yield RawQueryResult(query, ClinicalID(query_clinical_id), cache.docs[query_clinical_id],
                                     cache.docs[genomic_id])
    # for clinical_id, genomic_ids in all_results.items():
    #     yield RawQueryResult(multi_collection_query,
    #                          ClinicalID(clinical_id),
    #                          cache.docs[clinical_id],
    #                          [cache.docs[genomic_id] for genomic_id in genomic_ids])
    # return [RawQueryResult(multi_collection_query,
    #                        ClinicalID(clinical_id),
    #                        cache.docs[clinical_id],
    #                        [cache.docs[genomic_id]
    #                         for genomic_id in genomic_ids])
    #         for clinical_id, genomic_ids in all_results.items()]


def create_trial_matches(trial_match: TrialMatch) -> Dict:
    """
    Create a trial match document to be inserted into the db. Add clinical, genomic, and trial details as specified
    in config.json
    :param trial_match:
    :return:
    """
    genomic_doc = trial_match.raw_query_result.genomic_doc
    query = trial_match.raw_query_result.query
    new_trial_match = dict()
    new_trial_match.update(format(trial_match.raw_query_result.clinical_doc))
    if genomic_doc is None:
        new_trial_match.update(format(format_not_match(query)))
    else:
        new_trial_match.update(format(get_genomic_details(genomic_doc, query)))
    new_trial_match.update(
        {'match_level': trial_match.match_clause_data.match_clause_level,
         'internal_id': trial_match.match_clause_data.internal_id})
    # remove extra fields from trial_match output
    new_trial_match.update({k: v
                            for k, v in trial_match.trial.items() if k not in {'treatment_list',
                                                                               '_summary',
                                                                               'status',
                                                                               '_id',
                                                                               '_elasticsearch',
                                                                               'match'}
                            })
    new_trial_match['query_hash'] = comparable_dict({'query': trial_match.match_criterion}).hash()
    new_trial_match['hash'] = comparable_dict(new_trial_match).hash()
    # new_trial_match["query"] = trial_match.match_criterion
    new_trial_match["is_disabled"] = False
    new_trial_match.update({'match_path': '.'.join([str(item) for item in trial_match.match_clause_data.parent_path])})
    return new_trial_match


async def update_trial_matches(trial_matches: List[Dict], protocol_no: str, input_sample_ids: List[str]):
    if input_sample_ids is None:
        input_sample_ids = list({trial_match['sample_id'] for trial_match in trial_matches})
    sample_id_to_trial_match = defaultdict(list)
    for idx, trial_match in enumerate(trial_matches):
        sample_id_to_trial_match[trial_match['sample_id']].append(idx)

    def chunks(l, n):
        n = max(1, n)
        return (l[i:i + n] for i in range(0, len(l), n))

    """
    Update trial matches by diff'ing the newly created trial matches against existing matches in the db.
    'Delete' matches by adding {is_disabled: true} and insert all new matches.
    :param protocol_no:
    :param trial_matches:
    :param sample_ids:
    :return:
    """
    for sample_ids in chunks(input_sample_ids, 200):
        sample_ids_set = set(sample_ids)
        new_matches_hashes = [trial_match['hash']
                              for trial_match in trial_matches
                              if trial_match['sample_id'] in sample_ids_set]

        query = {'hash': {'$in': new_matches_hashes}}

        with MongoDBConnection(read_only=True) as db:
            trial_matches_to_not_change = {result['hash']: result.setdefault('is_disabled', False)
                                           for result in await db.trial_match_test.find(query,
                                                                                        {"hash": 1,
                                                                                         "is_disabled": 1}).to_list(
                    None)}

        delete_where = {'hash': {'$nin': new_matches_hashes}}
        if protocol_no:
            delete_where['protocol_no'] = protocol_no
        if sample_ids:
            delete_where['sample_id'] = {'$in': sample_ids}
        update = {"$set": {'is_disabled': True}}

        trial_matches_to_insert = [trial_match
                                   for trial_match in trial_matches
                                   if trial_match['hash'] not in trial_matches_to_not_change]
        trial_matches_to_mark_available = [trial_match
                                           for trial_match in trial_matches
                                           if trial_match['hash'] in trial_matches_to_not_change
                                           and trial_matches_to_not_change.setdefault('is_disabled', False)]

        with MongoDBConnection(read_only=False) as db:
            ops = list()
            ops.append(UpdateMany(delete_where, update))
            ops.extend([InsertOne(doc) for doc in trial_matches_to_insert])
            ops.append(UpdateMany({'hash': {'$in': trial_matches_to_insert}}, {'$set': {'is_disabled': False}}))
            result = await db.trial_match_test.bulk_write(ops, ordered=False)

            async def delete():
                log.info('Deleting')
                await db.trial_match_test.update_many(delete_where, update)
                log.info("Delete done")

            async def insert():
                if trial_matches_to_insert:
                    log.info("Trial matches to insert: {}".format(len(trial_matches_to_insert)))
                    await db.trial_match_test.insert_many(trial_matches_to_insert)
                log.info("Insert Done")

            async def mark_available():
                log.info("{}".format(trial_matches_to_mark_available))
                if trial_matches_to_mark_available:
                    log.info("Trial matches to mark available: {}".format(len(trial_matches_to_mark_available)))
                    await db.trial_match_test.update({'hash': {'$in': trial_matches_to_mark_available}},
                                                     {'$set': {'is_disabled': False}})
                    log.info("Mark available done")

            # await asyncio.gather(asyncio.create_task(delete()),
            #                      asyncio.create_task(insert()),
            #                      asyncio.create_task(mark_available()))


async def check_indices():
    """
    Ensure indexes exist on the trial_match collection so queries are performant
    :return:
    """
    with MongoDBConnection(read_only=False) as db:
        indexes = db.trial_match.list_indexes()
        existing_indexes = set()
        desired_indexes = {'hash', 'mrn', 'sample_id', 'clinical_id', 'protocol_no'}
        async for index in indexes:
            index_key = list(index['key'].to_dict().keys())[0]
            existing_indexes.add(index_key)
        indexes_to_create = desired_indexes - existing_indexes
        for index in indexes_to_create:
            log.info('Creating index %s' % index)
            await db.trial_match.create_index(index)


async def main(args):
    await check_indices()
    all_new_matches = find_matches(sample_ids=args.samples,
                                   protocol_nos=args.trials,
                                   num_workers=args.workers[0],
                                   match_on_closed=args.match_on_closed,
                                   match_on_deceased=args.match_on_deceased)
    async for protocol_no, sample_ids, matches in all_new_matches:
        log.info("{} all matches: {}".format(protocol_no, len(matches)))
        await update_trial_matches(matches, protocol_no, sample_ids)


if __name__ == "__main__":
    # todo unit tests
    # todo refactor run_query
    # todo output CSV file functions
    # todo dry flag
    # todo regex SV matching
    # todo update/delete/insert run log
    # todo failsafes for insert logic (fallback?)

    param_trials_help = 'Path to your trial data file or a directory containing a file for each trial.' \
                        'Default expected format is YML.'
    param_mongo_uri_help = 'Your MongoDB URI. If you do not supply one, for matching, it will default to whatever' \
                           ' is set to "MONGO_URI" in your SECRETS.JSON file. This file must be set as an ' \
                           'environmental variable. For data loading you must specify a URI with a database ' \
                           'ex: mongodb://localhost:27017/matchminer. ' \
                           'See https://docs.mongodb.com/manual/reference/connection-string/ for more information.'
    param_clinical_help = 'Path to your clinical file. Default expected format is CSV.'
    param_genomic_help = 'Path to your genomic file. Default expected format is CSV'
    param_outpath_help = 'Destination and name of your results file.'
    param_trial_format_help = 'File format of input trial data. Default is YML.'
    param_patient_format_help = 'File format of input patient data (both clinical and genomic files). Default is CSV.'

    parser = argparse.ArgumentParser()
    closed_help = 'Match on all closed trials and all suspended steps, arms and doses. Default is to skip.'
    deceased_help = 'Match on deceased patients. Default is to match only on alive patients.'
    upsert_help = 'When loading clinical or trial data, specify a field other than _id to use as a unique key. ' \
                  'Must be comma separated values if using more than one field e.g. name,age,gender'

    subp = parser.add_subparsers(help='sub-command help')
    subp_p = subp.add_parser('load', help='Sets up your MongoDB for matching.')
    subp_p.add_argument('-t', dest='trials', help=param_trials_help)
    subp_p.add_argument('-c', dest='clinical', help=param_clinical_help)
    subp_p.add_argument('-g', dest='genomic', help=param_genomic_help)
    subp_p.add_argument('--mongo-uri', dest='mongo_uri', required=True, default=None, help=param_mongo_uri_help)
    subp_p.add_argument('--trial-format',
                        dest='trial_format',
                        default='yml',
                        action='store',
                        choices=['yml', 'json', 'bson'],
                        help=param_trial_format_help)
    subp_p.add_argument('--patient-format', dest='patient_format',
                        default='csv',
                        action='store',
                        choices=['csv', 'pkl', 'bson', 'json'],
                        help=param_patient_format_help)
    subp_p.add_argument('--upsert-fields', dest='upsert_fields', default='', required=False, help=upsert_help)
    subp_p.set_defaults(func=load)

    subp_p = subp.add_parser('match', help='Match patients to trials.')
    subp_p.add_argument("-trials", nargs="*", type=str, default=None)
    subp_p.add_argument("-samples", nargs="*", type=str, default=None)
    subp_p.add_argument("--match-on-closed",
                        dest="match_on_closed",
                        action="store_true",
                        default=False,
                        help=closed_help)
    subp_p.add_argument("--match-on-deceased-patients",
                        dest="match_on_deceased",
                        action="store_true",
                        help=deceased_help)
    subp_p.add_argument("-workers", nargs=1, type=int, default=[cpu_count() * 5])
    subp_p.add_argument('-o', dest="outpath", required=False, help=param_outpath_help)
    args = parser.parse_args()
    # args.func(args)
    asyncio.run(main(args))
