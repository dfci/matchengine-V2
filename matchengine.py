from match_criteria_transform import MatchCriteriaTransform
from mongo_connection import MongoDBConnection
from collections import deque, defaultdict
from typing import Generator, Set

import pymongo.database
import networkx as nx
import logging
import json
import argparse
import asyncio

from matchengine_types import *
from trial_match_utils import *
from sort import Sort

logging.basicConfig(level=logging.INFO)
log = logging.getLogger('matchengine')


async def queue_worker(q, result_q, config, worker_id) -> None:
    if not q.empty():
        match_criteria_transform = MatchCriteriaTransform(config)
        with MongoDBConnection(read_only=True) as db:
            while not q.empty():
                task: QueueTask = await q.get()
                try:
                    # logging.info("Worker: {}, query: {}".format(worker_id, task.query))
                    log.info(
                        "Worker: {}, protocol_no: {} got new task".format(worker_id, task.trial['protocol_no']))
                    async for result in run_query(db, match_criteria_transform, task.query):
                        await result_q.put((task, result))
                        log.info("Worker: {},  protocol_no: {}, clinical_id: {}, qsize: {}".format(worker_id,
                                                                                                   task.trial[
                                                                                                       'protocol_no'],
                                                                                                   result.clinical_id,
                                                                                                   q.qsize()))
                    q.task_done()
                except Exception as e:
                    log.error("ERROR: Worker: {}, error: {}".format(worker_id, e))
                    q.task_done()
                    await q.put(task)


async def find_matches(sample_ids: list = None,
                       protocol_nos: list = None,
                       debug: bool = False,
                       num_workers: int = 25) -> Generator[TrialMatch,
                                                           None,
                                                           None]:
    """
    Take a list of sample ids and trial protocol numbers, return a dict of trial matches
    :param sample_ids:
    :param protocol_nos:
    :param debug:
    :param num_workers
    :return:
    """
    log.info('Beginning trial matching.')

    with open("config/config.json") as config_file_handle:
        config = json.load(config_file_handle)
    q = asyncio.queues.Queue()
    result_q = asyncio.queues.Queue()
    match_criteria_transform = MatchCriteriaTransform(config)

    with MongoDBConnection(read_only=True) as db:
        trials = [trial async for trial in get_trials(db, protocol_nos)]
        _ids = await get_clinical_ids_from_sample_ids(db, sample_ids)
    for trial in trials:
        log.info("Begin Protocol No: {}".format(trial["protocol_no"]))
        for match_clause_data in extract_match_clauses_from_trial(trial):
            for match_path in get_match_paths(create_match_tree(match_clause_data.match_clause)):
                translated_match_path = translate_match_path(match_clause_data,
                                                             match_path,
                                                             match_criteria_transform)
                query = add_ids_to_query(translated_match_path, _ids, match_criteria_transform)
                await q.put(QueueTask(match_criteria_transform, trial, match_clause_data, match_path, query))
    workers = [asyncio.create_task(queue_worker(q, result_q, config, i))
               for i in range(0, min(q.qsize(), num_workers))]
    await asyncio.gather(*workers)
    await q.join()
    while not result_q.empty():
        task: QueueTask
        result: RawQueryResult
        task, result = await result_q.get()
        yield TrialMatch(task.trial, task.match_clause_data, task.match_path, task.query, result)


async def get_trials(db: pymongo.database.Database, protocol_nos: list = None) -> Generator[Trial, None, None]:
    trial_find_query = dict()

    # the minimum criteria needed in a trial projection. add extra values in config.json
    projection = {'protocol_no': 1, 'nct_id': 1, 'treatment_list': 1, 'status': 1}
    projection.update(match_criteria_transform.trial_projection)

    if protocol_nos is not None:
        trial_find_query['protocol_no'] = {"$in": [protocol_no for protocol_no in protocol_nos]}

    async for trial in db.trial.find(trial_find_query, projection):
        # TODO toggle with flag
        if trial['status'].lower().strip() in {"open to accrual"}:
            yield Trial(trial)
        else:
            logging.info('Trial %s is closed, skipping' % trial['protocol_no'])


async def get_clinical_ids_from_sample_ids(db, sample_ids: List[str]) -> List[ClinicalID]:
    if sample_ids is None:
        return [result['_id']
                for result in await db.clinical.find({"VITAL_STATUS": 'alive'}, {"_id": 1}).to_list(None)]
    else:
        return [result['_id']
                for result in await db.clinical.find({"SAMPLE_ID": {"$in": sample_ids}}, {"_id": 1}).to_list(None)]


def extract_match_clauses_from_trial(trial: Trial) -> Generator[MatchClauseData, None, None]:
    """
    Pull out all of the matches from a trial curation.
    Return the parent path and the values of that match clause.

    Default to only extracting match clauses on steps, arms or dose levels which are open to accrual unless otherwise
    specified

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
                        if parent_value.setdefault('arm_suspended', 'n').lower().strip() == 'y':
                            continue
                    elif path[-1] == 'dose':
                        if parent_value.setdefault('level_suspended', 'n').lower().strip() == 'y':
                            continue
                    elif path[-1] == 'step':
                        if all([arm.setdefault('arm_suspended', 'n').lower().strip() == 'y'
                                for arm in parent_value.setdefault('arm', list({'arm_suspended': 'y'}))]):
                            continue
                    parent_path = ParentPath(path + (parent_key, inner_key))
                    level = MatchClauseLevel([item for item in parent_path[::-1] if not isinstance(item, int)][0])
                    yield MatchClauseData(inner_value, parent_path, level, parent_value)
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


def translate_match_path(match_clause_data: MatchClauseData,
                         match_criterion: MatchCriterion,
                         match_criteria_transformer: MatchCriteriaTransform) -> MultiCollectionQuery:
    """
    Translate the keys/values from the trial curation into keys/values used in a genomic/clinical document.
    Uses an external config file ./config/config.json

    :param match_clause_data:
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
                            parent_path=match_clause_data.parent_path,
                            trial_path=genomic_or_clinical,
                            trial_key=trial_key)
                args.update(trial_key_settings)
                and_query.update(sample_function(match_criteria_transformer, **args))
            categories[genomic_or_clinical].append(and_query)
    return MultiCollectionQuery(categories)


def add_ids_to_query(query: MultiCollectionQuery,
                     id_list: List[ClinicalID],
                     match_criteria_transformer: MatchCriteriaTransform) -> MultiCollectionQuery:
    if id_list is not None:
        query[match_criteria_transformer.CLINICAL].append({
            match_criteria_transformer.primary_collection_unique_field: {"$in": id_list}
        })
        for genomic_query in query.setdefault('genomic', list()):
            genomic_query[match_criteria_transformer.collection_mappings['genomic']['join_field']] = {"$in": id_list}
    return query


async def execute_clinical_query(db: pymongo.database.Database,
                                 match_criteria_transformer: MatchCriteriaTransform,
                                 multi_collection_query: MultiCollectionQuery) -> Tuple[
    Dict[ObjectId, MongoQueryResult],
    Set[ObjectId]]:
    clinical_docs = dict()
    clinical_ids = set()
    if match_criteria_transformer.CLINICAL in multi_collection_query:
        collection = match_criteria_transformer.CLINICAL
        join_field = match_criteria_transformer.primary_collection_unique_field

        # minimum projection necessary for matching. Append extra values from config if desired
        projection = {
            join_field: 1,
            "SAMPLE_ID": 1,
            "MRN": 1,
            "ONCOTREE_PRIMARY_DIAGNOSIS_NAME": 1,
            "VITAL_STATUS": 1,
            "FIRST_LAST": 1
        }
        projection.update(match_criteria_transformer.clinical_projection)
        query = {"$and": multi_collection_query[collection]}
        cursor = await db[collection].find(query, projection).to_list(None)
        clinical_docs = {doc['_id']: doc for doc in cursor}
        clinical_ids = set(clinical_docs.keys())

    return clinical_docs, clinical_ids


async def run_query(db: pymongo.database.Database,
                    match_criteria_transformer: MatchCriteriaTransform,
                    multi_collection_query: MultiCollectionQuery) -> Generator[RawQueryResult, None, RawQueryResult]:
    """
    Execute a mongo query on the clinical and genomic collections to find trial matches.
    First execute the clinical query. If no records are returned short-circuit and return.

    :param db:
    :param match_criteria_transformer:
    :param multi_collection_query:
    :return:
    """
    # TODO refactor into smaller functions
    all_results: Dict[ObjectId, Dict[Collection, Dict[ObjectId, Dict[Any, Any]]]] = defaultdict(
        lambda: defaultdict(dict))

    # get clinical docs first
    clinical_docs, clinical_ids = await execute_clinical_query(db, match_criteria_transformer, multi_collection_query)

    # If no clinical docs are returned, skip executing genomic portion of the query
    if not clinical_docs:
        return
    for key, doc in clinical_docs.items():
        collection = Collection(match_criteria_transformer.CLINICAL)
        all_results[key][collection] = doc

    # iterate over all queries
    for items in multi_collection_query.items():
        genomic_or_clinical, queries = items

        # skip clinical queries as they've already been executed
        if genomic_or_clinical == match_criteria_transformer.CLINICAL and clinical_docs:
            continue

        join_field = match_criteria_transformer.collection_mappings[genomic_or_clinical]['join_field']

        # minimum fields required to execute matching. Extra matching fields can be added in config.json
        projection = {
            join_field: 1,
            "SAMPLE_ID": 1,
            "CLINICAL_ID": 1,
            "VARIANT_CATEGORY": 1,
            "WILDTYPE": 1,
            "TIER": 1,
            "TRUE_HUGO_SYMBOL": 1,
            "TRUE_PROTEIN_CHANGE": 1,
            "CNV_CALL": 1,
            "TRUE_VARIANT_CLASSIFICATION": 1,
            "MMR_STATUS": 1,
        }

        if genomic_or_clinical == 'genomic':
            projection.update(match_criteria_transformer.genomic_projection)

        for query in queries:
            query.update({join_field: {"$in": list(clinical_ids)}})
            if join_field in query:
                new_query = {"$and": list()}
                for k, v in query.items():
                    if k == join_field:
                        new_query['$and'].insert(0, {k: v})
                    else:
                        new_query['$and'].append({k: v})
            else:
                new_query = query
            cursor = await db[genomic_or_clinical].find(new_query, projection).to_list(None)
            results = [result for result in cursor]
            result_ids = {result[join_field] for result in results}

            results_to_remove = clinical_ids - result_ids
            for result_to_remove in results_to_remove:
                if result_to_remove in all_results:
                    del all_results[result_to_remove]
            clinical_ids.intersection_update(result_ids)

            if not clinical_docs:
                return
            else:
                for doc in results:
                    if doc[join_field] in clinical_ids:
                        doc_id = doc["_id"]
                        all_results[doc[join_field]][genomic_or_clinical][doc_id] = doc

    for clinical_id, doc in all_results.items():
        clinical_doc = doc['clinical']
        genomic_docs = [genomic_doc for genomic_doc in doc.setdefault('genomic', dict()).values()]
        yield RawQueryResult(multi_collection_query, ClinicalID(clinical_id), clinical_doc, genomic_docs)


def create_trial_match(trial_match: TrialMatch):
    """
    Create a trial match document to be inserted into the db. Add clinical, genomic, and trial details as specified
    in config.json
    """
    # remove extra fields from trial_match output
    trial = dict()
    for key in trial_match.trial:
        if key in ['treatment_list', '_summary', 'status', '_id']:
            continue
        else:
            trial[key] = trial_match.trial[key]

    for results in trial_match.raw_query_results:
        for genomic_doc in results.genomic_docs:
            new_trial_match = {
                **format(results.clinical_doc),
                **format(get_genomic_details(genomic_doc, trial_match.multi_collection_query['genomic'])),
                **trial_match.match_clause_data.match_clause_additional_attributes,
                **trial,
                "query": trial_match.match_criterion
            }

            yield new_trial_match


async def main(args):
    count = 0
    print(args.trials)
    async for match in find_matches(sample_ids=args.samples, protocol_nos=args.trials):
        count += 1
        if count == 100:
            exit()


async def serve(args):
    host = args.serve[0]
    port = int(args.serve[1]) if len(args.serve) > 1 else 6379
    password = args.serve[2] if len(args.serve) > 2 else None
    import redis

    r = redis.Redis(host=host, port=port, password=password if password else None)
    trials = list()
    samples = list()
    if args.trials is None:
        with MongoDBConnection(read_only=True) as db:
            for protocol_no in await db.trial.distinct('protocol_no'):
                trials.append(protocol_no)
    if args.samples is None:
        with MongoDBConnection(read_only=True) as db:
            for sample_id in await db.clinical.distinct("SAMPLE_ID"):
                samples.append(sample_id)
    pipe = r.pipeline()

    def chunks(l, n):
        """Yield successive n-sized chunks from l."""
        for i in range(0, len(l), n):
            yield l[i:i + n]

    for protocol_no in trials:
        for sample_ids in chunks(samples, args.serve_batch):
            run_args = json.dumps({'protocol_nos': [protocol_no], 'sample_ids': sample_ids})
            pipe.lpush('job_queue', run_args)
    pipe.execute()


async def client(args):
    host = args.client[0]
    port = int(args.client[1]) if len(args.client) > 1 else 6379
    password = args.client[2] if len(args.client) > 2 else None
    import redis
    import time

    r = redis.Redis(host=host, port=port, password=password if password else None)
    while True:
        while not r.llen('job_queue'):
            print(1)
            time.sleep(1)
        run_args = json.loads(r.lpop('job_queue'))
        async for match in find_matches(**run_args):
            result_str = "{}".format(match)
            r.lpush('results', result_str)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "-trials",
        nargs="*",
        type=str,
        default=None
    )
    parser.add_argument(
        "-samples",
        nargs="*",
        type=str,
        default=None
    )
    parser.add_argument(
        '-serve',
        nargs="*",
        type=str,
        default=None
    )
    parser.add_argument(
        '-client',
        nargs="*",
        type=str,
        default=None
    )
    parser.add_argument(
        '-serve_batch',
        nargs=1,
        type=int,
        default=200
    )
    args = parser.parse_args()
    if args.serve is not None:
        asyncio.run(serve(args))

    elif args.client is not None:
        asyncio.run(client(args))
    else:
        asyncio.run(main(args))
        # logging.info("{}".format(match))
    # find_matches(protocol_nos=['17-251'])
    # list(find_matches(sample_ids=["BL-17-W40535"], protocol_nos=["18-626"]))
    # for trial_match in find_matches(protocol_nos=['16-265']):
    #     pass
    # print(trial_match)
    # find_matches(sample_ids=None, protocol_nos=None)
