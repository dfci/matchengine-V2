import time

from networkx.drawing.nx_agraph import graphviz_layout
from pymongo import UpdateMany, InsertOne
from pymongo.errors import AutoReconnect, CursorNotFound

from match_criteria_transform import MatchCriteriaTransform, query_node_transform
from mongo_connection import MongoDBConnection
from collections import deque, defaultdict
from typing import Generator, AsyncGenerator
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


async def updater_worker(worker_id, q) -> None:
    if not q.empty():
        with MongoDBConnection(read_only=False) as rw_db:
            while not q.empty():
                task: Union[list, int] = await q.get()
                try:
                    # log.info(
                    #     "Worker: {} got new UpdateTask {}".format(worker_id, count3)),
                    await rw_db.trial_match_test.bulk_write(task, ordered=False)
                    q.task_done()
                except Exception as e:
                    log.error("ERROR: Worker: {}, error: {}".format(worker_id, e))
                    if isinstance(e, AutoReconnect):
                        q.task_done()
                        await q.put(task)
                    else:
                        raise e


async def update_trial_matches(trial_matches: List[Dict], protocol_no: str, sample_ids: List[str], num_workers: int):
    """
    Update trial matches by diff'ing the newly created trial matches against existing matches in the db.
    'Delete' matches by adding {is_disabled: true} and insert all new matches.
    :param num_workers:
    :param protocol_no:
    :param trial_matches:
    :param sample_ids:
    :return:
    """
    q = asyncio.queues.Queue()
    trial_matches_by_sample_id = defaultdict(list)
    for trial_match in trial_matches:
        trial_matches_by_sample_id[trial_match['sample_id']].append(trial_match)
    if sample_ids is None:
        sample_ids = list(trial_matches_by_sample_id.keys())
    with MongoDBConnection(read_only=True) as db:
        for sample_id in sample_ids:
            # log.info("Sample ID {}".format(sample_id))
            new_matches_hashes = [match['hash'] for match in trial_matches_by_sample_id[sample_id]]

            query = {'hash': {'$in': new_matches_hashes}}

            trial_matches_to_not_change = {result['hash']: result.setdefault('is_disabled', False)
                                           for result
                                           in await db.trial_match_test.find(query,
                                                                             {"hash": 1,
                                                                              "is_disabled": 1}).to_list(None)}

            delete_where = {'hash': {'$nin': new_matches_hashes}, 'sample_id': sample_id, 'protocol_no': protocol_no}
            update = {"$set": {'is_disabled': True}}

            trial_matches_to_insert = [trial_match
                                       for trial_match in trial_matches_by_sample_id[sample_id]
                                       if trial_match['hash'] not in trial_matches_to_not_change]
            trial_matches_to_mark_available = [trial_match
                                               for trial_match in trial_matches_by_sample_id[sample_id]
                                               if trial_match['hash'] in trial_matches_to_not_change
                                               and trial_matches_to_not_change.setdefault('is_disabled', False)]

            ops = list()
            ops.append(UpdateMany(delete_where, update))
            for to_insert in trial_matches_to_insert:
                ops.append(InsertOne(to_insert))
            ops.append(UpdateMany({'hash': {'$in': trial_matches_to_mark_available}}, {'$set': {'is_disabled': False}}))
            await q.put(ops)

    workers = [asyncio.create_task(updater_worker(i, q))
               for i in range(0, min(q.qsize(), num_workers))]
    await asyncio.gather(*workers)
    await q.join()


def check_indices():
    """
    Ensure indexes exist on the trial_match collection so queries are performant
    :return:
    """
    with MongoDBConnection(read_only=False, async_init=False) as db:
        indexes = db.trial_match_test.list_indexes()
        existing_indexes = set()
        desired_indexes = {'hash', 'mrn', 'sample_id', 'clinical_id', 'protocol_no'}
        for index in indexes:
            index_key = list(index['key'].to_dict().keys())[0]
            existing_indexes.add(index_key)
        indexes_to_create = desired_indexes - existing_indexes
        for index in indexes_to_create:
            log.info('Creating index %s' % index)
            db.trial_match_test.create_index(index)


class MatchEngine(object):
    cache: Cache
    config: Dict
    match_criteria_transform: MatchCriteriaTransform
    protocol_nos: Union[List[str], None]
    sample_ids: Union[List[str], None]
    match_on_closed: bool
    match_on_deceased: bool
    debug: bool
    num_workers: int
    clinical_ids: Set[ClinicalID]
    _match_q: asyncio.queues.Queue
    matches: Dict[str, Dict[str, List[Dict]]]
    _loop: asyncio.AbstractEventLoop
    _queue_task_count: int
    _workers: Dict[int, asyncio.Task]

    def __enter__(self):
        return self

    async def _async_exit(self):
        for _ in range(0, self.num_workers):
            await self._match_q.put(PoisonPill())
        await self._match_q.join()

    def __exit__(self, exc_type, exc_val, exc_tb):
        self._async_db_ro.__exit__(exc_type, exc_val, exc_tb)
        self._db_ro.__exit__(exc_type, exc_val, exc_tb)
        self._loop.run_until_complete(self._async_exit())
        self._loop.stop()

    def __init__(self,
                 cache: Cache = None, sample_ids: Set[str] = None, protocol_nos: Set[str] = None,
                 match_on_deceased: bool = False, match_on_closed: bool = False, debug: bool = False,
                 num_workers: int = 25):
        self.cache = Cache() if cache is None else cache
        self.sample_ids = sample_ids
        self.protocol_nos = protocol_nos
        self.match_on_closed = match_on_closed
        self.match_on_deceased = match_on_deceased
        self.debug = debug
        self.num_workers = num_workers
        self._db_ro = MongoDBConnection(read_only=True, async_init=False)
        self.db_ro = self._db_ro.__enter__()
        self._loop = asyncio.new_event_loop()
        asyncio.set_event_loop(self._loop)
        self._queue_task_count = int()
        self.matches = defaultdict(lambda: defaultdict(list))

        with open("config/config.json") as config_file_handle:
            self.config = json.load(config_file_handle)

        self.match_criteria_transform = MatchCriteriaTransform(self.config)

        self.trials = self.get_trials()
        self.clinical_ids = self.get_clinical_ids_from_sample_ids()
        self._loop.run_until_complete(self._async_init())

    async def _async_init(self):
        self._match_q = asyncio.queues.Queue()
        self._async_db_ro = MongoDBConnection(read_only=True)
        self.async_db_ro = self._async_db_ro.__enter__()
        self._workers = {
            worker_id: self._loop.create_task(self.queue_worker(worker_id))
            for worker_id in range(0, self.num_workers)
        }
        pass

    async def execute_clinical_queries(self,
                                       multi_collection_query: MultiCollectionQuery,
                                       clinical_ids: Set[ClinicalID]) -> Tuple[Set[ObjectId],
                                                                               List[ClinicalMatchReason]]:
        collection = self.match_criteria_transform.CLINICAL
        reasons = list()
        for query_node in multi_collection_query.clinical:
            for query_part in query_node.query_parts:
                if not query_part.render:
                    continue
                # inner_query_part = {k: v}

                # hash the inner query to use as a reference for returned clinical ids, if necessary
                query_hash = query_part.hash()
                if query_hash not in self.cache.ids:
                    self.cache.ids[query_hash] = dict()

                # create a nested id_cache where the key is the clinical ID being queried and the vals
                # are the clinical IDs returned
                id_cache = self.cache.ids[query_hash]
                queried_ids = id_cache.keys()
                need_new = clinical_ids - set(queried_ids)

                if need_new:
                    new_query = {'$and': [{'_id': {'$in': list(need_new)}}, query_part.query]}
                    if self.debug:
                        log.info("{}".format(new_query))
                    docs = await self.async_db_ro[collection].find(new_query, {'_id': 1}).to_list(None)

                    # save returned ids
                    for doc in docs:
                        id_cache[doc['_id']] = doc['_id']

                    # save IDs NOT returned as None so if a query is run in the future which is the same, it will skip
                    for unfound in need_new - set(id_cache.keys()):
                        id_cache[unfound] = None

                for clinical_id in list(clinical_ids):

                    # an exclusion criteria returned a clinical document hence doc is not a match
                    if id_cache[clinical_id] is not None and query_part.negate:
                        clinical_ids.remove(clinical_id)

                    # clinical doc fulfills exclusion criteria
                    elif id_cache[clinical_id] is None and query_part.negate:
                        pass

                    # doc meets inclusion criteria
                    elif id_cache[clinical_id] is not None and not query_part.negate:
                        pass

                    # no clinical doc returned for an inclusion critera query, so remove _id from future queries
                    elif id_cache[clinical_id] is None and not query_part.negate:
                        clinical_ids.remove(clinical_id)

        for clinical_id in clinical_ids:
            for query_node in multi_collection_query.clinical:
                reasons.append(ClinicalMatchReason(query_node, clinical_id))
        return clinical_ids, reasons

    async def execute_genomic_queries(self,
                                      multi_collection_query: MultiCollectionQuery,
                                      clinical_ids: Set[ClinicalID],
                                      debug: bool = False) -> Tuple[Dict[ObjectId, Set[ObjectId]],
                                                                    List[GenomicMatchReason]]:
        all_results: Dict[ObjectId, Set[ObjectId]] = defaultdict(set)
        reasons = list()
        for genomic_query_node in multi_collection_query.genomic:

            join_field = self.match_criteria_transform.collection_mappings['genomic']['join_field']

            # for negate, query in queries:
            query = genomic_query_node.query_parts_to_single_query()
            uniq = comparable_dict(query).hash()
            clinical_ids = clinical_ids
            if uniq not in self.cache.ids:
                self.cache.ids[uniq] = dict()
            need_new = clinical_ids - set(self.cache.ids[uniq].keys())
            if need_new:
                new_query = query
                new_query['$and'] = new_query.setdefault('$and', list())
                new_query['$and'].insert(0,
                                         {join_field:
                                              {'$in': list(need_new)}})
                if debug:
                    log.info("{}".format(new_query))
                cursor = await self.async_db_ro['genomic'].find(new_query, {"_id": 1, "CLINICAL_ID": 1}).to_list(None)
                for result in cursor:
                    if result["CLINICAL_ID"] not in self.cache.ids[uniq]:
                        self.cache.ids[uniq][result["CLINICAL_ID"]] = set()
                    self.cache.ids[uniq][result["CLINICAL_ID"]].add(result["_id"])
                for unfound in need_new - set(self.cache.ids[uniq].keys()):
                    self.cache.ids[uniq][unfound] = None
            clinical_result_ids = set()
            for query_clinical_id in clinical_ids:
                if self.cache.ids[uniq][query_clinical_id] is not None:
                    genomic_ids = self.cache.ids[uniq][query_clinical_id]
                    clinical_result_ids.add(query_clinical_id)
                    for genomic_id in genomic_ids:
                        if not genomic_query_node.exclusion:
                            all_results[query_clinical_id].add(genomic_id)
                            reasons.append(GenomicMatchReason(genomic_query_node, query_clinical_id, genomic_id))
                        elif genomic_query_node.exclusion and query_clinical_id in all_results:
                            del all_results[query_clinical_id]
                elif self.cache.ids[uniq][query_clinical_id] is None and genomic_query_node.exclusion:
                    if query_clinical_id not in all_results:
                        all_results[query_clinical_id] = set()
                    reasons.append(GenomicMatchReason(genomic_query_node, query_clinical_id, None))
            if not genomic_query_node.exclusion:
                clinical_ids.intersection_update(clinical_result_ids)
            else:
                clinical_ids.difference_update(clinical_result_ids)
            if not clinical_ids:
                return dict(), list()
            else:
                for id_to_remove in set(all_results.keys()) - clinical_ids:
                    del all_results[id_to_remove]
        return all_results, reasons

    async def run_query(self,
                        multi_collection_query: MultiCollectionQuery,
                        initial_clinical_ids: Set[ClinicalID]) -> List[MatchReason]:
        """
        Execute a mongo query on the clinical and genomic collections to find trial matches.
        First execute the clinical query. If no records are returned short-circuit and return.

        :param initial_clinical_ids:
        :param multi_collection_query:
        :param db:
        :param match_criteria_transformer:
        :return:
        """
        clinical_ids = set(initial_clinical_ids)

        new_clinical_ids, clinical_match_reasons = await self.execute_clinical_queries(multi_collection_query,
                                                                                       clinical_ids
                                                                                       if clinical_ids
                                                                                       else set(initial_clinical_ids))
        clinical_ids = new_clinical_ids
        # If no clinical docs are returned, skip executing genomic portion of the query
        if not clinical_ids:
            return list()

        # iterate over all queries
        all_results, genomic_match_reasons = await self.execute_genomic_queries(multi_collection_query,
                                                                                clinical_ids
                                                                                if clinical_ids
                                                                                else set(
                                                                                    initial_clinical_ids))

        needed_clinical = list()
        needed_genomic = list()
        for clinical_id, genomic_ids in all_results.items():
            if clinical_id not in self.cache.docs:
                needed_clinical.append(clinical_id)
            for genomic_id in genomic_ids:
                if genomic_id not in self.cache.docs:
                    needed_genomic.append(genomic_id)

        # matching criteria for clinical and genomic values can be set/extended in config.json
        genomic_projection = self.match_criteria_transform.genomic_projection
        clinical_projection = self.match_criteria_transform.clinical_projection

        results = await asyncio.gather(self.perform_db_call("clinical",
                                                            {"_id": {"$in": list(needed_clinical)}},
                                                            clinical_projection),
                                       self.perform_db_call("genomic",
                                                            {"_id": {"$in": list(needed_genomic)}},
                                                            genomic_projection))
        for outer_result in results:
            for result in outer_result:
                self.cache.docs[result["_id"]] = result
        return [
            genomic_reason
            for genomic_reason in genomic_match_reasons
            if genomic_reason.clinical_id in all_results
               and (genomic_reason.query_node.exclusion
                    or genomic_reason.genomic_id in all_results[genomic_reason.clinical_id])
        ]

    async def queue_worker(self, worker_id) -> None:
        while True:
            task: Union[QueryTask, PoisonPill] = await self._match_q.get()
            if isinstance(task, PoisonPill):
                log.info(
                    "Worker: {} got PoisonPill".format(worker_id))
                self._match_q.task_done()
                break
            elif isinstance(task, QueryTask):
                log.info(
                    "Worker: {}, protocol_no: {} got new QueryTask".format(worker_id,
                                                                           task.trial['protocol_no']))
                try:
                    results = await self.run_query(task.query, task.clinical_ids)
                except Exception as e:
                    log.error("ERROR: Worker: {}, error: {}".format(worker_id, e))
                    results = list()
                    if isinstance(e, AutoReconnect):
                        await self._match_q.put(task)
                        self._match_q.task_done()
                    elif isinstance(e, CursorNotFound):
                        await self._match_q.put(task)
                        self._match_q.task_done()
                    else:
                        raise e
                for result in results:
                    self._queue_task_count += 1
                    if self._queue_task_count % 100 == 0:
                        log.info("Trial match count: {}".format(self._queue_task_count))
                    match_document = self.create_trial_matches(TrialMatch(task.trial,
                                                                          task.match_clause_data,
                                                                          task.match_path,
                                                                          task.query,
                                                                          result))
                    self.matches[task.trial['protocol_no']][match_document['sample_id']].append(
                        match_document)
                self._match_q.task_done()

    def extract_match_clauses_from_trial(self,
                                         protocol_no: str) -> Generator[MatchClauseData, None, None]:
        """
        Pull out all of the matches from a trial curation.
        Return the parent path and the values of that match clause.

        Default to only extracting match clauses on steps, arms or dose levels which are open to accrual unless otherwise
        specified

        :param match_criteria_transform:
        :param match_on_closed:
        :param trial:
        :return:
        """

        trial = self.trials[protocol_no]
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
                        if not self.match_on_closed:
                            match_level = path[-1]
                            # suspension_key = self.match_criteria_transform.suspension_mapping.setdefault(match_level,
                            #                                                                              None)
                            if match_level == 'arm':
                                if parent_value.setdefault('arm_suspended', 'n').lower().strip() == 'y':
                                    continue
                            elif match_level == 'dose_level':
                                if parent_value.setdefault('level_suspended', 'n').lower().strip() == 'y':
                                    continue
                            elif match_level == 'step':
                                if all([arm.setdefault('arm_suspended', 'n').lower().strip() == 'y'
                                        for arm in parent_value.setdefault('arm', list({'arm_suspended': 'y'}))]):
                                    continue

                        parent_path = ParentPath(path + (parent_key, inner_key))
                        level = MatchClauseLevel(
                            self.match_criteria_transform.level_mapping[
                                [item for item in parent_path[::-1] if not isinstance(item, int) and item != 'match'][
                                    0]])

                        internal_id = parent_value[self.match_criteria_transform.internal_id_mapping[level]]
                        yield MatchClauseData(inner_value,
                                              internal_id,
                                              parent_path,
                                              level,
                                              parent_value,
                                              trial['protocol_no'])
                    else:
                        process_q.append((path + (parent_key,), inner_key, inner_value))
            elif isinstance(parent_value, list):
                for index, item in enumerate(parent_value):
                    process_q.append((path + (parent_key,), index, item))

    def create_match_tree(self, match_clause_data: MatchClauseData) -> MatchTree:
        match_clause = match_clause_data.match_clause
        process_q: deque[Tuple[NodeID, Dict[str, Any]]] = deque()
        graph = nx.DiGraph()
        node_id: NodeID = NodeID(1)
        graph.add_node(0)  # root node is 0
        graph.nodes[0]['criteria_list'] = list()
        graph.nodes[0]['is_and'] = True
        graph.nodes[0]['or_nodes'] = set()
        graph.nodes[0]['label'] = '0 - ROOT and'
        graph.nodes[0]['label_list'] = list()
        for item in match_clause:
            if any([k.startswith('or') for k in item.keys()]):
                process_q.appendleft((NodeID(0), item))
            else:
                process_q.append((NodeID(0), item))

        def grapth_match_clause():
            import matplotlib.pyplot as plt
            labels = {node: graph.nodes[node]['label'] for node in graph.nodes}
            for node in graph.nodes:
                if graph.nodes[node]['label_list']:
                    labels[node] = labels[node] + ' [' + ','.join(graph.nodes[node]['label_list']) + ']'
            pos = graphviz_layout(graph, prog="dot", root=0)
            plt.figure(figsize=(30, 30))
            nx.draw_networkx(graph, pos, with_labels=True, node_size=[600 for _ in graph.nodes], labels=labels)
            # plt.show()
            return plt

        while process_q:
            parent_id, values = process_q.pop()
            parent_is_or = True if graph.nodes[parent_id].setdefault('is_or', False) else False
            parent_is_and = True if graph.nodes[parent_id].setdefault('is_and', False) else False
            for label, value in values.items():  # label is 'and', 'or', 'genomic' or 'clinical'
                if label.startswith('and'):
                    criteria_list = list()
                    label_list = list()
                    for item in value:
                        for inner_label, inner_value in item.items():
                            if inner_label.startswith("or"):
                                process_q.appendleft((parent_id if parent_is_and else node_id, item))
                            elif inner_label.startswith("and"):
                                process_q.append((parent_id if parent_is_and else node_id, item))
                            else:
                                criteria_list.append(item)
                                label_list.append(inner_label)
                    if parent_is_and:
                        graph.nodes[parent_id]['criteria_list'].extend(criteria_list)
                        graph.nodes[parent_id]['label_list'].extend(label_list)
                    else:
                        graph.add_edges_from([(parent_id, node_id)])
                        graph.nodes[node_id].update({
                            'criteria_list': criteria_list,
                            'is_and': True,
                            'is_or': False,
                            'or_nodes': set(),
                            'label': str(node_id) + ' - ' + label,
                            'label_list': label_list
                        })
                        node_id += 1
                elif label.startswith("or"):
                    or_node_id = node_id
                    graph.add_node(or_node_id)
                    graph.nodes[or_node_id].update({
                        'criteria_list': list(),
                        'is_and': False,
                        'is_or': True,
                        'label': str(or_node_id) + ' - ' + label,
                        'label_list': list()
                    })
                    node_id += 1
                    for item in value:
                        process_q.append((or_node_id, item))
                    if parent_is_and:
                        parent_or_nodes = graph.nodes[parent_id]['or_nodes']
                        if not parent_or_nodes:
                            graph.add_edges_from([(parent_id, or_node_id)])
                            graph.nodes[parent_id]['or_nodes'] = {or_node_id}
                        else:
                            successors = [
                                (successor, or_node_id)
                                for parent_or_node in parent_or_nodes
                                for successor in nx.descendants(graph, parent_or_node)
                                if graph.out_degree(successor) == 0
                            ]
                            graph.add_edges_from(successors)
                    else:
                        graph.add_edge(parent_id, or_node_id)
                else:
                    if parent_is_and:
                        graph.nodes[parent_id]['criteria_list'].append(values)
                        graph.nodes[parent_id]['label_list'].append(label)
                    else:
                        graph.add_node(node_id)
                        graph.nodes[node_id].update({
                            'criteria_list': [values],
                            'is_or': False,
                            'is_and': True,
                            'label': str(node_id) + ' - ' + label,
                            'label_list': list()
                        })
                        graph.add_edge(parent_id, node_id)
                        node_id += 1
        return MatchTree(graph)

    def get_match_paths(self, match_tree: MatchTree) -> Generator[MatchCriterion, None, None]:
        leaves = list()
        for node in match_tree.nodes:
            if match_tree.out_degree(node) == 0:
                leaves.append(node)
        for leaf in leaves:
            for path in nx.all_simple_paths(match_tree, 0, leaf) if leaf != 0 else [[leaf]]:
                match_path = MatchCriterion(list())
                for node in path:
                    if match_tree.nodes[node]['criteria_list']:
                        match_path.append(match_tree.nodes[node]['criteria_list'])
                if match_path:
                    yield match_path

    def translate_match_path(self,
                             match_clause_data: MatchClauseData,
                             match_criterion: MatchCriterion) -> MultiCollectionQuery:
        """
        Translate the keys/values from the trial curation into keys/values used in a genomic/clinical document.
        Uses an external config file ./config/config.json

        :param match_clause_data:
        :param match_criterion:
        :param match_criteria_transformer:
        :return:
        """
        multi_collection_query = MultiCollectionQuery(list(), list())
        query_cache = set()
        for node in match_criterion:
            for criteria in node:
                for genomic_or_clinical, values in criteria.items():
                    query_node = QueryNode(genomic_or_clinical, list(), None)
                    for trial_key, trial_value in values.items():
                        trial_key_settings = self.match_criteria_transform.trial_key_mappings[
                            genomic_or_clinical].setdefault(
                            trial_key.upper(),
                            dict())

                        if trial_key_settings.setdefault('ignore', False):
                            continue

                        sample_value_function_name = trial_key_settings.setdefault('sample_value', 'nomap')
                        sample_function = getattr(MatchCriteriaTransform, sample_value_function_name)
                        sample_function_args = dict(sample_key=trial_key.upper(),
                                                    trial_value=trial_value,
                                                    parent_path=match_clause_data.parent_path,
                                                    trial_path=genomic_or_clinical,
                                                    trial_key=trial_key,
                                                    query_node=query_node)
                        sample_function_args.update(trial_key_settings)
                        sample_value, negate = sample_function(self.match_criteria_transform, **sample_function_args)
                        query_part = QueryPart(sample_value, negate, True)
                        query_node.query_parts.append(query_part)
                        # set the exclusion = True on the query node if ANY of the query parts are negate
                        query_node.exclusion = True if negate or query_node.exclusion else False
                    if query_node.exclusion is not None:
                        query_node_transform(query_node)
                        query_node_hash = query_node.hash()
                        if query_node_hash not in query_cache:
                            getattr(multi_collection_query, genomic_or_clinical).append(query_node)
                            query_cache.add(query_node_hash)
        return multi_collection_query

    def get_matches_for_all_trials(self) -> Dict[str, Dict[str, List]]:
        for protocol_no in self.trials.keys():
            self.get_matches_for_trial(protocol_no)
        return self.matches

    def get_matches_for_trial(self, protocol_no: str):
        log.info("Begin Protocol No: {}".format(protocol_no))
        task = self._loop.create_task(self._async_get_matches_for_trial(protocol_no))
        return self._loop.run_until_complete(task)

    async def _async_get_matches_for_trial(self, protocol_no: str) -> Dict[str, List[Dict]]:
        trial = self.trials[protocol_no]
        match_clauses = self.extract_match_clauses_from_trial(protocol_no)
        for match_clause in match_clauses:
            match_paths = self.get_match_paths(self.create_match_tree(match_clause))
            for match_path in match_paths:
                query = self.translate_match_path(match_clause, match_path)
                if self.debug:
                    log.info("Query: {}".format(query))
                if query:
                    await self._match_q.put(QueryTask(trial,
                                                      match_clause,
                                                      match_path,
                                                      query,
                                                      self.clinical_ids))
        await self._match_q.join()
        logging.info("Total results: {}".format(len(self.matches[protocol_no])))
        return self.matches[protocol_no]

    def get_clinical_ids_from_sample_ids(self) -> Set[ClinicalID]:
        # if no sample ids are passed in as args, get all clinical documents
        query = {} if self.match_on_deceased else {"VITAL_STATUS": 'alive'}
        if self.sample_ids is not None:
            query.update({"SAMPLE_ID": {"$in": self.sample_ids}})
        with MongoDBConnection(read_only=True, async_init=False) as db:
            results = {result['_id'] for result in db.clinical.find(query, {'_id': 1})}
        return results

    def get_trials(self) -> Dict[str, Trial]:
        trial_find_query = dict()

        # matching criteria can be set and extended in config.json. for more details see the README
        projection = self.match_criteria_transform.trial_projection

        if self.protocol_nos is not None:
            trial_find_query['protocol_no'] = {"$in": [protocol_no for protocol_no in self.protocol_nos]}

        all_trials = {
            result['protocol_no']: result
            for result in self.db_ro.trial.find(trial_find_query, projection)
        }
        if self.match_on_closed:
            return all_trials
        else:
            open_trials = dict()
            for protocol_no, trial in all_trials.items():
                if trial['status'].lower().strip() not in {"open to accrual"}:
                    logging.info('Trial %s is closed, skipping' % trial['protocol_no'])
                else:
                    open_trials.update({protocol_no: trial})
            return open_trials

    async def perform_db_call(self, collection, query, projection):
        return await self.async_db_ro[collection].find(query, projection).to_list(None)

    def create_trial_matches(self, trial_match: TrialMatch) -> Dict:
        """
        Create a trial match document to be inserted into the db. Add clinical, genomic, and trial details as specified
        in config.json
        :param cache:
        :param trial_match:
        :return:
        """
        genomic_doc = self.cache.docs.setdefault(trial_match.match_reason.genomic_id, None)
        query = trial_match.match_reason.query_node.query_parts_to_single_query()

        new_trial_match = dict()
        new_trial_match.update(format(self.cache.docs[trial_match.match_reason.clinical_id]))

        if genomic_doc is None:
            new_trial_match.update(format(format_exclusion_match(query)))
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
        new_trial_match["is_disabled"] = False
        new_trial_match.update(
            {'match_path': '.'.join([str(item) for item in trial_match.match_clause_data.parent_path])})
        return new_trial_match


def main(args):
    check_indices()
    with MatchEngine(
            sample_ids=args.samples,
            protocol_nos=args.trials,
            match_on_closed=args.match_on_closed,
            match_on_deceased=args.match_on_deceased,
            debug=args.debug,
            num_workers=args.workers[0]
    ) as me:
        matches = me.get_matches_for_all_trials()
    # all_new_matches = find_matches(sample_ids=args.samples,
    #                                protocol_nos=args.trials,
    #                                num_workers=args.workers[0],
    #                                match_on_closed=args.match_on_closed,
    #                                match_on_deceased=args.match_on_deceased,
    #                                debug=args.debug)
    # async for protocol_no, sample_ids, matches in all_new_matches:
    #     log.info("{} all matches: {}".format(protocol_no, len(matches)))
    #     if not args.dry:
    #         await update_trial_matches(matches, protocol_no, sample_ids, args.workers[0])


if __name__ == "__main__":
    # todo unit tests
    # todo refactor run_query
    # todo output CSV file functions
    # todo regex SV matching
    # todo update/delete/insert run log
    # todo failsafes for insert logic (fallback?)
    # todo trial_match_test -> trial_match
    # todo increase db cursor timeout
    # todo trial_match view (for sort_order)
    # todo configuration of trial_match document logic
    # todo - squash clinical criteria for age
    # todo index every field in projections?

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
    dry_help = "Execute a full matching run but do not insert any matches into the DB"
    debug_help = "Enable debug logging"

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
    subp_p.add_argument("--dry-run", dest="dry", action="store_true", default=False, help=dry_help)
    subp_p.add_argument("--debug", dest="debug", action="store_true", default=False, help=debug_help)
    subp_p.add_argument("--match-on-deceased-patients",
                        dest="match_on_deceased",
                        action="store_true",
                        help=deceased_help)
    subp_p.add_argument("-workers", nargs=1, type=int, default=[cpu_count() * 5])
    subp_p.add_argument('-o', dest="outpath", required=False, help=param_outpath_help)
    args = parser.parse_args()
    # args.func(args)
    main(args)
