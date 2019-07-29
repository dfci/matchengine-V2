from __future__ import annotations
import uuid
import json
import asyncio
import logging
from collections import defaultdict
from multiprocessing import cpu_count

import pymongo
import datetime
from matchengine.utilities.mongo_connection import MongoDBConnection
from matchengine.utilities.utilities import (
    check_indices,
    find_plugins
)
from matchengine.match_criteria_transform import MatchCriteriaTransform
from matchengine.utilities.task_utils import (
    run_query_task,
    run_poison_pill,
    run_update_task,
    run_run_log_update_task
)
from matchengine.match_translator import (
    extract_match_clauses_from_trial,
    create_match_tree,
    get_match_paths,
    translate_match_path
)
from matchengine.utilities.query_utils import (
    execute_clinical_queries,
    execute_genomic_queries,
    get_needed_ids,
    get_query_results,
    get_valid_genomic_reasons,
    get_valid_clinical_reasons
)
from matchengine.utilities.update_match_utils import async_update_matches_by_protocol_no
from matchengine.utilities.matchengine_types import (
    PoisonPill,
    Cache,
    QueryTask,
    UpdateTask,
    RunLogUpdateTask
)
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from matchengine.utilities.matchengine_types import (
        Dict,
        Union,
        List,
        Set,
        ClinicalID,
        MultiCollectionQuery,
        MatchReason,
        ObjectId,
        Trial,
        QueryNode,
        TrialMatch
    )
    from typing import NoReturn

logging.basicConfig(level=logging.INFO)
log = logging.getLogger('matchengine')


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
    _task_q: asyncio.queues.Queue
    _matches: Dict[str, Dict[str, List[Dict]]]
    _loop: asyncio.AbstractEventLoop
    _queue_task_count: int
    _workers: Dict[int, asyncio.Task]

    def __enter__(self):
        return self

    async def _async_exit(self):
        """
        Ensure that all async workers exit gracefully.
        """
        for _ in range(0, self.num_workers):
            await self._task_q.put(PoisonPill())
        await self._task_q.join()

    def __exit__(self, exception_type, exception_value, exception_traceback):
        """
        Teardown database connections (async + synchronous) and async workers gracefully.
        """
        if self.db_init:
            self._async_db_ro.__exit__(exception_type, exception_value, exception_traceback)
            self._async_db_rw.__exit__(exception_type, exception_value, exception_traceback)
            self._db_ro.__exit__(exception_type, exception_value, exception_traceback)
        self._loop.run_until_complete(self._async_exit())
        self._loop.stop()
        self._loop.close()

    def __init__(
            self,
            cache: Cache = None,
            sample_ids: Set[str] = None,
            protocol_nos: Set[str] = None,
            match_on_deceased: bool = False,
            match_on_closed: bool = False,
            debug: bool = False,
            num_workers: int = cpu_count() * 5,
            visualize_match_paths: bool = False,
            fig_dir: str = None,
            config: Union[str, dict] = None,
            plugin_dir: str = None,
            db_init: bool = True,
            match_document_creator_class: str = "DFCITrialMatchDocumentCreator",
            query_node_transformer_class: str = "DFCIQueryNodeTransformer",
            db_secrets_class: str = None,
            report_clinical_reasons: bool = True,
            ignore_run_log: bool = False
    ):

        self.starttime = datetime.datetime.now()
        self.run_id = uuid.uuid4()
        self.run_log_entries = dict()
        self.ignore_run_log = ignore_run_log
        self.clinical_run_log_entries = dict()
        self._protocol_nos_param = protocol_nos
        self._sample_ids_param = sample_ids

        if isinstance(config, str):
            with open(config) as config_file_handle:
                self.config = json.load(config_file_handle)
        else:
            self.config = config

        self.match_criteria_transform = MatchCriteriaTransform(self.config)

        self.plugin_dir = plugin_dir
        self.match_document_creator_class = match_document_creator_class
        self.query_node_transformer_class = query_node_transformer_class
        self.db_secrets_class = db_secrets_class
        find_plugins(self)
        self.db_init = db_init
        self._db_ro = MongoDBConnection(read_only=True, async_init=False) if self.db_init else None
        self.db_ro = self._db_ro.__enter__() if self.db_init else None
        self._db_rw = MongoDBConnection(read_only=False, async_init=False) if self.db_init else None
        self.db_rw = self._db_rw.__enter__() if self.db_init else None
        log.info(f"Connected to database {self.db_ro.name}")

        check_indices(self)
        # A cache-like object used to accumulate query results
        self.cache = Cache() if cache is None else cache
        self.sample_ids = sample_ids
        self.protocol_nos = protocol_nos
        self.match_on_closed = match_on_closed
        self.match_on_deceased = match_on_deceased
        self.report_clinical_reasons = report_clinical_reasons
        self.debug = debug
        self.num_workers = num_workers
        self.visualize_match_paths = visualize_match_paths
        self.fig_dir = fig_dir
        self._queue_task_count = int()
        self._matches = defaultdict(lambda: defaultdict(list))

        self.trials = self.get_trials()
        self._trials_to_match_on = self._get_trials_to_match_on(self.trials)
        if self.protocol_nos is None:
            self.protocol_nos = list(self.trials.keys())

        self._clinical_data = self._get_clinical_data()
        self.clinical_mapping = self.get_clinical_ids_from_sample_ids()
        self.clinical_update_mapping = self.get_clinical_updated_mapping()
        self.clinical_run_log_mapping = self.get_clinical_run_log_mapping()
        self.sample_mapping = {sample_id: clinical_id for clinical_id, sample_id in self.clinical_mapping.items()}
        self.clinical_ids = set(self.clinical_mapping.keys())
        if self.sample_ids is None:
            self.sample_ids = list(self.clinical_mapping.values())
        self._param_cache = dict()

        # instantiate a new async event loop to allow class to be used as if it is synchronous
        self._loop = asyncio.new_event_loop()
        asyncio.set_event_loop(self._loop)
        self._loop.run_until_complete(self._async_init())

    async def _async_init(self):
        """
        Instantiate asynchronous db connections and workers.
        Create a task que which holds all matching and update tasks for processing via workers.
        """
        self._task_q = asyncio.queues.Queue()
        self._async_db_ro = MongoDBConnection(read_only=True)
        self.async_db_ro = self._async_db_ro.__enter__()
        self._async_db_rw = MongoDBConnection(read_only=False)
        self.async_db_rw = self._async_db_rw.__enter__()
        self._workers = {
            worker_id: self._loop.create_task(self._queue_worker(worker_id))
            for worker_id in range(0, self.num_workers)
        }

    async def run_query(self,
                        multi_collection_query: MultiCollectionQuery,
                        initial_clinical_ids: Set[ClinicalID]) -> List[MatchReason]:
        """
        Execute a mongo query on the clinical and genomic collections to find trial matches.
        First execute the clinical query. If no records are returned short-circuit and return.
        """
        clinical_ids = set(initial_clinical_ids)
        new_clinical_ids, clinical_match_reasons = await execute_clinical_queries(self,
                                                                                  multi_collection_query,
                                                                                  clinical_ids
                                                                                  if clinical_ids
                                                                                  else set(initial_clinical_ids))
        clinical_ids = new_clinical_ids
        if not clinical_ids:
            return list()

        all_results, genomic_match_reasons = await execute_genomic_queries(self,
                                                                           multi_collection_query,
                                                                           clinical_ids
                                                                           if clinical_ids
                                                                           else set(initial_clinical_ids))

        needed_clinical, needed_genomic = get_needed_ids(all_results, self.cache.docs)
        results = await get_query_results(self, needed_clinical, needed_genomic)

        # asyncio.gather returns [[],[]]. Save the resulting values on the cache for use when creating trial matches
        for outer_result in results:
            for result in outer_result:
                self.cache.docs[result["_id"]] = result

        valid_genomic_reasons: List[MatchReason] = get_valid_genomic_reasons(genomic_match_reasons, all_results)
        valid_clinical_reasons: List[MatchReason] = get_valid_clinical_reasons(self, clinical_match_reasons,
                                                                               all_results)

        return valid_genomic_reasons + valid_clinical_reasons

    async def _queue_worker(self, worker_id: int) -> None:
        """
        Function which executes tasks placed on the task queue.
        """
        while True:
            # Execute update task
            task: Union[QueryTask, UpdateTask, PoisonPill] = await self._task_q.get()
            if isinstance(task, PoisonPill):
                await run_poison_pill(self, worker_id)
                break

            elif isinstance(task, QueryTask):
                await run_query_task(self, task, worker_id)

            elif isinstance(task, UpdateTask):
                await run_update_task(self, task, worker_id)

            elif isinstance(task, RunLogUpdateTask):
                await run_run_log_update_task(self, task, worker_id)

    def query_node_transform(self, query_node: QueryNode) -> NoReturn:
        """Stub function to be overriden by plugin"""
        pass

    def update_matches_for_protocol_number(self, protocol_no):
        """
        Updates all trial matches for a given protocol number
        """
        self._loop.run_until_complete(async_update_matches_by_protocol_no(self, protocol_no))

    def update_all_matches(self):
        """
        Synchronously iterates over each protocol number, updating the matches in the database for each
        """
        for protocol_number in self.protocol_nos:
            self.update_matches_for_protocol_number(protocol_number)

    def get_matches_for_all_trials(self) -> Dict[str, Dict[str, List]]:
        """
        Synchronously iterates over each protocol number, getting trial matches for each
        """
        for protocol_no in self.protocol_nos:
            if protocol_no not in self._trials_to_match_on:
                logging.info((f'Trial {protocol_no} '
                              f'has status {self.trials[protocol_no]["status"]}, skipping'))
                continue
            self.get_matches_for_trial(protocol_no)
        return self._matches

    def get_matches_for_trial(self, protocol_no: str):
        """
        Get the trial matches for a given protocol number
        """
        log.info(f"Begin Protocol No: {protocol_no}")
        task = self._loop.create_task(self._async_get_matches_for_trial(protocol_no))
        return self._loop.run_until_complete(task)

    async def _async_get_matches_for_trial(self, protocol_no: str) -> Dict[str, List[Dict]]:
        """
        Asynchronous function used by get_matches_for_trial, not meant to be called externally.
        Gets the matches for a given trial
        """
        # Get each match clause in the trial document
        trial = self.trials[protocol_no]
        match_clauses = extract_match_clauses_from_trial(self, protocol_no)

        clinical_ids_to_run = self.get_clinical_ids_for_protocol(protocol_no)
        self.create_run_log_entry(protocol_no, clinical_ids_to_run)

        # for each match clause, create the match tree, and extract each possible match path from the tree
        for match_clause in match_clauses:
            match_tree = create_match_tree(self, match_clause)
            match_paths = get_match_paths(match_tree)

            # for each match path, translate the path into valid mongo queries
            for match_path in match_paths:
                queries = translate_match_path(self, match_clause, match_path)
                for query in queries:
                    if self.debug:
                        log.info(f"Query: {query}")
                    if query:
                        # put the query onto the task queue for execution
                        await self._task_q.put(QueryTask(trial,
                                                         match_clause,
                                                         match_path,
                                                         query,
                                                         clinical_ids_to_run))
        await self._task_q.join()
        logging.info(f"Total results: {len(self._matches[protocol_no])}")
        return self._matches[protocol_no]

    def _get_clinical_data(self):
        # if no sample ids are passed in as args, get all clinical documents
        query: Dict = {} if self.match_on_deceased else {"VITAL_STATUS": 'alive'}
        if self.sample_ids is not None:
            query.update({"SAMPLE_ID": {"$in": list(self.sample_ids)}})
        return {result['_id']: result
                for result in
                self.db_ro.clinical.find(query, {'_id': 1, 'SAMPLE_ID': 1, '_updated': 1, 'run_history': 1})}

    def get_clinical_updated_mapping(self) -> Dict[ObjectId: datetime.datetime]:
        return {clinical_id: clinical_data.get('_updated', None) for clinical_id, clinical_data in
                self._clinical_data.items()}

    def get_clinical_run_log_mapping(self) -> Dict[ObjectId: ObjectId]:
        return {clinical_id: clinical_data.get('run_history', None) for clinical_id, clinical_data in
                self._clinical_data.items()}

    def get_clinical_ids_from_sample_ids(self) -> Dict[ClinicalID, str]:
        """
        Clinical ids are unique to sample_ids
        """
        return {clinical_id: clinical_data['SAMPLE_ID'] for clinical_id, clinical_data in self._clinical_data.items()}

    def get_trials(self) -> Dict[str, Trial]:
        """
        Gets all the trial documents in the database, or just the relevant trials (if protocol numbers supplied)
        """
        trial_find_query = dict()

        # matching criteria can be set and extended in config.json. for more details see the README
        projection = self.match_criteria_transform.trial_projection

        if self.protocol_nos is not None:
            trial_find_query['protocol_no'] = {"$in": [protocol_no for protocol_no in self.protocol_nos]}

        all_trials = {
            result['protocol_no']: result
            for result in
            self.db_ro.trial.find(trial_find_query, dict({"_updated": 1, "last_updated": 1}, **projection))
        }
        return all_trials

    def _get_trials_to_match_on(self, trials: Dict[str, Trial]) -> Set[str]:
        return {
            protocol_no
            for protocol_no, trial
            in trials.items()
            if self.match_on_closed or trial.get('status', "key not found").lower().strip() in {"open to accrual"}
        }

    def create_run_log_entry(self, protocol_no, clinical_ids: Set[ClinicalID]):
        """
        Create a record of a matchengine run by protocol no.
        Include clinical ids ran during run. 'all' meaning all sample ids in the db, or a subsetted list
        Include original arguments.
        """
        run_log_clinical_ids_new = dict()
        if self._sample_ids_param is None:
            run_log_clinical_ids_new['all'] = None
        else:
            run_log_clinical_ids_new['list'] = list(self.clinical_ids)

        self.run_log_entries[protocol_no] = {
            'protocol_no': protocol_no,
            'clinical_ids': run_log_clinical_ids_new,
            'run_id': self.run_id.hex,
            'run_params': {
                'trials': self._protocol_nos_param,
                'sample_ids': self._sample_ids_param,
                'match_on_deceased': self.match_on_deceased,
                'match_on_closed': self.match_on_closed,
                'report_clinical_reasons': self.report_clinical_reasons,
                'workers': self.num_workers,
                'ignore_run_log': self.ignore_run_log
            },
            '_created': self.starttime
        }
        self.clinical_run_log_entries[protocol_no] = clinical_ids

    def get_clinical_ids_for_protocol(self, protocol_no: str) -> Set(ObjectId):
        """
        Take protocol from args and lookup all run_log entries after protocol_no._updated_date.
        Subset self.clinical_ids which have already been run since the trial's been updated.

        """

        if self.ignore_run_log:
            return self.clinical_ids
        # run logs since trial has been last updated
        default_datetime = 'January 01, 0001'
        fmt_string = '%B %d, %Y'
        trial_last_update = datetime.datetime.strptime(self.trials[protocol_no].get('last_updated', default_datetime),
                                                       fmt_string)
        query = {"protocol_no": protocol_no, "_created": {'$gte': trial_last_update}}
        run_log_entries = list(self.db_ro.run_log.find(query).sort([("_created", pymongo.DESCENDING)]))

        if not run_log_entries:
            return self.clinical_ids

        clinical_ids_to_not_run = set()
        clinical_ids_to_run = set()

        # Iterate over all run logs
        for run_log in run_log_entries:
            # All sample ids are accounted for, short circuit
            if clinical_ids_to_not_run.union(clinical_ids_to_run) == self.clinical_ids:
                break

            run_log_clinical_ids = run_log['clinical_ids']
            is_all = 'all' in run_log_clinical_ids
            run_log_created_at = run_log['_created']

            # Check if clinical_id has been updated since last run with current protocol.
            # If it has been updated, run.
            for clinical_id, updated_at in self.clinical_update_mapping.items():
                if updated_at is None:
                    clinical_ids_to_run.add(clinical_id)
                elif updated_at > run_log_created_at and clinical_id not in clinical_ids_to_not_run:
                    if is_all or clinical_id in run_log_clinical_ids['list']:
                        clinical_ids_to_run.add(clinical_id)

            if is_all:
                clinical_ids_to_not_run.update(self.clinical_ids - clinical_ids_to_run)
                continue

            elif 'list' in run_log_clinical_ids:
                run_prev = set(run_log_clinical_ids['list']).intersection(self.clinical_ids)
                run_now_not_run_prev = self.clinical_ids - run_prev
                clinical_ids_to_run.update(run_now_not_run_prev - clinical_ids_to_not_run)
                clinical_ids_to_not_run.update(run_prev - clinical_ids_to_run)

        # ensure that we have accounted for all clinical ids
        assert clinical_ids_to_run.union(clinical_ids_to_not_run) == self.clinical_ids

        return clinical_ids_to_run

    def create_trial_matches(self, trial_match: TrialMatch) -> Dict:
        """Stub function to be overriden by plugin"""
        return dict()

    @property
    def task_q(self):
        return self._task_q

    @property
    def loop(self):
        return self._loop

    @property
    def queue_task_count(self):
        return self._queue_task_count

    @queue_task_count.setter
    def queue_task_count(self, value):
        self._queue_task_count = value

    @property
    def matches(self):
        return self._matches
