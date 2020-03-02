from __future__ import annotations

import asyncio
import logging
import traceback
from collections import defaultdict
from typing import TYPE_CHECKING, List, Dict

from pymongo import InsertOne
from pymongo.errors import (
    AutoReconnect,
    CursorNotFound,
    ServerSelectionTimeoutError)

from matchengine.internals.utilities.list_utils import chunk_list
from matchengine.internals.typing.matchengine_types import (
    TrialMatch, IndexUpdateTask,
    MatchReason, UpdateTask,
    RunLogUpdateTask, ClinicalID
)
from matchengine.internals.utilities.object_comparison import nested_object_hash
from matchengine.internals.utilities.utilities import get_sort_order

if TYPE_CHECKING:
    from matchengine.internals.engine import MatchEngine

logging.basicConfig(level=logging.INFO)
log = logging.getLogger('matchengine')


async def run_check_indices_task(matchengine: MatchEngine, task, worker_id):
    """
    Ensure indexes exist on collections so queries are performant
    """
    if matchengine.debug:
        log.info(
            f"Worker: {worker_id}, got new CheckIndicesTask")
    try:
        for collection, desired_indices in matchengine.config['indices'].items():
            if collection == "trial_match":
                collection = matchengine.trial_match_collection
            indices = list()
            indices.extend(matchengine.db_ro[collection].list_indexes())
            existing_indices = set()
            for index in indices:
                index_key = list(index['key'].to_dict().keys())[0]
                existing_indices.add(index_key)
            indices_to_create = set(desired_indices) - existing_indices
            for index in indices_to_create:
                matchengine.task_q.put_nowait(IndexUpdateTask(collection, index))
        matchengine.task_q.task_done()
    except Exception as e:
        log.error(f"ERROR: Worker: {worker_id}, error: {e}")
        log.error(f"TRACEBACK: {traceback.print_tb(e.__traceback__)}")
        if e.__class__ is AutoReconnect:
            await matchengine.task_q.put(task)
            matchengine.task_q.task_done()
        elif e.__class__ is CursorNotFound:
            matchengine.task_q.put_nowait(task)
            matchengine.task_q.task_done()
        elif e.__class__ is ServerSelectionTimeoutError:
            matchengine.task_q.put_nowait(task)
            matchengine.task_q.task_done()
        else:
            matchengine.__exit__(None, None, None)
            matchengine.loop.stop()
            log.error((f"ERROR: Worker: {worker_id}, error: {e}"
                       f"TRACEBACK: {traceback.print_tb(e.__traceback__)}"))
            raise e


async def run_index_update_task(matchengine: MatchEngine, task: IndexUpdateTask, worker_id):
    if matchengine.debug:
        log.info(
            f"Worker: {worker_id}, index {task.index}, collection {task.collection} got new IndexUpdateTask")
    try:
        matchengine.db_rw[task.collection].create_index(task.index)
        matchengine.task_q.task_done()
    except Exception as e:
        log.error(f"ERROR: Worker: {worker_id}, error: {e}")
        log.error(f"TRACEBACK: {traceback.print_tb(e.__traceback__)}")
        if e.__class__ is AutoReconnect:
            matchengine.task_q.put_nowait(task)
            matchengine.task_q.task_done()
        elif e.__class__ is CursorNotFound:
            matchengine.task_q.put_nowait(task)
            matchengine.task_q.task_done()
        elif e.__class__ is ServerSelectionTimeoutError:
            matchengine.task_q.put_nowait(task)
            matchengine.task_q.task_done()
        else:
            matchengine.loop.stop()
            log.error((f"ERROR: Worker: {worker_id}, error: {e}"
                       f"TRACEBACK: {traceback.print_tb(e.__traceback__)}"))


async def run_query_task(matchengine: MatchEngine, task, worker_id):
    trial_identifier = matchengine.match_criteria_transform.trial_identifier
    if matchengine.debug:
        log.info((f"Worker: {worker_id}, {trial_identifier}: {task.trial[trial_identifier]} got new QueryTask, "
                  f"{matchengine._task_q.qsize()} tasks left in queue"))
    try:
        results: Dict[ClinicalID, List[MatchReason]] = await matchengine.run_query(task.query,
                                                                                   task.clinical_ids)
    except Exception as e:
        results = dict()
        log.error(f"ERROR: Worker: {worker_id}, error: {e}")
        log.error(f"TRACEBACK: {traceback.print_tb(e.__traceback__)}")
        if e.__class__ is AutoReconnect:
            matchengine.task_q.put_nowait(task)
            matchengine.task_q.task_done()
        elif e.__class__ is CursorNotFound:
            matchengine.task_q.put_nowait(task)
            matchengine.task_q.task_done()
        elif e.__class__ is ServerSelectionTimeoutError:
            matchengine.task_q.put_nowait(task)
            matchengine.task_q.task_done()
        else:
            matchengine.loop.stop()
            log.error(f"ERROR: Worker: {worker_id}, error: {e}")
            log.error(f"TRACEBACK: {traceback.print_tb(e.__traceback__)}")

    try:
        by_sample_id = defaultdict(list)
        matchengine.results_transformer(results)
        if not results:
            matchengine.matches.setdefault(task.match_clause_data.protocol_no, dict())
        for _, sample_results in results.items():
            for result in sample_results:
                matchengine.queue_task_count += 1
                if matchengine.queue_task_count % 1000 == 0 and matchengine.debug:
                    log.info(f"Trial match count: {matchengine.queue_task_count}")
                match_context_data = TrialMatch(task.trial,
                                                task.match_clause_data,
                                                task.match_path,
                                                task.query,
                                                result,
                                                matchengine.starttime)

                # allow user to extend trial_match objects in plugin functions
                # generate required fields on trial match doc before
                # generate sort_order and hash fields after all fields are added
                new_match_proto = matchengine.pre_process_trial_matches(match_context_data)
                match_document = matchengine.create_trial_matches(match_context_data, new_match_proto)
                sort_order = get_sort_order(matchengine, match_document)
                match_document['sort_order'] = sort_order
                to_hash = {key: match_document[key] for key in match_document if key not in {'hash', 'is_disabled'}}
                match_document['hash'] = nested_object_hash(to_hash)
                match_document['_me_id'] = matchengine.run_id.hex

                matchengine.matches.setdefault(task.trial[trial_identifier],
                                               dict()).setdefault(match_document['sample_id'],
                                                                  list()).append(match_document)
                by_sample_id[match_document['sample_id']].append(match_document)

    except Exception as e:
        matchengine.loop.stop()
        log.error(f"ERROR: Worker: {worker_id}, error: {e}")
        log.error(f"TRACEBACK: {traceback.print_tb(e.__traceback__)}")
        raise e

    matchengine.task_q.task_done()


async def run_poison_pill(matchengine: MatchEngine, task, worker_id):
    if matchengine.debug:
        log.info(f"Worker: {worker_id} got PoisonPill")
    matchengine.task_q.task_done()


async def run_update_task(matchengine: MatchEngine, task: UpdateTask, worker_id):
    try:
        if matchengine.debug:
            log.info(f"Worker {worker_id} got new UpdateTask {task.protocol_no}")
        tasks = [
            matchengine.async_db_rw[matchengine.trial_match_collection].bulk_write(chunked_ops,
                                                                                   ordered=False)
            for chunked_ops
            in chunk_list(task.ops, matchengine.chunk_size)
        ]
        await asyncio.gather(*tasks)
        matchengine.task_q.task_done()
    except Exception as e:
        log.error(f"ERROR: Worker: {worker_id}, error: {e}")
        log.error(f"TRACEBACK: {traceback.print_tb(e.__traceback__)}")
        if e.__class__ is AutoReconnect:
            matchengine.task_q.task_done()
            matchengine.task_q.put_nowait(task)
        elif e.__class__ is CursorNotFound:
            matchengine.task_q.put_nowait(task)
            matchengine.task_q.task_done()
        elif e.__class__ is ServerSelectionTimeoutError:
            matchengine.task_q.put_nowait(task)
            matchengine.task_q.task_done()
        else:
            raise e


async def run_run_log_update_task(matchengine: MatchEngine, task: RunLogUpdateTask, worker_id):
    clinical_run_history_collection = f"clinical_run_history_{matchengine.trial_match_collection}"
    run_log_collection = f"run_log_{matchengine.trial_match_collection}"
    if task.protocol_no not in matchengine.trials_to_match_on:
        matchengine.task_q.task_done()
        return
    try:
        if matchengine.debug:
            log.info(f"Worker {worker_id} got new RunLogUpdateTask {task.protocol_no}")
            logging.error(matchengine.run_log_entries[task.protocol_no])

        dont_need_insert, _ = await asyncio.gather(
            matchengine.async_db_ro.get_collection(clinical_run_history_collection).distinct("clinical_id"),
            matchengine.async_db_rw[run_log_collection].insert_one(matchengine.run_log_entries[task.protocol_no])
        )
        new_clinical_run_log_docs = set(matchengine.clinical_run_log_entries[task.protocol_no]) - set(dont_need_insert)
        clinical_update_ops = [
            InsertOne({"clinical_id": clinical_id, "run_history": list()})
            for clinical_id
            in new_clinical_run_log_docs
        ]
        if clinical_update_ops:
            await matchengine.async_db_rw.get_collection(
                clinical_run_history_collection
            ).bulk_write(clinical_update_ops, ordered=False)
        await matchengine.async_db_rw.get_collection(clinical_run_history_collection).update_many(
            {'clinical_id': {"$in": list(matchengine.clinical_run_log_entries[task.protocol_no])}},
            {'$addToSet': {"run_history": matchengine.run_id.hex}}
        )
        matchengine.task_q.task_done()
    except Exception as e:
        log.error(f"ERROR: Worker: {worker_id}, error: {e}")
        log.error(f"TRACEBACK: {traceback.print_tb(e.__traceback__)}")
        if e.__class__ is AutoReconnect:
            matchengine.task_q.task_done()
            matchengine.task_q.put_nowait(task)
        elif e.__class__ is CursorNotFound:
            matchengine.task_q.put_nowait(task)
            matchengine.task_q.task_done()
        elif e.__class__ is ServerSelectionTimeoutError:
            matchengine.task_q.put_nowait(task)
            matchengine.task_q.task_done()
        else:
            raise e
