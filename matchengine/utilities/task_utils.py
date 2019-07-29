from __future__ import annotations
from typing import TYPE_CHECKING
import logging
import traceback

from pymongo.errors import (
    AutoReconnect,
    CursorNotFound
)

from matchengine.utilities.matchengine_types import TrialMatch

if TYPE_CHECKING:
    from matchengine.engine import MatchEngine

logging.basicConfig(level=logging.INFO)
log = logging.getLogger('matchengine')


async def run_query_task(matchengine: MatchEngine, task, worker_id):
    if matchengine.debug:
        log.info(
            f"Worker: {worker_id}, protocol_no: {task.trial['protocol_no']} got new QueryTask")
    try:
        results = await matchengine.run_query(task.query, task.clinical_ids)
    except Exception as e:
        log.error(f"ERROR: Worker: {worker_id}, error: {e}")
        log.error(f"TRACEBACK: {traceback.print_tb(e.__traceback__)}")
        results = list()
        if isinstance(e, AutoReconnect):
            await matchengine.task_q.put(task)
            await matchengine.task_q.task_done()
        elif isinstance(e, CursorNotFound):
            await matchengine.task_q.put(task)
            await matchengine.task_q.task_done()
        else:
            matchengine.loop.stop()
            log.error(f"ERROR: Worker: {worker_id}, error: {e}")
            log.error(f"TRACEBACK: {traceback.print_tb(e.__traceback__)}")

    try:
        for result in results:
            matchengine.queue_task_count += 1
            if matchengine.queue_task_count % 1000 == 0:
                log.info(f"Trial match count: {matchengine.queue_task_count}")
            match_document = matchengine.create_trial_matches(TrialMatch(task.trial,
                                                                         task.match_clause_data,
                                                                         task.match_path,
                                                                         task.query,
                                                                         result,
                                                                         matchengine.starttime))
            matchengine.matches[task.trial['protocol_no']][match_document['sample_id']].append(match_document)
    except Exception as e:
        matchengine.loop.stop()
        log.error(f"ERROR: Worker: {worker_id}, error: {e}")
        log.error(f"TRACEBACK: {traceback.print_tb(e.__traceback__)}")
        raise e

    matchengine.task_q.task_done()


async def run_poison_pill(matchengine: MatchEngine, worker_id):
    if matchengine.debug:
        log.info(f"Worker: {worker_id} got PoisonPill")
    matchengine.task_q.task_done()


async def run_update_task(matchengine: MatchEngine, task, worker_id):
    try:
        if matchengine.debug:
            log.info(f"Worker {worker_id} got new UpdateTask {task.protocol_no}")
        await matchengine.async_db_rw.trial_match.bulk_write(task.ops, ordered=False)
    except Exception as e:
        log.error(f"ERROR: Worker: {worker_id}, error: {e}")
        log.error(f"TRACEBACK: {traceback.print_tb(e.__traceback__)}")
        if isinstance(e, AutoReconnect):
            matchengine.task_q.task_done()
            await matchengine.task_q.put(task)
        else:
            raise e
    finally:
        matchengine.task_q.task_done()


async def run_run_log_update_task(matchengine: MatchEngine, task, worker_id):
    try:
        if matchengine.debug:
            log.info(f"Worker {worker_id} got new RunLogUpdateTask {task.protocol_no}")
        await matchengine.async_db_rw.run_log.insert_one(matchengine.run_log_entries[task.protocol_no])
        await matchengine.async_db_rw.clinical.update_many(
            {'_id': {"$in": list(matchengine.clinical_run_log_entries[task.protocol_no])}},
            {'$push': {"run_history": matchengine.run_id.hex}}
        )
    except Exception as e:
        log.error(f"ERROR: Worker: {worker_id}, error: {e}")
        log.error(f"TRACEBACK: {traceback.print_tb(e.__traceback__)}")
        if isinstance(e, AutoReconnect):
            matchengine.task_q.task_done()
            await matchengine.task_q.put(task)
        else:
            raise e
    finally:
        matchengine.task_q.task_done()
