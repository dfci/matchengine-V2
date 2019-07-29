import logging
import traceback

from pymongo.errors import AutoReconnect, CursorNotFound

from utilities.matchengine_types import TrialMatch

logging.basicConfig(level=logging.INFO)
log = logging.getLogger('matchengine')


async def run_query_task(me, task, worker_id):
    if me.debug:
        log.info(
            f"Worker: {worker_id}, protocol_no: {task.trial['protocol_no']} got new QueryTask")
    try:
        results = await me.run_query(task.query, task.clinical_ids)
    except Exception as e:
        log.error(f"ERROR: Worker: {worker_id}, error: {e}")
        log.error(f"TRACEBACK: {traceback.print_tb(e.__traceback__)}")
        results = list()
        if isinstance(e, AutoReconnect):
            await me._task_q.put(task)
            me._task_q.task_done()
        elif isinstance(e, CursorNotFound):
            await me._task_q.put(task)
            me._task_q.task_done()
        else:
            me._loop.stop()
            log.error(f"ERROR: Worker: {worker_id}, error: {e}")
            log.error(f"TRACEBACK: {traceback.print_tb(e.__traceback__)}")

    try:
        for result in results:
            me._queue_task_count += 1
            if me._queue_task_count % 1000 == 0:
                log.info(f"Trial match count: {me._queue_task_count}")
            match_document = me.create_trial_matches(TrialMatch(task.trial,
                                                                task.match_clause_data,
                                                                task.match_path,
                                                                task.query,
                                                                result,
                                                                me.starttime))
            me.matches[task.trial['protocol_no']][match_document['sample_id']].append(match_document)
    except Exception as e:
        me._loop.stop()
        log.error(f"ERROR: Worker: {worker_id}, error: {e}")
        log.error(f"TRACEBACK: {traceback.print_tb(e.__traceback__)}")
        raise e

    me._task_q.task_done()


async def run_poison_pill(me, worker_id):
    if me.debug:
        log.info(f"Worker: {worker_id} got PoisonPill")
    me._task_q.task_done()


async def run_update_task(me, task, worker_id):
    try:
        if me.debug:
            log.info(f"Worker {worker_id} got new UpdateTask {task.protocol_no}")
        await me.async_db_rw.trial_match.bulk_write(task.ops, ordered=False)
    except Exception as e:
        log.error(f"ERROR: Worker: {worker_id}, error: {e}")
        log.error(f"TRACEBACK: {traceback.print_tb(e.__traceback__)}")
        if isinstance(e, AutoReconnect):
            me._task_q.task_done()
            await me._task_q.put(task)
        else:
            raise e
    finally:
        me._task_q.task_done()


async def run_run_log_update_task(me, task, worker_id):
    try:
        if me.debug:
            log.info(f"Worker {worker_id} got new RunLogUpdateTask {task.protocol_no}")
        await me.async_db_rw.run_log.insert_one(me.run_log_entries[task.protocol_no])
        await me.async_db_rw.clinical.update_many(
            {'_id': {"$in": list(me.clinical_run_log_entries[task.protocol_no])}},
            {'$push': {"run_history": me.run_id.hex}}
        )
    except Exception as e:
        log.error(f"ERROR: Worker: {worker_id}, error: {e}")
        log.error(f"TRACEBACK: {traceback.print_tb(e.__traceback__)}")
        if isinstance(e, AutoReconnect):
            me._task_q.task_done()
            await me._task_q.put(task)
        else:
            raise e
    finally:
        me._task_q.task_done()
