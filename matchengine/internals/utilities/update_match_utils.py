from __future__ import annotations

import asyncio
import datetime
import logging
from typing import TYPE_CHECKING

from pymongo import UpdateMany, InsertOne

from matchengine.internals.typing.matchengine_types import RunLogUpdateTask, UpdateTask, MongoQuery
from matchengine.internals.utilities.list_utils import chunk_list
from matchengine.internals.utilities.utilities import perform_db_call

logging.basicConfig(level=logging.INFO)
log = logging.getLogger('matchengine')
if TYPE_CHECKING:
    from matchengine.internals.engine import MatchEngine


async def async_update_matches_by_protocol_no(matchengine: MatchEngine, protocol_no: str):
    """
    Update trial matches by diff'ing the newly created trial matches against existing matches in
    the db. Delete matches by adding {is_disabled: true} and insert all new matches.
    """
    matches_by_sample_id = matchengine.matches.get(protocol_no, dict())
    updated_time = datetime.datetime.now()
    for matches in matches_by_sample_id.values():
        for match in matches:
            match['_updated'] = updated_time
    if protocol_no not in matchengine.matches:
        log.info(f"{matchengine.match_criteria_transform.trial_collection} {protocol_no} was not matched on, not updating {matchengine.match_criteria_transform.trial_collection} matches")
        if not matchengine.skip_run_log_entry:
            matchengine.task_q.put_nowait(RunLogUpdateTask(protocol_no))
        await matchengine.task_q.join()
        return
    log.info(f"Updating matches for {protocol_no}")
    if not matchengine.drop:
        if not matchengine.matches[protocol_no]:
            for chunk in chunk_list(list(matchengine.clinical_ids_for_protocol_cache[protocol_no]),
                                    matchengine.chunk_size):
                matchengine.task_q.put_nowait(
                    UpdateTask(
                        [UpdateMany(filter={matchengine.match_criteria_transform.match_trial_link_id: protocol_no,
                                            'clinical_id': {'$in': chunk}},
                                    update={'$set': {"is_disabled": True,
                                                     '_updated': updated_time}})],
                        protocol_no
                    )
                )
        else:
            matches_to_disable = await get_all_except(matchengine, protocol_no, matches_by_sample_id)
            delete_ops = await get_delete_ops(matches_to_disable, matchengine)
            matchengine.task_q.put_nowait(UpdateTask(delete_ops, protocol_no))

    for sample_id in matches_by_sample_id.keys():
        if not matchengine.drop:
            new_matches_hashes = [match['hash'] for match in matches_by_sample_id[sample_id]]

            # get existing state of trial match collection
            existing = await get_existing_matches(matchengine, new_matches_hashes)
            existing_hashes = {result['hash'] for result in existing}
            disabled = {result['hash'] for result in existing if result['is_disabled']}

            # insert matches in new_matches_hashes if they don't already exist
            # disable matches NOT in new_matches_hashes
            matches_to_insert = get_matches_to_insert(matches_by_sample_id,
                                                      existing_hashes,
                                                      sample_id)
            matches_to_disable = await get_matches_to_disable(matchengine,
                                                              new_matches_hashes,
                                                              protocol_no,
                                                              sample_id)

            # flip is_disabled flag if a new match generated during run matches hash of an existing
            matches_to_mark_available = [m for m in matches_by_sample_id[sample_id] if
                                         m['hash'] in disabled]
            ops = get_update_operations(matches_to_disable,
                                        matches_to_insert,
                                        matches_to_mark_available,
                                        matchengine)
        else:
            ops = [InsertOne(document=trial_match) for trial_match in
                   matches_by_sample_id[sample_id]]
        matchengine.task_q.put_nowait(UpdateTask(ops, protocol_no))

    if not matchengine.skip_run_log_entry:
        matchengine.task_q.put_nowait(RunLogUpdateTask(protocol_no))
    await matchengine.task_q.join()


async def get_all_except(matchengine: MatchEngine,
                         protocol_no: str,
                         trial_matches_by_sample_id: dict) -> list:
    """Return all matches except ones matching current protocol_no"""
    clinical_ids = {matchengine.sample_mapping[sample_id] for sample_id in trial_matches_by_sample_id.keys()}

    if protocol_no in matchengine.clinical_run_log_entries:
        clinical_ids = matchengine.clinical_run_log_entries[protocol_no] - clinical_ids

    query = {
        matchengine.match_criteria_transform.match_trial_link_id: protocol_no,
        "clinical_id": {
            '$in': [clinical_id for clinical_id in clinical_ids]
        }
    }
    projection = {
        '_id': 1,
        'hash': 1,
        'clinical_id': 1
    }

    results = await perform_db_call(matchengine,
                                    collection=matchengine.trial_match_collection,
                                    query=MongoQuery(query),
                                    projection=projection)

    return [result for result in results]


async def get_delete_ops(matches_to_disable: list, matchengine: MatchEngine) -> list:
    updated_time = datetime.datetime.now()
    hashes = [result['hash'] for result in matches_to_disable]
    ops = list()
    for chunk in chunk_list(hashes, matchengine.chunk_size):
        ops.append(UpdateMany(filter={'hash': {'$in': chunk}},
                              update={'$set': {"is_disabled": True, '_updated': updated_time}}))
    return ops


async def get_existing_matches(matchengine: MatchEngine, new_matches_hashes: list) -> list:
    matches_to_not_change_query = MongoQuery({'hash': {'$in': new_matches_hashes}})
    projection = {"hash": 1, "is_disabled": 1}
    matches = await asyncio.gather(
        perform_db_call(matchengine,
                        matchengine.trial_match_collection,
                        matches_to_not_change_query,
                        projection)
    )
    return matches[0]


async def get_matches_to_disable(matchengine: MatchEngine,
                                 new_matches_hashes: list,
                                 protocol_no: str,
                                 sample_id: str) -> list:

    matches_to_disable_query = MongoQuery({matchengine.match_criteria_transform.match_trial_link_id: protocol_no,
                                           'sample_id': sample_id,
                                           'is_disabled': False,
                                           'hash': {'$nin': new_matches_hashes}})
    projection = {"hash": 1, "is_disabled": 1}
    matches = await asyncio.gather(
        perform_db_call(matchengine,
                        matchengine.trial_match_collection,
                        matches_to_disable_query,
                        projection)
    )
    return matches[0]


def get_update_operations(matches_to_disable: list,
                          matches_to_insert: list,
                          matches_to_mark_available: list,
                          matchengine: MatchEngine) -> list:
    ops = list()
    updated_time = datetime.datetime.now()
    disable_hashes = [trial_match['hash'] for trial_match in matches_to_disable]
    for chunk in chunk_list(disable_hashes, matchengine.chunk_size):
        ops.append(UpdateMany(filter={'hash': {'$in': chunk}},
                              update={'$set': {'is_disabled': True,
                                               '_updated': updated_time}}))
    for to_insert in matches_to_insert:
        ops.append(InsertOne(document=to_insert))

    available_hashes = [trial_match['hash'] for trial_match in matches_to_mark_available]
    for chunk in chunk_list(available_hashes, matchengine.chunk_size):
        ops.append(UpdateMany(filter={'hash': {'$in': chunk}},
                              update={'$set': {'is_disabled': False,
                                               '_updated': updated_time}}))
    return ops


def get_matches_to_insert(matches_by_sample_id: list, existing_hashes: set,
                          sample_id: str) -> list:
    return [m for m in matches_by_sample_id[sample_id] if m['hash'] not in existing_hashes]
