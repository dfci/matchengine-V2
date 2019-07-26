import asyncio
import logging

from pymongo import UpdateMany, InsertOne

from utilities.matchengine_types import RunLogUpdateTask, UpdateTask, MongoQuery

logging.basicConfig(level=logging.INFO)
log = logging.getLogger('matchengine')


async def _async_update_matches_by_protocol_no(self, protocol_no: str):
    """
    Update trial matches by diff'ing the newly created trial matches against existing matches in the db.
    'Delete' matches by adding {is_disabled: true} and insert all new matches.
    """
    trial_matches_by_sample_id = self.matches.get(protocol_no, dict())
    log.info(f"Updating trial matches for {protocol_no}")
    remaining_to_disable = [
        result
        for result in await self._perform_db_call(collection='trial_match',
                                                  query=MongoQuery(
                                                      {
                                                          'protocol_no': protocol_no,
                                                          "sample_id": {
                                                              '$nin': list(trial_matches_by_sample_id.keys())
                                                          }
                                                      }),
                                                  projection={'_id': 1, 'hash': 1, 'clinical_id': 1})
    ]
    initial_delete_ops = [
        UpdateMany(filter={'hash': {'$in': [result['hash']
                                            for result in remaining_to_disable]}},
                   update={'$set': {"is_disabled": True}})
    ]
    await self._task_q.put(UpdateTask(initial_delete_ops, protocol_no))

    for sample_id in trial_matches_by_sample_id.keys():
        new_matches_hashes = [match['hash'] for match in trial_matches_by_sample_id[sample_id]]

        trial_matches_to_not_change_query = MongoQuery({'hash': {'$in': new_matches_hashes}})
        trial_matches_to_disable_query = MongoQuery({'protocol_no': protocol_no,
                                                     'sample_id': sample_id,
                                                     'is_disabled': False,
                                                     'hash': {'$nin': new_matches_hashes}})
        projection = {"hash": 1, "is_disabled": 1}
        trial_matches_existent_results, trial_matches_to_disable = await asyncio.gather(
            self._perform_db_call('trial_match', trial_matches_to_not_change_query, projection),
            self._perform_db_call('trial_match', trial_matches_to_disable_query, projection)
        )

        trial_matches_hashes_existent = {
            result['hash']
            for result
            in trial_matches_existent_results
        }
        trial_matches_disabled = {
            result['hash']
            for result in trial_matches_existent_results
            if result['is_disabled']
        }

        trial_matches_to_insert = [
            trial_match
            for trial_match in trial_matches_by_sample_id[sample_id]
            if trial_match['hash'] not in trial_matches_hashes_existent
        ]
        trial_matches_to_mark_available = [
            trial_match
            for trial_match in trial_matches_by_sample_id[sample_id]
            if trial_match['hash'] in trial_matches_disabled
        ]

        ops = list()
        ops.append(UpdateMany(filter={'hash': {'$in': [trial_match['hash']
                                                       for trial_match in trial_matches_to_disable]}},
                              update={'$set': {'is_disabled': True}}))
        for to_insert in trial_matches_to_insert:
            ops.append(InsertOne(document=to_insert))
        ops.append(UpdateMany(filter={'hash': {'$in': [trial_match['hash']
                                                       for trial_match in trial_matches_to_mark_available]}},
                              update={'$set': {'is_disabled': False}}))
        await self._task_q.put(UpdateTask(ops, protocol_no))

    await self._task_q.put(RunLogUpdateTask(protocol_no))
    await self._task_q.join()
