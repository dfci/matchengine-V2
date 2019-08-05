from __future__ import annotations

import asyncio
import copy
import logging
from typing import TYPE_CHECKING

from matchengine.typing.matchengine_types import (
    ClinicalMatchReason,
    GenomicMatchReason,
    MongoQuery
)
from matchengine.utilities.utilities import perform_db_call

if TYPE_CHECKING:
    from bson import ObjectId
    from matchengine.engine import MatchEngine
    from matchengine.typing.matchengine_types import (
        ClinicalID,
        MultiCollectionQuery
    )
    from typing import (
        Tuple,
        Set,
        List,
    )

logging.basicConfig(level=logging.INFO)
log = logging.getLogger('matchengine')


async def execute_clinical_queries(me,
                                   multi_collection_query: MultiCollectionQuery,
                                   clinical_ids: Set[ClinicalID]) -> Tuple[Set[ObjectId],
                                                                           List[ClinicalMatchReason]]:
    """
    Take in a list of queries and only execute the clinical ones. Take the resulting clinical ids, and pass that
    to the next clinical query. Repeat for all clinical queries, continuously subsetting the returned ids.
    Finally, return all clinical IDs which matched every query, and match reasons.

    Match Reasons are not used by default, but are composed of QueryNode objects and a clinical ID.
    """
    collection = me.match_criteria_transform.CLINICAL
    reasons = list()
    for _clinical in multi_collection_query.clinical:
        for query_node in _clinical.query_nodes:
            for query_part in query_node.query_parts:
                if not query_part.render:
                    continue

                # hash the inner query to use as a reference for returned clinical ids, if necessary
                query_hash = query_part.hash()
                if query_hash not in me.cache.ids:
                    me.cache.ids[query_hash] = dict()

                # create a nested id_cache where the key is the clinical ID being queried and the vals
                # are the clinical IDs returned
                id_cache = me.cache.ids[query_hash]
                queried_ids = id_cache.keys()
                need_new = clinical_ids - set(queried_ids)

                if need_new:
                    new_query = {'$and': [{'_id': {'$in': list(need_new)}}, query_part.query]}
                    if me.debug:
                        log.info(f"{query_part.query}")
                    docs = await me.async_db_ro[collection].find(new_query, {'_id': 1}).to_list(None)

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

                    # no clinical doc returned for an inclusion criteria query, so remove _id from future queries
                    elif id_cache[clinical_id] is None and not query_part.negate:
                        clinical_ids.remove(clinical_id)

    reasons_cache = set()
    for clinical_id in clinical_ids:
        for _query_node_container in multi_collection_query.clinical:
            for query_node in _query_node_container.query_nodes:
                if (query_node.hash(), clinical_id) not in reasons_cache:
                    reasons_cache.add((query_node.hash(), clinical_id))
                    reasons.append(ClinicalMatchReason(query_node, clinical_id))
    return clinical_ids, reasons


async def execute_genomic_queries(me,
                                  multi_collection_query: MultiCollectionQuery,
                                  clinical_ids: Set[ClinicalID]) -> Tuple[Set[ObjectId], Set[ObjectId],
                                                                          List[GenomicMatchReason]]:
    """
    Take in a list of queries and clinical ids.
    Return an object e.g.
    { Clinical_ID : { GenomicID1, GenomicID2 etc } }
    """
    clinical_results_results = [set()]
    genomic_results_results = [set()]
    all_potential_reasons = list()
    all_potential_exclusion_reasons = set()
    for genomic_query_node_container in multi_collection_query.genomic:
        container_node_clinical_results = [set() for _ in range(0, len(genomic_query_node_container.query_nodes))]
        container_node_genomic_results = [set() for _ in range(0, len(genomic_query_node_container.query_nodes))]
        container_node_clinical_ids = [{clinical_id for clinical_id in clinical_ids}
                                       for _ in range(0, len(genomic_query_node_container.query_nodes))]
        for _idx, genomic_query_node in enumerate(genomic_query_node_container.query_nodes):
            clinical_results_results_slice_by = len(clinical_results_results) - 1
            genomic_results_results_slice_by = len(genomic_results_results) - 1
            for to_extend in [copy.deepcopy(clinical_results_results) for _ in range(1, _idx)]:
                clinical_results_results.extend(to_extend)
            for to_extend in [copy.deepcopy(genomic_results_results) for _ in range(1, _idx)]:
                genomic_results_results.extend(to_extend)
            local_clinical_results_results = clinical_results_results[clinical_results_results_slice_by::]
            local_genomic_results_results = genomic_results_results[genomic_results_results_slice_by::]
            node_clinical_results = container_node_clinical_results[_idx]
            node_genomic_results = container_node_genomic_results[_idx]
            node_clinical_ids = container_node_clinical_ids[_idx]
            node_potential_reasons = set()
            node_potential_exclusion_reasons = set()
            if genomic_query_node.mcq_invalidating:
                continue
            join_field = me.match_criteria_transform.collection_mappings['genomic']['join_field']
            query = genomic_query_node.extract_raw_query()

            # Create a nested id_cache where the key is the clinical ID being queried and the vals
            # are the genomic IDs returned
            query_hash = genomic_query_node.raw_query_hash()
            if query_hash not in me.cache.ids:
                me.cache.ids[query_hash] = dict()
            id_cache = me.cache.ids[query_hash]
            queried_ids = id_cache.keys()
            need_new = node_clinical_ids - set(queried_ids)

            if need_new:
                new_query = query
                new_query['$and'] = new_query.get('$and', list())
                new_query['$and'].insert(0, {join_field: {'$in': list(need_new)}})

                projection = {"_id": 1, join_field: 1}
                genomic_docs = await me.async_db_ro['genomic'].find(new_query, projection).to_list(None)
                if me.debug:
                    log.info(f"{new_query} returned {genomic_docs}")

                for genomic_doc in genomic_docs:
                    # If the clinical id of a returned genomic doc is not present in the cache, add it.
                    if genomic_doc[join_field] not in id_cache:
                        id_cache[genomic_doc[join_field]] = set()
                    id_cache[genomic_doc[join_field]].add(genomic_doc["_id"])

                # Clinical IDs which do not return genomic docs need to be recorded to cache exclusions
                for unfound in need_new - set(id_cache.keys()):
                    id_cache[unfound] = None

            for clinical_id in node_clinical_ids:
                if id_cache[clinical_id] is not None:
                    genomic_ids = id_cache[clinical_id]
                    node_clinical_results.add(clinical_id)

                    # Most of the time, queries associate one genomic doc to one query, but not always e.g. a patient
                    # has 2 KRAS mutations and the query is for any KRAS mutation
                    for genomic_id in genomic_ids:
                        # If an inclusion match...
                        if not genomic_query_node.exclusion:
                            node_genomic_results.add(genomic_id)
                            node_potential_reasons.add((len(genomic_ids),
                                                        clinical_id,
                                                        genomic_id))

                        # If an exclusion criteria returns a genomic doc, that means that clinical ID is not a match.
                        elif genomic_query_node.exclusion and clinical_id in node_clinical_results:
                            node_clinical_results.remove(clinical_id)

                # If the genomic query returns nothing for an exclusion query, for a specific clinical ID, it is a match
                elif id_cache[clinical_id] is None and genomic_query_node.exclusion:
                    node_clinical_results.add(clinical_id)
                    node_potential_exclusion_reasons.add((1, clinical_id))

            if not node_clinical_ids:
                continue
            else:
                # Remove everything from the output object which is not in the returned clinical IDs.
                # TODO: add tests for cases when all genomic nodes are exclusion
                all_potential_reasons.extend([
                    GenomicMatchReason(genomic_query_node, siblings, clinical_id, genomic_id)
                    for siblings, clinical_id, genomic_id
                    in node_potential_reasons
                    if clinical_id in node_clinical_results and genomic_id in node_genomic_results
                ])
                # TODO: figure out how to convey that one exclusion criteria matched many times even though it shows up once per match path
                all_potential_reasons.extend([
                    GenomicMatchReason(genomic_query_node, siblings, clinical_id, None)
                    for siblings, clinical_id
                    in node_potential_exclusion_reasons
                    if (siblings, clinical_id) not in all_potential_exclusion_reasons
                ])
                all_potential_exclusion_reasons |= node_potential_exclusion_reasons
                for clinical_id_to_add in node_clinical_results & node_clinical_ids:
                    for clinical_results in local_clinical_results_results:
                        clinical_results.add(clinical_id_to_add)
                for genomic_results in local_genomic_results_results:
                    genomic_results |= node_genomic_results

    return ({clinical_id for clinical_ids in clinical_results_results for clinical_id in clinical_ids},
            {genomic_id for genomic_ids in genomic_results_results for genomic_id in genomic_ids},
            all_potential_reasons)


async def get_docs_results(matchengine: MatchEngine, needed_clinical, needed_genomic):
    """
    Matching criteria for clinical and genomic values can be set/extended in config.json
    :param matchengine:
    :param needed_clinical:
    :param needed_genomic:
    :return:
    """
    genomic_projection = matchengine.match_criteria_transform.genomic_projection
    clinical_projection = matchengine.match_criteria_transform.clinical_projection
    clinical_query = MongoQuery({"_id": {"$in": list(needed_clinical)}})
    genomic_query = MongoQuery({"_id": {"$in": list(needed_genomic)}})
    results = await asyncio.gather(perform_db_call(matchengine, "clinical", clinical_query, clinical_projection),
                                   perform_db_call(matchengine, "genomic", genomic_query, genomic_projection))
    return results


def get_valid_genomic_reasons(genomic_match_reasons, clinical_ids, genomic_ids):
    return [
        genomic_reason
        for genomic_reason in genomic_match_reasons
        if genomic_reason.clinical_id in clinical_ids and any([genomic_reason.query_node.exclusion,
                                                               genomic_reason.genomic_id in genomic_ids])
    ]


def get_valid_clinical_reasons(clinical_match_reasons, clinical_ids):
    return [
        clinical_reason
        for clinical_reason in clinical_match_reasons
        if clinical_reason.clinical_id in clinical_ids
    ]
