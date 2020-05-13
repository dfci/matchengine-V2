from __future__ import annotations

import asyncio
import logging
import operator
from collections import defaultdict
from functools import reduce
from typing import TYPE_CHECKING, Dict

from matchengine.internals.typing.matchengine_types import (
    ClinicalMatchReason,
    ExtendedMatchReason,
    MongoQuery,
    Cache, MatchReason
)
from matchengine.internals.utilities.utilities import perform_db_call

if TYPE_CHECKING:
    from bson import ObjectId
    from matchengine.internals.engine import MatchEngine
    from matchengine.internals.typing.matchengine_types import (
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


async def execute_clinical_queries(matchengine: MatchEngine,
                                   multi_collection_query: MultiCollectionQuery,
                                   clinical_ids: Set[ClinicalID]) -> Tuple[Set[ObjectId],
                                                                           Dict[ClinicalID, List[ClinicalMatchReason]]]:
    """
    Take in a list of queries and only execute the clinical ones. Take the resulting clinical ids, and pass that
    to the next clinical query. Repeat for all clinical queries, continuously subsetting the returned ids.
    Finally, return all clinical IDs which matched every query, and match reasons.

    Match Reasons are not used by default, but are composed of QueryNode objects and a clinical ID.
    """
    reasons = defaultdict(list)
    reasons_cache = set()
    query_parts_by_hash = dict()
    for _clinical in multi_collection_query.clinical:
        for query_node in _clinical.query_nodes:
            query_level_mappings = matchengine.match_criteria_transform.ctml_collection_mappings[query_node.query_level]
            collection = query_level_mappings["query_collection"]
            join_field = query_level_mappings["join_field"]
            id_field = query_level_mappings["id_field"]
            show_in_ui, clinical_ids = matchengine.clinical_query_node_clinical_ids_subsetter(query_node, clinical_ids)
            for query_part in query_node.query_parts:
                if not query_part.render:
                    continue

                query_parts_by_hash[query_part.hash()] = query_part
                # hash the inner query to use as a reference for returned clinical ids, if necessary
                query_hash = query_part.hash()
                if query_hash not in matchengine.cache.ids:
                    matchengine.cache.ids[query_hash] = dict()

                # create a nested id_cache where the key is the clinical ID being queried and the vals
                # are the clinical IDs returned
                id_cache = matchengine.cache.ids[query_hash]
                queried_ids = set(id_cache.keys())
                still_waiting_for = matchengine.cache.in_process.setdefault(query_hash, set())
                need_new = clinical_ids - queried_ids - still_waiting_for
                matchengine.cache.in_process.setdefault(query_hash, set()).update(need_new)

                if need_new:
                    new_query = {'$and': [{join_field: {'$in': list(need_new)}}, query_part.query]}
                    if matchengine.debug:
                        log.info(f"{query_part.query}")
                    projection = {id_field: 1, join_field: 1}
                    docs = await matchengine.async_db_ro[collection].find(new_query, projection).to_list(None)

                    # save returned ids
                    for doc in docs:
                        id_cache[doc[id_field]] = doc[join_field]

                    # save IDs NOT returned as None so if a query is run in the future which is the same, it will skip
                    for unfound in need_new - set(id_cache.keys()):
                        id_cache[unfound] = None
                    matchengine.cache.in_process[query_hash].difference_update(need_new)

                while True:
                    still_waiting_for.intersection_update(matchengine.cache.in_process[query_hash])
                    if not still_waiting_for:
                        break
                    await asyncio.sleep(0.01)
                for clinical_id in list(clinical_ids):

                    # an exclusion criteria returned a clinical document hence doc is not a match
                    if id_cache[clinical_id] is not None and query_part.negate:
                        clinical_ids.remove(clinical_id)
                        if (query_node.hash(), clinical_id, query_node.query_depth) in reasons_cache:
                            reasons_cache.remove((show_in_ui, query_part.hash(), clinical_id, query_node.query_depth))

                    # clinical doc fulfills exclusion criteria
                    elif id_cache[clinical_id] is None and query_part.negate:
                        reasons_cache.add((show_in_ui, query_part.hash(), clinical_id, query_node.query_depth))

                    # doc meets inclusion criteria
                    elif id_cache[clinical_id] is not None and not query_part.negate:
                        reasons_cache.add((show_in_ui, query_part.hash(), clinical_id, query_node.query_depth))

                    # no clinical doc returned for an inclusion criteria query, so remove _id from future queries
                    elif id_cache[clinical_id] is None and not query_part.negate:
                        clinical_ids.remove(clinical_id)
                        if (query_node.hash(), clinical_id) in reasons_cache:
                            reasons_cache.remove((show_in_ui, query_part.hash(), clinical_id, query_node.query_depth))

    for show_in_ui, query_node_hash, clinical_id, depth in reasons_cache:
        reasons[clinical_id].append(
            ClinicalMatchReason(query_parts_by_hash[query_node_hash], clinical_id, depth, show_in_ui))
    return clinical_ids, reasons


async def execute_extended_queries(
        matchengine: MatchEngine,
        multi_collection_query: MultiCollectionQuery,
        initial_clinical_ids: Set[ClinicalID],
        reasons: Dict[ClinicalID, List[MatchReason]]) -> Tuple[Set[ObjectId],
                                                               Dict[str, Set[ObjectId]],
                                                               Dict[ClinicalID, List[MatchReason]]]:
    # This function will execute to filter patients on extended clinical/genomic attributes
    clinical_ids = {clinical_id: set() for clinical_id in initial_clinical_ids}
    qnc_qn_tracker = dict()
    for qnc_idx, query_node_container in enumerate(multi_collection_query.extended_attributes):
        query_node_container_clinical_ids = list()
        # TODO: add test for this - duplicate criteria causing empty qnc
        if not query_node_container.query_nodes:
            continue
        for qn_idx, query_node in enumerate(query_node_container.query_nodes):
            query_level_mappings = matchengine.match_criteria_transform.ctml_collection_mappings[query_node.query_level]
            collection = query_level_mappings["query_collection"]
            join_field = query_level_mappings["join_field"]
            id_field = query_level_mappings["id_field"]
            query_node_container_clinical_ids.append(
                matchengine.extended_query_node_clinical_ids_subsetter(query_node, clinical_ids.keys())
            )
            show_in_ui, working_clinical_ids = query_node_container_clinical_ids[qn_idx]
            if not working_clinical_ids:
                continue

            # Create a nested id_cache where the key is the clinical ID being queried and the vals
            # are the extended_attributes IDs returned
            query_hash = query_node.raw_query_hash()
            if query_hash not in matchengine.cache.ids:
                matchengine.cache.ids[query_hash] = dict()
            id_cache = matchengine.cache.ids[query_hash]
            queried_ids = set(id_cache.keys())
            still_waiting_for = matchengine.cache.in_process.setdefault(query_hash, set())
            need_new = working_clinical_ids - queried_ids - still_waiting_for
            matchengine.cache.in_process.setdefault(query_hash, set()).update(need_new)
            query = query_node.extract_raw_query()

            if need_new:
                new_query = query
                new_query['$and'] = new_query.get('$and', list())
                new_query['$and'].insert(0, {join_field: {'$in': list(need_new)}})

                projection = {id_field: 1, join_field: 1}
                genomic_docs = await matchengine.async_db_ro[collection].find(new_query, projection).to_list(None)
                if matchengine.debug:
                    log.info(f"{new_query} returned {genomic_docs}")

                for genomic_doc in genomic_docs:
                    # If the clinical id of a returned extended_attributes doc is not present in the cache, add it.
                    if genomic_doc[join_field] not in id_cache:
                        id_cache[genomic_doc[join_field]] = set()
                    id_cache[genomic_doc[join_field]].add(genomic_doc[id_field])

                # Clinical IDs which do not return extended_attributes docs need to be recorded to cache exclusions
                for unfound in need_new - set(id_cache.keys()):
                    id_cache[unfound] = None
                matchengine.cache.in_process[query_hash].difference_update(need_new)
            while True:
                still_waiting_for.intersection_update(matchengine.cache.in_process[query_hash])
                if not still_waiting_for:
                    break
                await asyncio.sleep(0.01)
            returned_clinical_ids = {clinical_id
                                     for clinical_id, genomic_docs
                                     in id_cache.items()
                                     if genomic_docs is not None}
            not_returned_clinical_ids = working_clinical_ids - returned_clinical_ids
            working_clinical_ids.intersection_update((
                not_returned_clinical_ids
                if query_node.exclusion
                else returned_clinical_ids
            ))
        current_clinical_ids = set(clinical_ids.keys())
        qnc_clinical_ids = {
            clinical_id
            for clinical_id
            in reduce(operator.or_, map(operator.itemgetter(1), query_node_container_clinical_ids), set())
        }
        for invalid_clinical in current_clinical_ids - qnc_clinical_ids:
            all_qnc_qn_to_remove = clinical_ids.pop(invalid_clinical)
            for qnc_qn_to_remove in all_qnc_qn_to_remove:
                qnc_qn_tracker[qnc_qn_to_remove][1].remove(invalid_clinical)
        for qn_idx, qn_results in enumerate(query_node_container_clinical_ids):
            for valid_clinical_id in qn_results[1] & qnc_clinical_ids:
                clinical_ids[valid_clinical_id].add((qnc_idx, qn_idx))
            qnc_qn_tracker[(qnc_idx, qn_idx)] = qn_results

    reasons, all_extended = get_reasons(qnc_qn_tracker, multi_collection_query, matchengine.cache, reasons)
    return set(clinical_ids.keys()), all_extended, reasons


def get_reasons(qnc_qn_tracker: Dict[Tuple: int, List[ClinicalID]],
                multi_collection_query: MultiCollectionQuery,
                cache: Cache,
                reasons: Dict[ClinicalID, List[MatchReason]]) -> Tuple[
    Dict[ClinicalID, List[MatchReason]], Dict[str, Set[ObjectId]]]:
    all_extended = defaultdict(set)

    for (qnc_idx, qn_idx), (show_in_ui, found_clinical_ids) in qnc_qn_tracker.items():
        genomic_query_node_container = multi_collection_query.extended_attributes[qnc_idx]
        query_node = genomic_query_node_container.query_nodes[qn_idx]
        for clinical_id in found_clinical_ids:
            reference_ids = cache.ids[query_node.raw_query_hash()][clinical_id]
            if reference_ids is not None:
                all_extended[query_node.query_level].update(reference_ids)
            for reference_id in (reference_ids if reference_ids is not None else [None]):
                id_cache = cache.ids[query_node.raw_query_hash()]
                reference_width = len(id_cache[clinical_id]) if reference_id is not None else -1
                clinical_width = len(id_cache)
                reasons[clinical_id].append(
                    ExtendedMatchReason(
                        query_node,
                        reference_width,
                        clinical_width,
                        clinical_id,
                        reference_id,
                        show_in_ui
                    ))
    return reasons, all_extended


async def get_docs_results(matchengine: MatchEngine, needed_clinical, needed_extended):
    """
    Matching criteria for clinical and extended_attributes values can be set/extended in config.json
    :param matchengine:
    :param needed_clinical:
    :param needed_extended:
    :return:
    """
    clinical_projection = matchengine.match_criteria_transform.projections["clinical"]
    clinical_query = MongoQuery({"_id": {"$in": list(needed_clinical)}})
    db_calls = list()
    db_calls.append(perform_db_call(matchengine, "clinical", clinical_query, clinical_projection))
    for extended_collection, extended_ids in needed_extended.items():
        genomic_query = MongoQuery({"_id": {"$in": list(extended_ids)}})
        projection = matchengine.match_criteria_transform.projections[extended_collection]
        db_calls.append(perform_db_call(matchengine, extended_collection, genomic_query, projection))

    results = await asyncio.gather(*db_calls)
    return results


def get_valid_reasons(matchengine: MatchEngine, possible_reasons, clinical_ids, genomic_ids):
    valid_reasons = {}
    for clinical_id, reasons in possible_reasons.items():
        if clinical_id in clinical_ids:
            list_o_reasons = list()
            for reason in reasons:
                if ((reason.__class__ is ExtendedMatchReason
                     and (reason.query_node.exclusion or reason.reference_id in genomic_ids[
                            reason.query_node.query_level]))
                        or (reason.__class__ is ClinicalMatchReason
                            and (matchengine.report_all_clinical_reasons
                                 or frozenset(reason.query_part.query.keys())
                                 in matchengine.match_criteria_transform.valid_clinical_reasons))):
                    list_o_reasons.append(reason)
                valid_reasons[clinical_id] = list_o_reasons

    return valid_reasons
