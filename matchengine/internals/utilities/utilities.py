from __future__ import annotations

import glob
import logging
import os
import sys
from types import MethodType
from typing import TYPE_CHECKING

from bson import ObjectId

from matchengine.internals import query_transform
from matchengine.internals.database_connectivity.mongo_connection import MongoDBConnection
from matchengine.internals.plugin_helpers.plugin_stub import (
    QueryTransformerContainer,
    TrialMatchDocumentCreator,
    DBSecrets,
    QueryNodeTransformer,
    QueryNodeClinicalIDsSubsetter,
    QueryNodeContainerTransformer
)

if TYPE_CHECKING:
    from typing import Dict, List
    from matchengine.internals.engine import MatchEngine
    from matchengine.internals.typing.matchengine_types import MongoQuery

logging.basicConfig(level=logging.INFO)
log = logging.getLogger('matchengine')


async def perform_db_call(matchengine: MatchEngine, collection: str, query: MongoQuery, projection: Dict) -> List:
    """
    Asynchronously executes a find query on the database, with specified query and projection and a collection
    Used to parallelize DB calls, with asyncio.gather
    """
    return await matchengine.async_db_ro[collection].find(query, projection).to_list(None)


def find_plugins(matchengine: MatchEngine):
    """
    Plugins are *.py files located in the ./plugins directory. They must be python classes which inherit either from
    QueryTransformerContainer or TrialMatchDocumentCreator.

    For more information on how the plugins function, see the README.
    """
    log.info(f"Checking for plugins in {matchengine.plugin_dir}")
    potential_files = glob.glob(os.path.join(matchengine.plugin_dir, "*.py"))
    to_load = [(None, 'matchengine.internals.query_transform')]
    for potential_file_path in potential_files:
        dir_path = os.path.dirname(potential_file_path)
        module_name = ''.join(os.path.basename(potential_file_path).split('.')[0:-1])
        to_load.append((dir_path, module_name))
    for dir_path, module_name in to_load:
        if dir_path is not None:
            sys.path.append(dir_path)
        module = __import__(module_name)
        module_path = module_name.split('.')
        if len(module_path) > 1:
            for sub_item in module_path[1::]:
                module = getattr(module, sub_item)
        if dir_path is not None:
            sys.path.pop()
        for item_name in getattr(module, '__shared__', list()):
            if matchengine.debug:
                log.info(f"Found shared plugin resource {item_name} in module {module_name}, path {dir_path}")
            setattr(matchengine.match_criteria_transform.transform, item_name, getattr(module, item_name))
        for item_name in module.__export__:
            item = getattr(module, item_name)
            if matchengine.debug:
                log.info(f"Found exported plugin item {item_name} in module {module_name}, path {dir_path}")
            if issubclass(item, QueryTransformerContainer):
                if matchengine.debug:
                    log.info(f"Loading QueryTransformerContainer {item_name} type: {item}")
                query_transform.attach_transformers_to_match_criteria_transform(matchengine.match_criteria_transform,
                                                                                item)
            elif issubclass(item, TrialMatchDocumentCreator):
                if item_name == matchengine.match_document_creator_class:
                    if matchengine.debug:
                        log.info(f"Loading TrialMatchDocumentCreator {item_name} type: {item}")
                    setattr(matchengine,
                            'create_trial_matches',
                            MethodType(getattr(item,
                                               'create_trial_matches',
                                               matchengine.create_trial_matches),
                                       matchengine))
                    setattr(matchengine,
                            'results_transformer',
                            MethodType(getattr(item,
                                               'results_transformer',
                                               matchengine.results_transformer),
                                       matchengine))
            elif issubclass(item, DBSecrets):
                if item_name == matchengine.db_secrets_class:
                    if matchengine.debug:
                        log.info(f"Loading DBSecrets {item_name} type: {item}")
                    secrets = item().get_secrets()
                    setattr(MongoDBConnection, 'secrets', secrets)
            elif issubclass(item, QueryNodeTransformer):
                if item_name == matchengine.query_node_transformer_class:
                    if matchengine.debug:
                        log.info(f"Loading QueryNodeTransformer {item_name} type: {item}")
                    setattr(matchengine,
                            "query_node_transform",
                            MethodType(getattr(item,
                                               "query_node_transform"),
                                       matchengine))
            elif issubclass(item, QueryNodeClinicalIDsSubsetter):
                if item_name == matchengine.query_node_subsetter_class:
                    if matchengine.debug:
                        log.info(f"Loading QueryNodeClinicalIDsSubsetter {item_name} type: {item}")
                    setattr(matchengine,
                            "extended_query_node_clinical_ids_subsetter",
                            MethodType(getattr(item,
                                               "extended_query_node_clinical_ids_subsetter"),
                                       matchengine))
                    setattr(matchengine,
                            "clinical_query_node_clinical_ids_subsetter",
                            MethodType(getattr(item,
                                               "clinical_query_node_clinical_ids_subsetter"),
                                       matchengine))
            elif issubclass(item, QueryNodeContainerTransformer):
                if item_name == matchengine.query_node_container_transformer_class:
                    if matchengine.debug:
                        log.info(f"Loading QueryNodeContainerTransformer {item_name} type: {item}")
                    setattr(matchengine,
                            "query_node_container_transform",
                            MethodType(getattr(item,
                                               "query_node_container_transform"),
                                       matchengine))


def get_sort_order(matchengine: MatchEngine, match_document: Dict) -> list:
    """
    Sort trial matches based on sorting order specified in config.json under the key 'trial_match_sorting'.

    The function will iterate over the objects in the 'trial_match_sorting', and then look for that value
    in the trial_match document, placing it in an array.

    If being displayed, the matchminerAPI filters the array to output a single sort number.

    The sorting is currently organized as follows:
    1. MMR status
    2. Tumor Mutational Burden
    3. UVA/POLE/APOBEC/Tobacco Status
    4. Tier 1
    5. Tier 2
    6. CNV
    7. Tier 3
    8. Tier 4
    9. wild type
    10. Variant Level
    11. Gene-level
    12. Exact cancer match
    13. General cancer match (all solid/liquid)
    14. DFCI Coordinating Center
    15. All other Coordinating centers
    16. Protocol Number
    """
    sort_map = matchengine.config['trial_match_sorting']
    sort_array = list()

    for sort_dimension in sort_map:
        sort_index = 99
        for sort_key in sort_dimension:
            if sort_key in match_document:
                sorting_vals = sort_dimension[sort_key]
                is_any = sorting_vals.get("ANY_VALUE", None)
                trial_match_val = str(match_document[sort_key]) if is_any is None else "ANY_VALUE"

                if (trial_match_val is not None and trial_match_val in sorting_vals) or is_any is not None:
                    matched_sort_int = sort_dimension[sort_key][trial_match_val]
                    if matched_sort_int < sort_index:
                        sort_index = matched_sort_int

        sort_array.append(sort_index)

    # If an idenfitifer is not a protocol id (e.g. 17-251) then skip replacing
    identifier = match_document.get(matchengine.match_criteria_transform.trial_identifier, None)
    if isinstance(identifier, ObjectId) or identifier is None:
        pass
    else:
        sort_array.append(int(identifier.replace("-", "")))

    return sort_array
