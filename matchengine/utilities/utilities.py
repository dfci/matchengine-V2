import glob
import logging
import os
import sys
from types import MethodType

from matchengine import query_transform
from matchengine.utilities.mongo_connection import MongoDBConnection
from matchengine.plugin_stub import QueryTransformerContainer, TrialMatchDocumentCreator, DBSecrets, QueryNodeTransformer

logging.basicConfig(level=logging.INFO)
log = logging.getLogger('matchengine')


def check_indices(me):
    """
    Ensure indexes exist on collections so queries are performant
    """
    for collection, desired_indices in me.config['indices'].items():
        indices = me.db_ro[collection].list_indexes()
        existing_indices = set()
        for index in indices:
            index_key = list(index['key'].to_dict().keys())[0]
            existing_indices.add(index_key)
        indices_to_create = set(desired_indices) - existing_indices
        for index in indices_to_create:
            log.info('Creating index %s' % index)
            me.db_rw[collection].create_index(index)


def find_plugins(me):
    """
    Plugins are *.py files located in the ./plugins directory. They must be python classes which inherit either from
    QueryTransformerContainer or TrialMatchDocumentCreator.

    For more information on how the plugins function, see the README.
    """
    log.info(f"Checking for plugins in {me.plugin_dir}")
    potential_files = glob.glob(os.path.join(me.plugin_dir, "*.py"))
    to_load = [(None, 'matchengine.query_transform')]
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
            log.info(f"Found shared plugin resource {item_name} in module {module_name}, path {dir_path}")
            setattr(me.match_criteria_transform.transform, item_name, getattr(module, item_name))
        for item_name in module.__export__:
            item = getattr(module, item_name)
            log.info(f"Found exported plugin item {item_name} in module {module_name}, path {dir_path}")
            if issubclass(item, QueryTransformerContainer):
                log.info(f"Loading QueryTransformerContainer {item_name} type: {item}")
                query_transform.attach_transformers_to_match_criteria_transform(me.match_criteria_transform,
                                                                                item)
            elif issubclass(item, TrialMatchDocumentCreator):
                if item_name == me.match_document_creator_class:
                    log.info(f"Loading TrialMatchDocumentCreator {item_name} type: {item}")
                    setattr(me,
                            'create_trial_matches',
                            MethodType(getattr(item,
                                               'create_trial_matches'),
                                       me))
            elif issubclass(item, DBSecrets):
                if item_name == me.db_secrets_class:
                    log.info(f"Loading DBSecrets {item_name} type: {item}")
                    secrets = item().get_secrets()
                    setattr(MongoDBConnection, 'secrets', secrets)
            elif issubclass(item, QueryNodeTransformer):
                if item_name == me.query_node_transformer_class:
                    log.info(f"Loading QueryNodeTransformer {item_name} type: {item}")
                    setattr(me,
                            "query_node_transform",
                            MethodType(getattr(item,
                                               "query_node_transform"),
                                       me))
