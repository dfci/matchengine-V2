from __future__ import annotations

import os

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from typing import (
        Callable,
        Any
    )


class AllTransformersContainer(object):
    def __init__(self, match_criteria_transform: MatchCriteriaTransform):
        self._ = match_criteria_transform

    @property
    def resources(self):
        return self._.resources

    @property
    def transform(self):
        return self._.transform

    @property
    def resource_paths(self):
        return self._.resource_paths


class TransformFunctions(object):
    if TYPE_CHECKING:
        is_negate: Callable[[Any], bool]


class MatchCriteriaTransform(object):
    """
    A class used to transform values used in trial curation into values which can be used to query extended_attributes and
    clinical collections.

    For more details and examples, please see the README.
    """

    resources: dict = None
    query_transformers: AllTransformersContainer
    transform: TransformFunctions
    valid_clinical_reasons: set
    config: dict = None
    trial_key_mappings: dict = None
    primary_collection_unique_field: str = "_id"
    ctml_collection_mappings: dict = None
    level_mapping = {
        'dose_level': 'dose',
        'arm': 'arm',
        'step': 'step'
    }
    suspension_mapping = {
        'dose_level': 'level_suspended',
        'arm': 'arm_suspended'
    }
    internal_id_mapping = {'dose': 'level_internal_id',
                           'step': 'step_internal_id',
                           'arm': 'arm_internal_id'}
    code_mapping = {'step': 'step_code',
                    'arm': 'arm_code',
                    'dose': 'level_code'}

    def __init__(self, config, resource_dirs):
        self.resources = dict()
        self.resource_paths = dict()
        for resource_dir in resource_dirs:
            for file_path in os.listdir(resource_dir):
                self.resource_paths[os.path.basename(file_path)] = os.path.join(resource_dir, file_path)
        self.config = config
        self.ctml_collection_mappings = config['ctml_collection_mappings']

        # values used to match extended_attributes/clinical information to trials. for more details and explanation, see the README
        self.projections = {
            collection: {field: 1 for field in fields} for collection, fields in config["projections"].items()
        }
        self.query_transformers = AllTransformersContainer(self)
        self.transform = TransformFunctions()
        self.valid_clinical_reasons = {
            frozenset(reasons)
            for reasons in
            self.config.get("valid_clinical_reasons", list())
        }
