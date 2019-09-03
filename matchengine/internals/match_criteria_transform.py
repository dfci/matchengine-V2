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
    A class used to transform values used in trial curation into values which can be used to query genomic and
    clinical collections.

    For more details and examples, please see the README.
    """
    CLINICAL: str = "clinical"
    GENOMIC: str = "genomic"

    resources: dict = None
    query_transformers: AllTransformersContainer
    transform: TransformFunctions
    valid_clinical_reasons: set
    config: dict = None
    trial_key_mappings: dict = None
    primary_collection_unique_field: str = "_id"
    collection_mappings: dict = {
        "genomic": {
            "join_field": "CLINICAL_ID"
        },
        "clinical": {
            "join_field": "_id"
        }
    }
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
        self.trial_key_mappings = config['trial_key_mappings']

        # values used to match genomic/clinical information to trials. for more details and explanation, see the README
        self.clinical_projection = {proj: 1 for proj in config["match_criteria"]['clinical']}
        self.genomic_projection = {proj: 1 for proj in config["match_criteria"]['genomic']}
        self.trial_projection = {proj: 1 for proj in config["match_criteria"]['trial']}
        self.query_transformers = AllTransformersContainer(self)
        self.transform = TransformFunctions()
        self.valid_clinical_reasons = {
            frozenset(reasons)
            for reasons in
            self.config.get("valid_clinical_reasons", list())
        }
