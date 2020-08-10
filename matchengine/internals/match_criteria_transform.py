from __future__ import annotations

import logging
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
    trial_collection: str = None
    trial_identifier: str = None
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

        # values used to match extended_attributes/clinical information to trials.
        # For more details and explanation, see the README
        self.projections = {
            collection: {field: 1 for field in fields} for collection, fields in config["projections"].items()
        }
        self.query_transformers = AllTransformersContainer(self)
        self.trial_collection = config.get('trial_collection', 'trial')
        self.trial_identifier = config.get('trial_identifier', 'protocol_no')
        self.match_trial_link_id = config.get('match_trial_link_id', self.trial_identifier)
        self.transform = TransformFunctions()
        self.valid_clinical_reasons = {
            frozenset(reasons)
            for reasons in
            self.config.get("valid_clinical_reasons", list())
        }
        # By default, only trials that are "Open to Accrual" are run.
        # This value by default is stored inside a "_summary" object.
        # If a different field indicates trial accrual status, that is set here.
        self.use_custom_trial_status_key = self.config.get("trial_status_key", None)
        if self.use_custom_trial_status_key is not None:
            self.custom_status_key_name = self.use_custom_trial_status_key.get("key_name", None)
            self.custom_open_to_accrual_vals = []
            if self.use_custom_trial_status_key.get("open_to_accrual_values", None) is None:
                logging.error("Missing config field: trial_status_key.open_to_accrual_values. Must contain list of "
                              "acceptable 'open to accrual' values")
                exit(1)

            # be case insensitive when checking trial open/close status
            for val in self.use_custom_trial_status_key.get("open_to_accrual_values", None):
                if isinstance(val, str):
                    self.custom_open_to_accrual_vals.append(val.lower().strip())
                else:
                    self.custom_open_to_accrual_vals.append(val)
