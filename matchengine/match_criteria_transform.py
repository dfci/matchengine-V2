from __future__ import annotations


class AllTransformersContainer(object):
    def __init__(self, match_criteria_transform):
        self._ = match_criteria_transform


class TransformFunctions(object):
    pass


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

    def __init__(self, config):
        self.resources = dict()
        self.config = config
        self.trial_key_mappings = config['trial_key_mappings']

        # values used to match genomic/clinical information to trials. for more details and explanation, see the README
        self.clinical_projection = {proj: 1 for proj in config["match_criteria"]['clinical']}
        self.genomic_projection = {proj: 1 for proj in config["match_criteria"]['genomic']}
        self.trial_projection = {proj: 1 for proj in config["match_criteria"]['trial']}
        self.query_transformers = AllTransformersContainer(self)
        self.transform = TransformFunctions()
