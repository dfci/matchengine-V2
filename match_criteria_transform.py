import re

from matchengine_types import QueryNode, QueryPart
from itertools import chain


def get_query_part_by_key(query_node: QueryNode, key: str) -> QueryPart:
    return next(chain((query_part
                       for query_part in query_node.query_parts
                       if key in query_part.query),
                      iter([None])))


def query_node_transform(query_node: QueryNode):
    """
    If a trial curation key/value requires alteration to a separate AND clause in the mongo query, do that here.
    Used to modify a query part dependent on another query part
    :return:
    """

    # If a trial curation calls for a structural variant but does NOT have the structured SV data field
    # FUSION_PARTNER_HUGO_SYMBOL, then the genomic query is done using a regex search of the free text
    # STRUCTURAL_VARIANT_COMMENT field on the patient's genomic document.
    whole_query = query_node.extract_raw_query()
    # encode as full search criteria
    if 'STRUCTURAL_VARIANT_COMMENT' in whole_query:
        gene_part = get_query_part_by_key(query_node, 'TRUE_HUGO_SYMBOL')
        sv_part = get_query_part_by_key(query_node, 'STRUCTURAL_VARIANT_COMMENT')
        gene_part.render = False
        gene = whole_query.pop('TRUE_HUGO_SYMBOL')
        sv_part.query['STRUCTURAL_VARIANT_COMMENT'] = re.compile(rf"(.*\W{gene}\W.*)|(^{gene}\W.*)|(.*\W{gene}$)",
                                                                 re.IGNORECASE)
    # if fusion_partner is present, query left/right gene
    if 'FUSION_PARTNER_HUGO_SYMBOL' in whole_query:
        pass

    # if signature curation is passed, do not query TRUE_HUGO_SYMBOL
    if {'UVA_STATUS',
        'TABACCO_STATUS',
        'POLE_STATUS',
        'TEMOZOLOMIDE_STATUS',
        'MMR_STATUS',
        'APOBEC_STATUS'}.intersection(set(whole_query.keys())):
        gene_part = get_query_part_by_key(query_node, 'TRUE_HUGO_SYMBOL')
        if gene_part is not None:
            gene_part.render = False


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
