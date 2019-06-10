import json
import datetime
import re
from dateutil.relativedelta import relativedelta


class MatchCriteriaTransform(object):
    """
    A class used to transform values used in trial curation into values which can be used to query genomic and
    clinical collections.

    For more details and examples, please see the README.
    """
    CLINICAL: str = "clinical"
    GENOMIC: str = "genomic"

    resources: dict = None
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
    internal_id_mapping = {'dose': 'level_internal_id',
                           'step': 'step_internal_id',
                           'arm': 'arm_internal_id'}

    def __init__(self, config):
        self.resources = dict()
        self.config = config
        self.trial_key_mappings = config['trial_key_mappings']

        # values used to match genomic/clinical information to trials. for more details and explanation, see the README
        self.clinical_projection = {proj: 1 for proj in config["match_criteria"]['clinical']}
        self.genomic_projection = {proj: 1 for proj in config["match_criteria"]['genomic']}
        self.trial_projection = {proj: 1 for proj in config["match_criteria"]['trial']}

    def is_negate(self, trial_value):
        """
        Example: !EGFR => (True, EGFR)

        :param trial_value:
        :return:
        """
        negate = True if isinstance(trial_value, str) and trial_value[0] == '!' else False
        trial_value = trial_value[1::] if negate else trial_value
        return trial_value, negate

    def nomap(self, **kwargs):
        trial_path = kwargs['trial_path']
        trial_key = kwargs['trial_key']
        trial_value = kwargs['trial_value']
        sample_key = kwargs['sample_key']
        trial_value, negate = self.is_negate(trial_value)
        return {sample_key: trial_value}, negate

    def age_range_to_date_query(self, **kwargs):
        sample_key = kwargs['sample_key']
        trial_value = kwargs['trial_value']
        operator_map = {
            "==": "$eq",
            "<=": "$gte",
            ">=": "$lte",
            ">": "$lt",
            "<": "$gt"
        }
        # funky logic is because 1 month curation is curated as "0.083" (1/12 a year)
        operator = ''.join([i for i in trial_value if not i.isdigit() and i != '.'])
        numeric = "".join([i for i in trial_value if i.isdigit() or i == '.'])
        split_time = numeric.split('.')
        years = int(split_time[0])
        months_fraction = float(split_time[1]) if len(split_time) > 1 else 0
        months = int(months_fraction * 12)
        current_date = datetime.date.today()
        query_date = current_date - relativedelta(years=years, months=months)
        query_datetime = datetime.datetime(query_date.year, query_date.month, query_date.day, 0, 0, 0, 0)
        return {sample_key: {operator_map[operator]: query_datetime}}, False

    def external_file_mapping(self, **kwargs):
        trial_value = kwargs['trial_value']
        sample_key = kwargs['sample_key']
        file = kwargs['file']
        if file not in self.resources:
            with open(file) as file_handle:
                self.resources[file] = json.load(file_handle)
        resource = self.resources[file]
        trial_value, negate = self.is_negate(trial_value)
        match_value = resource.setdefault(trial_value, trial_value)  # TODO: fix
        if isinstance(match_value, list):
            return {sample_key: {"$in": sorted(match_value)}}, negate
        else:
            return {sample_key: match_value}, negate

    def bool_from_text(self, **kwargs):
        trial_value = kwargs['trial_value']
        sample_key = kwargs['sample_key']
        if trial_value.upper() == 'TRUE':
            return {sample_key: True}, False
        elif trial_value.upper() == 'FALSE':
            return {sample_key: False}, False

    def to_upper(self, **kwargs):
        trial_value = kwargs['trial_value']
        sample_key = kwargs['sample_key']
        trial_value, negate = self.is_negate(trial_value)
        return {sample_key: trial_value.upper()}, negate

    def cnv_map(self, **kwargs):
        # Heterozygous deletion,
        # Gain,
        # Homozygous deletion,
        # High level amplification,
        # Neu

        trial_value = kwargs['trial_value']
        sample_key = kwargs['sample_key']
        cnv_map = {
            "High Amplification": "High level amplification"
        }

        trial_value, negate = self.is_negate(trial_value)
        if trial_value in cnv_map:
            return {sample_key: cnv_map[trial_value]}, negate
        else:
            return {sample_key: trial_value}, negate

    def variant_category_map(self, **kwargs):
        trial_value = kwargs['trial_value']
        sample_key = kwargs['sample_key']
        query_list = kwargs['and_query']
        variant_category_map = {
            "Copy Number Variation": "CNV"
        }

        trial_value, negate = self.is_negate(trial_value)

        # if a curation calls for a Structural Variant, search the free text in the genomic document under
        # STRUCTURAL_VARIANT_COMMENT for mention of the TRUE_HUGO_SYMBOL
        if trial_value == 'Structural Variation':
            gene = ''
            for query_tuple in query_list:
                if 'TRUE_HUGO_SYMBOL' in query_tuple[1]:
                    gene = query_tuple[1]['TRUE_HUGO_SYMBOL']

            return {'STRUCTURAL_VARIANT_COMMENT': {"$regex": gene}}, negate
        elif trial_value in variant_category_map:
            return {sample_key: variant_category_map[trial_value]}, negate
        else:
            return {sample_key: trial_value.upper()}, negate

    def wildcard_regex(self, **kwargs):
        """
        When trial curation criteria include a wildcard prefix (e.g. WILDCARD_PROTEIN_CHANGE), a genomic query must
        use a $regex to search for all genomic documents which match the protein prefix.

        E.g.
        Trial curation match clause:
        | genomic:
        |    wildcard_protein_change: p.R132

        Patient genomic data:
        |    true_protein_change: p.R132H

        The above should match in a mongo query.
        """
        trial_value = kwargs['trial_value']

        # By convention, all protein changes being with "p."
        if not trial_value.startswith('p.'):
            trial_value = 'p.' + trial_value

        trial_value, negate = self.is_negate(trial_value)
        trial_value = '^%s[A-Z]' % trial_value
        return {kwargs['sample_key']: {'$regex': trial_value}}, negate

    def and_query_transform(self, **kwargs):
        """
        If a trial curation key/value requires alteration to a separate AND clause in the mongo query, do that here.
        :return:
        """
        trial_key = kwargs['trial_key']
        trial_value = kwargs['trial_value']

        # If a trial curation calls for a structural variant but does NOT have the structured SV data field
        # FUSION_PARTNER_HUGO_SYMBOL, then the genomic query is done using a regex search of the free text
        # STRUCTURAL_VARIANT_COMMENT field on the patient's genomic document.
        if trial_key == 'variant_category' and trial_value == 'Structural Variation':
            and_query = kwargs['and_query']
            for negate, query in and_query:
                if 'TRUE_HUGO_SYMBOL' in query and 'FUSION_PARTNER_HUGO_SYMBOL' not in query:
                    index = and_query.index([negate, query])
                    del and_query[index]
            return and_query
        else:
            return kwargs['and_query']
