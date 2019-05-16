import json
import datetime
from dateutil.relativedelta import relativedelta


class MatchCriteriaTransform(object):
    CLINICAL: str = "clinical"

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

    def __init__(self, config):
        self.resources = dict()
        self.config = config
        self.trial_key_mappings = config['trial_key_mappings']

    def nomap(self, **kwargs):
        trial_path = kwargs['trial_path']
        trial_key = kwargs['trial_key']
        trial_value = kwargs['trial_value']
        sample_key = kwargs['sample_key']
        if isinstance(trial_value, str) and trial_value[0] == '!':
            return {sample_key: {"$ne": trial_value[1::]}}
        else:
            return {sample_key: trial_value}

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
        split_time = trial_value.split('.')
        years = int(split_time[0]) if trial_value[0].isdigit() else 0
        months_fraction = float(split_time[1]) if len(split_time) > 1 else 0
        months = int(months_fraction * 12)
        current_date = datetime.date.today()
        query_date = current_date - relativedelta(years=years, months=months)
        query_datetime = datetime.datetime(query_date.year, query_date.month, query_date.day, 0, 0, 0, 0)
        return {sample_key: {operator_map[operator]: query_datetime}}

    def external_file_mapping(self, **kwargs):
        trial_value = kwargs['trial_value']
        sample_key = kwargs['sample_key']
        file = kwargs['file']
        if file not in self.resources:
            with open(file) as file_handle:
                self.resources[file] = json.load(file_handle)
        resource = self.resources[file]
        negate = True if trial_value[0] == '!' else False
        if negate:
            trial_value = trial_value[1::]
        match_value = resource.setdefault(trial_value, trial_value)  # TODO: fix
        if isinstance(match_value, list):
            if negate:
                return {sample_key: {"$nin": match_value}}
            else:
                return {sample_key: {"$in": match_value}}
        else:
            if negate:
                return {sample_key: {"$ne": match_value}}
            else:
                return {sample_key: match_value}

    def bool_from_text(self, **kwargs):
        trial_value = kwargs['trial_value']
        sample_key = kwargs['sample_key']
        if trial_value.upper() == 'TRUE':
            return {sample_key: True}
        elif trial_value.upper() == 'FALSE':
            return {sample_key: False}

    def to_upper(self, **kwargs):
        trial_value = kwargs['trial_value']
        sample_key = kwargs['sample_key']
        if isinstance(trial_value, str) and trial_value[0] == '!':
            return {sample_key: {"$ne": trial_value[1::].upper()}}
        else:
            return {sample_key: trial_value.upper()}

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

        if trial_value in cnv_map:
            return {sample_key: cnv_map[trial_value]}
        else:
            return {sample_key: trial_value}

    def variant_category_map(self, **kwargs):
        trial_value = kwargs['trial_value']
        sample_key = kwargs['sample_key']
        vc_map = {
            "Copy Number Variation": "CNV"
        }

        if trial_value in vc_map:
            return {sample_key: vc_map[trial_value]}
        else:
            return {sample_key: trial_value.upper()}
