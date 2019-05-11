import json
import datetime
from dateutil.relativedelta import relativedelta


class MatchCriteriaTransform(object):
    resources: dict = None
    config: dict = None
    trial_key_mappings: dict = None
    primary_collection: str = None
    primary_collection_unique_field: str = None
    collection_mappings: dict = None

    def __init__(self, config):
        self.resources = dict()
        self.config = config
        self.trial_key_mappings = config['trial_key_mappings']
        self.primary_collection = config['primary_collection']
        self.primary_collection_unique_field = config['primary_collection_unique_field']
        self.collection_mappings = config['collection_mappings']

    def nomap(self, **kwargs):
        trial_path = kwargs['trial_path']
        sample_key = kwargs['sample_key']
        trial_key = kwargs['trial_key']
        trial_value = kwargs['trial_value']
        return {sample_key: trial_value}

    def age_range_to_date_query(self, **kwargs):
        sample_key = kwargs['sample_key']
        trial_value = kwargs['trial_value']
        operator_map = {
            "==": "$eq",
            "<=": "$lte",
            ">=": "$gte",
            ">": "$gt",
            "<": "$lt"
        }
        operator = ''.join([i for i in trial_value if not i.isdigit()])
        years = int(''.join([i for i in trial_value if i.isdigit() or i == '.']))
        current_date = datetime.date.today()
        query_date = current_date - relativedelta(years=years)
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
        match_value = resource.setdefault(trial_value, trial_value)  # TODO: fix
        if isinstance(match_value, list):
            return {sample_key: {"$in": match_value}}
        else:
            return {sample_key: match_value}

    def bool_from_text(self, **kwargs):
        trial_value = kwargs['trial_value']
        sample_key = kwargs['sample_key']
        if trial_value.upper() == 'TRUE':
            return {sample_key: True}
        elif trial_value.upper() == 'FALSE':
            return {sample_key: False}
