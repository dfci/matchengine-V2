from __future__ import annotations

import datetime
import json
from types import MethodType
from typing import Type

from dateutil.relativedelta import relativedelta

from matchengine.internals.match_criteria_transform import MatchCriteriaTransform
from matchengine.internals.plugin_helpers.plugin_stub import QueryTransformerContainer
from matchengine.internals.typing.matchengine_types import QueryTransformerResult


def is_negate(trial_value):
    """
    Example: !EGFR => (True, EGFR)

    :param trial_value:
    :return:
    """
    negate = True if trial_value.__class__ is str and trial_value and trial_value[0] == '!' else False
    trial_value = trial_value[1::] if negate else trial_value
    return trial_value, negate


def attach_transformers_to_match_criteria_transform(match_criteria_transform: MatchCriteriaTransform,
                                                    query_transformer_container: Type[QueryTransformerContainer]):
    for attr in dir(query_transformer_container):
        if not attr.startswith('_'):
            method = getattr(query_transformer_container, attr)
            setattr(match_criteria_transform.query_transformers,
                    attr,
                    MethodType(method, match_criteria_transform.query_transformers))


class BaseTransformers(QueryTransformerContainer):
    def age_range_to_date_query(self, **kwargs):
        sample_key = kwargs['sample_key']
        trial_value = kwargs['trial_value']
        current_date = kwargs.get('compare_date', datetime.date.today())
        operator_map = {
            "==": "$eq",
            "<=": "$gte",
            ">=": "$lte",
            ">": "$lte",
            "<": "$gte"
        }
        # funky logic is because 1 month curation is curated as "0.083" (1/12 a year)
        operator = ''.join([i for i in trial_value if not i.isdigit() and i != '.'])
        numeric = "".join([i for i in trial_value if i.isdigit() or i == '.'])
        if numeric.startswith('.'):
            numeric = '0' + numeric
        split_time = numeric.split('.')
        years = int(split_time[0] if split_time[0].isdigit() else 0)
        months_fraction = float('0.' + split_time[1]) if len(split_time) > 1 else 0
        months = round(months_fraction * 12)
        query_date = current_date + (- relativedelta(years=years, months=months))
        query_datetime = datetime.datetime(query_date.year, query_date.month, query_date.day, query_date.hour, 0, 0, 0)
        return QueryTransformerResult({sample_key: {operator_map[operator]: query_datetime}}, False)

    def age_range_to_date_int_query(self, **kwargs):
        sample_key = kwargs['sample_key']
        trial_value = kwargs['trial_value']
        current_date = kwargs.get('compare_date', datetime.date.today())
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
        if numeric.startswith('.'):
            numeric = '0' + numeric
        split_time = numeric.split('.')
        years = int(split_time[0] if split_time[0].isdigit() else 0)
        months_fraction = float('0.' + split_time[1]) if len(split_time) > 1 else 0
        months = round(months_fraction * 12)
        query_date = current_date + (- relativedelta(years=years, months=months))
        return QueryTransformerResult({sample_key: {operator_map[operator]: int(query_date.strftime('%Y%m%d'))}}, False)

    def nomap(self, **kwargs):
        trial_path = kwargs['trial_path']
        trial_key = kwargs['trial_key']
        trial_value = kwargs['trial_value']
        sample_key = kwargs['sample_key']
        trial_value, negate = is_negate(trial_value)
        return QueryTransformerResult({sample_key: trial_value}, negate)

    def external_file_mapping(self, **kwargs):
        trial_value = kwargs['trial_value']
        sample_key = kwargs['sample_key']
        file = kwargs['file']
        if file not in self.resources:
            with open(self.resource_paths[file]) as file_handle:
                self.resources[file] = json.load(file_handle)
        resource = self.resources[file]
        trial_value, negate = is_negate(trial_value)
        match_value = resource.setdefault(trial_value, trial_value)
        if match_value.__class__ is list:
            return QueryTransformerResult({sample_key: {"$in": sorted(match_value)}}, negate)
        else:
            return QueryTransformerResult({sample_key: match_value}, negate)

    def to_upper(self, **kwargs):
        trial_value = kwargs['trial_value']
        sample_key = kwargs['sample_key']
        trial_value, negate = is_negate(trial_value)
        return QueryTransformerResult({sample_key: trial_value.upper()}, negate)


__export__ = ["BaseTransformers"]
__shared__ = ["is_negate"]
