from __future__ import annotations

import json
from types import MethodType
from typing import Type

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
