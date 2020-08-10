from __future__ import annotations

import datetime
import re

from dateutil.relativedelta import relativedelta

from matchengine.internals.query_transform import QueryTransformerContainer
from matchengine.internals.typing.matchengine_types import QueryTransformerResult


class DFCIQueryTransformers(QueryTransformerContainer):
    def tmb_range_to_query(self, **kwargs):
        sample_key = kwargs['sample_key']
        trial_value = kwargs['trial_value']
        operator_map = {
            "==": "$eq",
            "<=": "$lte",
            ">=": "$gte",
            ">": "$gt",
            "<": "$lt"
        }
        operator = ''.join([i for i in trial_value if not i.isdigit() and i != '.'])
        numeric = "".join([i for i in trial_value if i.isdigit() or i == '.'])
        if numeric.startswith('.'):
            numeric = '0' + numeric
        return QueryTransformerResult({sample_key: {operator_map[operator]: float(numeric)}}, False)

    def bool_from_text(self, **kwargs):
        trial_value = kwargs['trial_value']
        sample_key = kwargs['sample_key']
        if trial_value.upper() == 'TRUE':
            return QueryTransformerResult({sample_key: True}, False)
        elif trial_value.upper() == 'FALSE':
            return QueryTransformerResult({sample_key: False}, False)

    def cnv_map(self, **kwargs):
        # Heterozygous deletion,
        # Gain,
        # Homozygous deletion,
        # High level amplification,
        # Neu

        trial_value = kwargs['trial_value']
        sample_key = kwargs['sample_key']
        cnv_map = {
            "High Amplification": "High level amplification",
            "Homozygous Deletion": "Homozygous deletion",
            'Low Amplification': 'Gain',
            'Heterozygous Deletion': 'Heterozygous deletion'

        }

        trial_value, negate = self.transform.is_negate(trial_value)
        if trial_value in cnv_map:
            return QueryTransformerResult({sample_key: cnv_map[trial_value]}, negate)
        else:
            return QueryTransformerResult({sample_key: trial_value}, negate)

    def variant_category_map(self, **kwargs):
        trial_value = kwargs['trial_value']
        sample_key = kwargs['sample_key']
        variant_category_map = {
            "Copy Number Variation".lower(): "CNV",
            "Any Variation".lower(): {"$in": ["MUTATION", "CNV"]},
            "Structural Variation".lower(): "SV"
        }

        trial_value, negate = self.transform.is_negate(trial_value)

        # if a curation calls for a Structural Variant, search the free text in the extended_attributes document under
        # STRUCTURAL_VARIANT_COMMENT for mention of the TRUE_HUGO_SYMBOL
        if trial_value == 'Structural Variation':
            sample_value = variant_category_map.get(trial_value.lower())
            results = QueryTransformerResult()
            results.add_result({'STRUCTURAL_VARIANT_COMMENT': None}, negate)
            results.add_result({'STRUCTURED_SV': None, sample_key: sample_value}, negate)
            return results
        elif trial_value.lower() in variant_category_map:
            return QueryTransformerResult({sample_key: variant_category_map[trial_value.lower()]}, negate)
        else:
            return QueryTransformerResult({sample_key: trial_value.upper()}, negate)

    def wildcard_regex(self, **kwargs):
        """
        When trial curation criteria include a wildcard prefix (e.g. WILDCARD_PROTEIN_CHANGE), a extended_attributes query must
        use a $regex to search for all extended_attributes documents which match the protein prefix.

        E.g.
        Trial curation match clause:
        | extended_attributes:
        |    wildcard_protein_change: p.R132

        Patient extended_attributes data:
        |    true_protein_change: p.R132H

        The above should match in a mongo query.
        """
        trial_value = kwargs['trial_value']

        # By convention, all protein changes being with "p."

        trial_value, negate = self.transform.is_negate(trial_value)
        if not trial_value.startswith('p.'):
            trial_value = re.escape('p.' + trial_value)
        trial_value = f'^{trial_value}[ACDEFGHIKLMNPQRSTVWY]$'
        return QueryTransformerResult({kwargs['sample_key']: {'$regex': re.compile(trial_value, re.IGNORECASE)}},
                                      negate)

    def mmr_ms_map(self, **kwargs):
        mmr_map = {
            'MMR-Proficient': 'Proficient (MMR-P / MSS)',
            'MMR-Deficient': 'Deficient (MMR-D / MSI-H)',
            'MSI-H': 'Deficient (MMR-D / MSI-H)',
            'MSI-L': 'Proficient (MMR-P / MSS)',
            'MSS': 'Proficient (MMR-P / MSS)'
        }
        trial_value = kwargs['trial_value']
        trial_value, negate = self.transform.is_negate(trial_value)
        sample_key = kwargs['sample_key']
        sample_value = mmr_map[trial_value]
        return QueryTransformerResult({sample_key: sample_value}, negate)


__export__ = ["DFCIQueryTransformers"]
