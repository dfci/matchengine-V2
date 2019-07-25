from typing import NoReturn

import re

from match_criteria_transform import get_query_part_by_key
from matchengine_types import QueryNode
from plugin_stub import QueryNodeTransformer


class DFCIQueryNodeTransformer(QueryNodeTransformer):
    def query_node_transform(self, query_node: QueryNode) -> NoReturn:
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


__export__ = ["DFCIQueryNodeTransformer"]
