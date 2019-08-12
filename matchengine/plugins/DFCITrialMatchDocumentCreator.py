from __future__ import annotations

from itertools import chain
from typing import TYPE_CHECKING

from matchengine.internals.plugin_helpers.plugin_stub import TrialMatchDocumentCreator
from matchengine.internals.utilities.object_comparison import nested_object_hash

if TYPE_CHECKING:
    from matchengine.internals.typing.matchengine_types import TrialMatch
    from typing import Dict


def get_genomic_details(genomic_doc, query):
    alteration = list()

    wildtype = genomic_doc.get('WILDTYPE', None)
    true_protein = genomic_doc.get('TRUE_PROTEIN_CHANGE', None)
    hugo_symbol = genomic_doc.get('TRUE_HUGO_SYMBOL', None)
    cnv = genomic_doc.get('CNV_CALL', None)
    variant_classification = genomic_doc.get("TRUE_VARIANT_CLASSIFICATION", None)
    variant_category = genomic_doc.get('VARIANT_CATEGORY', None)
    is_variant = 'variant' if true_protein else 'gene'

    # add wildtype calls
    if wildtype:
        alteration.append('wt ')

    # add gene
    if hugo_symbol is not None:
        alteration.append(hugo_symbol)

    # add mutation
    if true_protein is not None:
        alteration.append(f' {true_protein}')

    # add cnv call
    elif cnv:
        alteration.append(f' {cnv}')

    # add variant classification
    elif variant_classification:
        alteration.append(f' {variant_classification}')

    # add structural variation
    elif variant_category == 'SV':
        left = genomic_doc.get("LEFT_PARTNER_GENE", False)
        right = genomic_doc.get("RIGHT_PARTNER_GENE", False)
        if (left is not False) or (right is not False):
            alteration.append((f'{"intergenic" if left is None else left}'
                               '-'
                               f'{"intergenic" if right is None else right}'
                               ' Structural Variation'))
            is_variant = 'structural_variant'
        else:
            sv_comment = query.get('STRUCTURAL_VARIANT_COMMENT', None)
            pattern = sv_comment.pattern.split("|")[0] if sv_comment is not None else None
            gene = pattern.replace("(.*\\W", "").replace("\\W.*)", "") if pattern is not None else None
            alteration.append(f'{gene} Structural Variation' if gene else 'Structural Variation')

    # add mutational signature
    elif variant_category == 'SIGNATURE':
        signature_type = next(chain({
                                        'UVA_STATUS',
                                        'TABACCO_STATUS',
                                        'POLE_STATUS',
                                        'TEMOZOLOMIDE_STATUS',
                                        'MMR_STATUS',
                                        'APOBEC_STATUS'
                                    }.intersection(query.keys())))
        signature_value = genomic_doc.get(signature_type, None)
        if signature_type == 'MMR_STATUS':
            is_variant = "signature"
            mapped_mmr_status = {
                'Proficient (MMR-P / MSS)': 'MMR-P/MSS',
                'Deficient (MMR-D / MSI-H)': 'MMR-D/MSI-H'
            }.get(signature_value, None)
            if mapped_mmr_status:
                alteration.append(mapped_mmr_status)
        elif signature_type is not None:
            signature_type = signature_type.replace('_STATUS', ' Signature')
            signature_type = {
                'TEMOZOLOMIDE Signature': 'Temozolomide Signature',
                'TABACCO Signature': 'Tobacco Signature'
            }.get(signature_type, signature_type)
            alteration.append(f'{str() if signature_value.lower() == "yes" else "No "}'
                              f'{signature_type}')
    return {
        'match_type': is_variant,
        'genomic_alteration': ''.join(alteration),
        'genomic_id': genomic_doc['_id'],
        **genomic_doc
    }


def get_clinical_details(clinical_doc, query):
    alteration = list()

    c_tmb, q_tmb = map(lambda x: x.get("TUMOR_MUTATIONAL_BURDEN_PER_MEGABASE", None), (clinical_doc, query))
    if all((q_tmb, c_tmb)):
        alteration.append(f"TMB = {c_tmb}")
        match_type = "tmb"
    else:
        match_type = "generic_clinical"

    return {
        'match_type': match_type,
        'genomic_alteration': ''.join(alteration),
        **clinical_doc
    }


def format_exclusion_match(trial_match: TrialMatch):
    """Format the genomic alteration for genomic documents that matched a negative clause of a match tree"""
    query = trial_match.match_reason.query_node.extract_raw_query()

    true_hugo = 'TRUE_HUGO_SYMBOL'
    protein_change_key = 'TRUE_PROTEIN_CHANGE'
    cnv_call = 'CNV_CALL'
    variant_classification = 'TRUE_VARIANT_CLASSIFICATION'
    sv_comment = 'STRUCTURAL_VARIANT_COMMENT'
    alteration = ['!']
    is_variant = 'variant' if query.setdefault(protein_change_key, None) is not None else 'gene'

    if true_hugo in query and query[true_hugo] is not None:
        alteration.append(f'{query[true_hugo]}')

    # add mutation
    if query.get(protein_change_key, None) is not None:
        if '$regex' in query[protein_change_key]:
            alteration.append(f' {query[protein_change_key]["$regex"].pattern[1:].split("[")[0]}')
        else:
            alteration.append(f' {query[protein_change_key]}')

    # add cnv call
    elif query.get(cnv_call, None) is not None:
        alteration.append(f' {query[cnv_call]}')

    # add variant classification
    elif query.get(variant_classification, None) is not None:
        alteration.append(f' {query[variant_classification]}')

    # add structural variation
    elif query.get(sv_comment, None) is not None:
        pattern = query[sv_comment].pattern.split("|")[0]
        gene = pattern.replace("(.*\\W", "").replace("\\W.*)", "")
        alteration.append(f'{gene} Structural Variation')

    else:
        qn = trial_match.match_reason.query_node
        criteria = qn.criterion_ancestor[qn.query_level]
        if criteria.get('variant_category', str()).lower() == '!structural variation':
            is_variant = "structural_variant"
            left = criteria.get("hugo_symbol", None)
            right = criteria.get("fusion_partner_hugo_symbol", None)

            alteration.append((f'{"" if left is None else left}'
                               f'{"-" if left is not None and right is not None else ""}'
                               f'{"" if right is None else right}'
                               ' Structural Variation'))

    return {
        'match_type': is_variant,
        'genomic_alteration': ''.join(alteration)
    }


def format_trial_match_k_v(clinical_doc):
    return {key.lower(): val for key, val in clinical_doc.items() if key != "_id"}


def get_sort_order(sort_map: Dict, match_document: Dict) -> list:
    """
    Sort trial matches based on sorting order specified in config.json under the key 'trial_match_sorting'.

    The function will iterate over the objects in the 'trial_match_sorting', and then assess each trial match key
    to determine a final sort string e.g. 001010111000

    The sorting is multi-dimensional and currently organized as follows:
    MMR status > Tier 1 > Tier 2 > CNV > Tier 3 > Tier 4 > wild type
    Variant-level  > gene-level
    Exact cancer match > all solid/liquid
    DFCI > Coordinating centers
    """

    sort_array = list()

    for sort_dimension in sort_map:
        sort_index = 99
        for sort_key in sort_dimension:
            if match_document.setdefault(sort_key, None):
                trial_match_val = str(match_document[sort_key])
                if trial_match_val is not None and trial_match_val in sort_dimension[sort_key]:
                    if sort_dimension[sort_key][trial_match_val] < sort_index:
                        sort_index = sort_dimension[sort_key][trial_match_val]

        sort_array.append(sort_index)
    sort_array.append(int(match_document['protocol_no'].replace("-", "")))

    return sort_array


def get_cancer_type_match(trial_match):
    """Trial curations with _SOLID_ and _LIQUID_ should report those as reasons for match. All others should report
    'specific' """
    cancer_type_match = 'specific'
    for criteria in trial_match.match_criterion.criteria_list:
        for node in criteria.criteria:
            if 'clinical' in node and 'oncotree_primary_diagnosis' in node['clinical']:
                diagnosis = node['clinical']['oncotree_primary_diagnosis']
                if diagnosis == '_LIQUID_':
                    cancer_type_match = 'all_liquid'
                    break
                elif diagnosis == '_SOLID_':
                    cancer_type_match = 'all_solid'
                    break
    return cancer_type_match


class DFCITrialMatchDocumentCreator(TrialMatchDocumentCreator):
    def create_trial_matches(self, trial_match: TrialMatch) -> Dict:
        """
        Create a trial match document to be inserted into the db. Add clinical, genomic, and trial details as specified
        in config.json
        """
        query = trial_match.match_reason.extract_raw_query()

        new_trial_match = dict()
        clinical_doc = self.cache.docs[trial_match.match_reason.clinical_id]
        new_trial_match.update(format_trial_match_k_v(clinical_doc))
        new_trial_match['clinical_id'] = self.cache.docs[trial_match.match_reason.clinical_id]['_id']

        new_trial_match.update(
            {'match_level': trial_match.match_clause_data.match_clause_level,
             'internal_id': trial_match.match_clause_data.internal_id,
             'cancer_type_match': get_cancer_type_match(trial_match),
             'reason_type': trial_match.match_reason.reason_name,
             'q_depth': trial_match.match_reason.depth,
             'q_width': trial_match.match_reason.width,
             'code': trial_match.match_clause_data.code,
             'trial_accrual_level_status': 'closed' if trial_match.match_clause_data.is_suspended else 'open',
             'trial_summary_status': trial_match.match_clause_data.status,
             'coordinating_center': trial_match.match_clause_data.coordinating_center})

        # remove extra fields from trial_match output
        new_trial_match.update({
            k: v
            for k, v in trial_match.trial.items()
            if k not in {'treatment_list', '_summary', 'status', '_id', '_elasticsearch', 'match'}
        })

        if trial_match.match_reason.reason_name == 'genomic':
            genomic_doc = self.cache.docs.setdefault(trial_match.match_reason.genomic_id, None)
            new_trial_match.update({"q_c_width": trial_match.match_reason.clinical_width})
            if genomic_doc is None:
                new_trial_match.update(format_trial_match_k_v(format_exclusion_match(trial_match)))
            else:
                new_trial_match.update(format_trial_match_k_v(get_genomic_details(genomic_doc, query)))
        elif trial_match.match_reason.reason_name == 'clinical':
            new_trial_match.update(format_trial_match_k_v(get_clinical_details(clinical_doc, query)))

        sort_order = get_sort_order(self.config['trial_match_sorting'], new_trial_match)
        new_trial_match['sort_order'] = sort_order
        new_trial_match['query_hash'] = trial_match.match_criterion.hash()
        new_trial_match['hash'] = nested_object_hash(new_trial_match)
        new_trial_match["is_disabled"] = False
        new_trial_match.update(
            {'match_path': '.'.join([str(item) for item in trial_match.match_clause_data.parent_path])})

        new_trial_match['combo_coord'] = nested_object_hash({'query_hash': new_trial_match['query_hash'],
                                                             'match_path': new_trial_match['match_path'],
                                                             'protocol_no': new_trial_match['protocol_no']})
        new_trial_match.pop("_updated", None)
        new_trial_match.pop("last_updated", None)
        return new_trial_match


__export__ = ["DFCITrialMatchDocumentCreator"]
