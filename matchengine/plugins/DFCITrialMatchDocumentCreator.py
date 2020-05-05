from __future__ import annotations

import operator
from itertools import chain
from typing import TYPE_CHECKING, List

from matchengine.internals.plugin_helpers.plugin_stub import TrialMatchDocumentCreator

if TYPE_CHECKING:
    from matchengine.internals.typing.matchengine_types import TrialMatch, MatchReason, ClinicalID
    from matchengine.internals.engine import MatchEngine
    from typing import Dict


def get_genomic_details(genomic_doc: Dict, trial_match: TrialMatch):
    alteration = list()

    wildtype = genomic_doc.get('WILDTYPE', None)
    true_protein = genomic_doc.get('TRUE_PROTEIN_CHANGE', None)
    hugo_symbol = genomic_doc.get('TRUE_HUGO_SYMBOL', None)
    cnv = genomic_doc.get('CNV_CALL', None)
    variant_classification = genomic_doc.get("TRUE_VARIANT_CLASSIFICATION", None)
    variant_category = genomic_doc.get('VARIANT_CATEGORY', None)
    query_node = trial_match.match_reason.query_node
    criteria_ancestor = query_node.criterion_ancestor[query_node.query_level]
    is_variant = 'gene'

    # add wildtype calls
    if wildtype:
        alteration.append('wt ')

    # add gene
    if hugo_symbol is not None:
        alteration.append(hugo_symbol)

    # add mutation
    if true_protein is not None:
        alteration.append(f' {true_protein}')
        is_variant = ('variant'
                      if {'protein_change', 'wildcard_protein_change'}.intersection(
            set(criteria_ancestor.keys()))
                      else 'gene')

    # add cnv call
    elif cnv:
        alteration.append(f' {cnv}')

    # add variant classification
    elif variant_classification:
        alteration.append(f' {variant_classification}')

    # add structural variation
    elif variant_category == 'SV':
        genomic_left = genomic_doc.get("LEFT_PARTNER_GENE", False)
        genomic_right = genomic_doc.get("RIGHT_PARTNER_GENE", False)
        if (genomic_left is not False) or (genomic_right is not False):
            criteria_left = (criteria_ancestor.get("hugo_symbol", str())
                             .lower()
                             .replace(" ", "_"))
            criteria_right = (criteria_ancestor
                              .get("fusion_partner_hugo_symbol", str())
                              .lower()
                              .replace(" ", "_"))
            is_variant = ('variant'
                          if criteria_left not in {'', 'any_gene'}
                             and criteria_right not in {'', 'any_gene'}
                          else 'gene')
            if genomic_left and genomic_right:
                alteration.append(f'{genomic_left}-{genomic_right}')
            else:
                alteration.append(f'{genomic_left or genomic_right}-intergenic')
            structural_variant_type = genomic_doc.get('STRUCTURAL_VARIANT_TYPE', None)
            alteration.append(' ')
            alteration.append(
                'Structural Variant' if structural_variant_type is None else structural_variant_type)
        else:
            query = trial_match.match_reason.query_node.extract_raw_query()
            sv_comment = query.get('STRUCTURAL_VARIANT_COMMENT', None)
            pattern = sv_comment.pattern.split("|")[0] if sv_comment is not None else None
            gene = pattern.replace("(.*\\W", "").replace("\\W.*)",
                                                         "") if pattern is not None else None
            alteration.append(f'{gene} Structural Variation' if gene else 'Structural Variation')

    # add mutational signature
    elif variant_category == 'SIGNATURE':
        query = query_node.extract_raw_query()
        signature_type = next(chain({
                                        'UVA_STATUS',
                                        'TABACCO_STATUS',
                                        'POLE_STATUS',
                                        'TEMOZOLOMIDE_STATUS',
                                        'MMR_STATUS',
                                        'APOBEC_STATUS'
                                    }.intersection(query.keys())))
        signature_value = genomic_doc.get(signature_type, None)
        is_variant = "signature"
        if signature_type == 'MMR_STATUS':
            is_variant = 'mmr'
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

    c_tmb, q_tmb = map(lambda x: x.get("TUMOR_MUTATIONAL_BURDEN_PER_MEGABASE", None),
                       (clinical_doc, query))
    if all((q_tmb, c_tmb)):
        alteration.append(f"TMB = {c_tmb}")
        match_type = "tmb"
        clinical_doc.update({'variant_category': 'TMB'})
    else:
        match_type = "generic_clinical"

    return {
        'match_type': match_type,
        'genomic_alteration': ''.join(alteration),
        **clinical_doc
    }


def format_exclusion_match(trial_match: TrialMatch):
    """Format the extended_attributes alteration for extended_attributes documents that matched a negative clause of a match tree"""
    query = trial_match.match_reason.query_node.extract_raw_query()

    true_hugo = 'TRUE_HUGO_SYMBOL'
    protein_change_key = 'TRUE_PROTEIN_CHANGE'
    cnv_call = 'CNV_CALL'
    variant_classification = 'TRUE_VARIANT_CLASSIFICATION'
    sv_comment = 'STRUCTURAL_VARIANT_COMMENT'
    alteration = ['!']
    is_variant = 'variant' if query.setdefault(protein_change_key, None) is not None else 'gene'

    true_hugo_symbol_added = False
    if true_hugo in query and query[true_hugo] is not None:
        alteration.append(f'{query[true_hugo]}')
        true_hugo_symbol_added = True

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
            left = criteria.get("hugo_symbol", '')
            right = criteria.get("fusion_partner_hugo_symbol", '')
            is_variant = ('variant'
                          if left not in {'', 'any_gene'}
                             and right not in {'', 'any_gene'}
                          else 'gene')

            alteration.append((f'{left}'
                               f'{"-" if left and right else ""}'
                               f'{right}'
                               ' Structural Variation'))

    if len(alteration) == 2 and true_hugo_symbol_added:
        alteration.append(' Mutation')
    return {
        'match_type': is_variant,
        'genomic_alteration': ''.join(alteration)
    }


def format_trial_match_k_v(clinical_doc):
    return {key.lower(): val for key, val in clinical_doc.items() if key != "_id"}


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
    def results_transformer(self: MatchEngine, results: Dict[ClinicalID, List[MatchReason]]):
        for clinical_id, reasons in results.items():
            if not all(map(operator.attrgetter("show_in_ui"), reasons)):
                for reason in reasons:
                    reason.show_in_ui = False

    def create_trial_matches(self, trial_match: TrialMatch, new_trial_match: Dict) -> Dict:
        """
        Create a trial match document to be inserted into the db. Add clinical, extended_attributes, and trial details as specified
        in config.json
        """
        query = trial_match.match_reason.extract_raw_query()
        clinical_doc = self.cache.docs[trial_match.match_reason.clinical_id]
        new_trial_match.update({'cancer_type_match': get_cancer_type_match(trial_match)})

        if trial_match.match_reason.reason_name == 'genomic':
            genomic_doc = self.cache.docs.setdefault(trial_match.match_reason.reference_id, None)
            if genomic_doc is None:
                new_trial_match.update(format_trial_match_k_v(format_exclusion_match(trial_match)))
            else:
                new_trial_match.update(
                    format_trial_match_k_v(get_genomic_details(genomic_doc, trial_match)))
        elif trial_match.match_reason.reason_name == 'prior_treatments':
            prior_treatments_doc = self.cache.docs[trial_match.match_reason.genomic_id]
            new_trial_match.update({"prior_treatment_id": trial_match.match_reason.genomic_id})
            new_trial_match.update(
                {k: v for k, v in prior_treatments_doc.items() if
                 not k.startswith('_')})
        elif trial_match.match_reason.reason_name == 'clinical':
            new_trial_match.update(
                format_trial_match_k_v(get_clinical_details(clinical_doc, query)))

        new_trial_match.pop("_updated", None)
        new_trial_match.pop("last_updated", None)
        return new_trial_match


__export__ = ["DFCITrialMatchDocumentCreator"]
