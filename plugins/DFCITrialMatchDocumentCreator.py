from __future__ import annotations
from typing import TYPE_CHECKING

from matchengine.plugin_stub import TrialMatchDocumentCreator
from matchengine.utilities.frozendict import ComparableDict
if TYPE_CHECKING:
    from matchengine.utilities.matchengine_types import TrialMatch
    from typing import Dict


def get_genomic_details(genomic_doc, query):
    mmr_map_rev = {
        'Proficient (MMR-P / MSS)': 'MMR-P/MSS',
        'Deficient (MMR-D / MSI-H)': 'MMR-D/MSI-H'
    }

    # for clarity
    hugo_symbol = 'TRUE_HUGO_SYMBOL'
    true_protein = 'TRUE_PROTEIN_CHANGE'
    cnv = 'CNV_CALL'
    variant_classification = 'TRUE_VARIANT_CLASSIFICATION'
    variant_category = 'VARIANT_CATEGORY'
    sv_comment = 'STRUCTURAL_VARIANT_COMMENT'
    wildtype = 'WILDTYPE'
    mmr_status = 'MMR_STATUS'

    alteration = ''
    is_variant = 'gene'

    # determine if match was gene or variant-level
    if true_protein in query and query[true_protein] is not None:
        is_variant = 'variant'

    # add wildtype calls
    if wildtype in genomic_doc and genomic_doc[wildtype] is True:
        alteration += 'wt '

    # add gene
    if hugo_symbol in genomic_doc and genomic_doc[hugo_symbol] is not None:
        alteration += genomic_doc[hugo_symbol]

    # add mutation
    if true_protein in genomic_doc and genomic_doc[true_protein] is not None:
        alteration += ' %s' % genomic_doc[true_protein]

    # add cnv call
    elif cnv in genomic_doc and genomic_doc[cnv] is not None:
        alteration += ' %s' % genomic_doc[cnv]

    # add variant classification
    elif variant_classification in genomic_doc and genomic_doc[variant_classification] is not None:
        alteration += ' %s' % genomic_doc[variant_classification]

    # add structural variation
    elif variant_category in genomic_doc and genomic_doc[variant_category] == 'SV':
        pattern = query[sv_comment].pattern.split("|")[0]
        gene = pattern.replace("(.*\\W", "").replace("\\W.*)", "")
        alteration += f'{gene} Structural Variation'

    # add mutational signtature
    elif variant_category in genomic_doc \
            and genomic_doc[variant_category] == 'SIGNATURE' \
            and genomic_doc[mmr_status] is not None \
            and genomic_doc[mmr_status] in mmr_map_rev \
            and mmr_status in genomic_doc:
        alteration += mmr_map_rev[genomic_doc[mmr_status]]

    return {
        'match_type': is_variant,
        'genomic_alteration': alteration,
        'genomic_id': genomic_doc['_id'],
        **genomic_doc
    }


def format_exclusion_match(query):
    """Format the genomic alteration for genomic documents that matched a negative clause of a match tree"""

    true_hugo = 'TRUE_HUGO_SYMBOL'
    protein_change_key = 'TRUE_PROTEIN_CHANGE'
    cnv_call = 'CNV_CALL'
    variant_classification = 'TRUE_VARIANT_CLASSIFICATION'
    sv_comment = 'STRUCTURAL_VARIANT_COMMENT'
    alteration = '!'
    is_variant = 'variant' if query.setdefault(protein_change_key, None) is not None else 'gene'

    if true_hugo in query and query[true_hugo] is not None:
        alteration = f'!{query[true_hugo]}'

    # add mutation
    if query.setdefault(protein_change_key, None) is not None:
        if '$regex' in query[protein_change_key]:
            alteration += f' {query[protein_change_key]["$regex"].pattern[1:].split("[")[0]}'
        else:
            alteration += f' {query[protein_change_key]}'

    # add cnv call
    elif query.setdefault(cnv_call, None) is not None:
        alteration += f' {query[cnv_call]}'

    # add variant classification
    elif query.setdefault(variant_classification, None) is not None:
        alteration += f' {query[variant_classification]}'

    # add structural variation
    elif query.setdefault(sv_comment, None) is not None:
        pattern = query[sv_comment].pattern.split("|")[0]
        gene = pattern.replace("(.*\\W", "").replace("\\W.*)", "")
        alteration += f'{gene} Structural Variation'

    return {
        'match_type': is_variant,
        'genomic_alteration': alteration
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
        query = trial_match.match_reason.query_node.extract_raw_query()

        new_trial_match = dict()
        new_trial_match.update(format_trial_match_k_v(self.cache.docs[trial_match.match_reason.clinical_id]))
        new_trial_match['clinical_id'] = self.cache.docs[trial_match.match_reason.clinical_id]['_id']

        new_trial_match.update(
            {'match_level': trial_match.match_clause_data.match_clause_level,
             'internal_id': trial_match.match_clause_data.internal_id,
             'cancer_type_match': get_cancer_type_match(trial_match),
             'reason_type': trial_match.match_reason.reason_name,
             'q_depth': trial_match.match_reason.query_node.query_depth,
             'q_width': trial_match.match_reason.width if trial_match.match_reason.reason_name == 'genomic' else 1,
             'code': trial_match.match_clause_data.code,
             'trial_accrual_status': 'closed' if trial_match.match_clause_data.is_suspended else 'open',
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
            if genomic_doc is None:
                new_trial_match.update(format_trial_match_k_v(format_exclusion_match(query)))
            else:
                new_trial_match.update(format_trial_match_k_v(get_genomic_details(genomic_doc, query)))

        sort_order = get_sort_order(self.config['trial_match_sorting'], new_trial_match)
        new_trial_match['sort_order'] = sort_order
        new_trial_match['query_hash'] = trial_match.match_criterion.hash()
        new_trial_match['hash'] = ComparableDict(new_trial_match).hash()
        new_trial_match["is_disabled"] = False
        new_trial_match.update(
            {'match_path': '.'.join([str(item) for item in trial_match.match_clause_data.parent_path])})
        new_trial_match['combo_coord'] = ComparableDict({'query_hash': new_trial_match['query_hash'],
                                                         'match_path': new_trial_match['match_path'],
                                                         'protocol_no': new_trial_match['protocol_no']}).hash()
        new_trial_match.pop("_updated", None)
        new_trial_match.pop("last_updated", None)
        return new_trial_match


__export__ = ["DFCITrialMatchDocumentCreator"]
