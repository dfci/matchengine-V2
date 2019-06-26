from typing import Dict

from TrialMatchDocumentCreator import TrialMatchDocumentCreator
from frozendict import ComparableDict
from matchengine_types import TrialMatch


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
        alteration += ' Structural Variation'

    # add mutational signtature
    elif variant_category in genomic_doc \
            and genomic_doc[variant_category] == 'SIGNATURE' \
            and mmr_status in genomic_doc \
            and genomic_doc[mmr_status] is not None:
        alteration += mmr_map_rev[genomic_doc[mmr_status]]

    return {
        'match_type': is_variant,
        'genomic_alteration': alteration,
        'genomic_id': genomic_doc['_id'],
        **genomic_doc
    }


def format_exclusion_match(query):
    """Format the genomic alteration for genomic documents that matched a negative clause of a match tree"""

    gene_key = 'TRUE_HUGO_SYMBOL'
    protein_change_key = 'TRUE_PROTEIN_CHANGE'
    cnv_key = 'CNV_CALL'
    variant_classification_key = 'TRUE_VARIANT_CLASSIFICATION'
    sv_key = 'VARIANT_CATEGORY'
    alteration = '!'
    is_variant = 'variant' if query.setdefault(protein_change_key, None) is not None else 'gene'

    if gene_key in query and query[gene_key] is not None:
        alteration = f'!{query[gene_key]}'

    # add mutation
    if query.setdefault(protein_change_key, None) is not None:
        if '$regex' in query[protein_change_key]:
            alteration += query[protein_change_key]['$regex'].pattern[1:].replace('[A-Z]','')
        else:
            alteration += f' {query[protein_change_key]}'

    # add cnv call
    elif query.setdefault(cnv_key, None) is not None:
        alteration += f' {query[cnv_key]}'

    # add variant classification
    elif query.setdefault(variant_classification_key, None) is not None:
        alteration += f' {query[variant_classification_key]}'

    # add structural variation
    elif query.setdefault(sv_key, str()) == 'SV':
        alteration += ' Structural Variation'

    return {
        'match_type': is_variant,
        'genomic_alteration': alteration
    }


def format_trial_match_k_v(clinical_doc):
    return {key.lower(): val for key, val in clinical_doc.items() if key != "_id"}


def get_sort_order(sort_order_mapping: Dict, match_document: Dict) -> str:
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

    sort_string = ''

    for sort_val in sort_order_mapping:
        to_add = '99'
        for sort_key in sort_val:
            if match_document.setdefault(sort_key, None):
                matched_val = str(match_document[sort_key])
                # how to deal with oncotree primary diagnosis _LIQUID, or _SOLID
                if matched_val is not None and matched_val in sort_val[sort_key]:
                    to_add = sort_val[sort_key][matched_val]

        sort_string += to_add

    return sort_string + match_document['protocol_no'].replace("-", "")


    # sub_level_padding = 2
    # sub_level_slots = 4
    # top_level_slots = 5
    # top_level_sort = str()
    # for top_level_position, top_level_sort_mapping in enumerate(sort_order_mapping):
    #     if top_level_position >= top_level_slots:
    #         break
    #     sub_level_sort = str()
    #     for sub_level_position, sub_level_sort_mapping in enumerate(top_level_sort_mapping):
    #         if sub_level_position >= sub_level_slots:
    #             break
    #         sort_key, sort_values = sub_level_sort_mapping
    #         if match_document.setdefault(sort_key, None) in sort_values:
    #             sub_level_sort += str(sort_values.index(match_document[sort_key])).ljust(sub_level_padding, '0')
    #         else:
    #             sub_level_sort += str((10 ** sub_level_padding) - 1).ljust(sub_level_padding, '0')
    #     top_level_sort += sub_level_sort
    # return top_level_sort + match_document['protocol_no']

    # sort_string = ''
    # for sort_level_keys in self.trial_match_sorting:
    #     for sort_key in sort_level_keys:
    #         for trial_key in new_trial_match:
    #             if trial_key == sort_key:
    #                 new_trial_val = new_trial_match[trial_key]
    #
    #                 # Exact cancer match > all solid/liquid
    #                 if trial_key == "oncotree_primary_diagnosis_name":
    #                     diagnosis_from_query = trial_match.match_criterion[0]['clinical'][
    #                         'oncotree_primary_diagnosis']
    #                     new_trial_val = '__default'
    #                     if diagnosis_from_query in ['_SOLID_', '_LIQUID_']:
    #                         new_trial_val = diagnosis_from_query
    #
    #                 # lastly, sort on protocol_no
    #                 if new_trial_val is not None:
    #                     if trial_key == 'protocol_no':
    #                         sort_string_prepend_val = new_trial_val
    #                     else:
    #                         sort_string_prepend_val = sort_level_keys[sort_key][str(new_trial_val)]
    #
    #                     sort_string = sort_string_prepend_val + sort_string
    #
    # sort_string = ''.join([digit for digit in sort_string if digit.isdigit()])
    # new_trial_match['sort_order'] = sort_string
    # yield new_trial_match


class DFCITrialMatchDocumentCreator(TrialMatchDocumentCreator):
    def create_trial_matches(self, trial_match: TrialMatch) -> Dict:
        """
        Create a trial match document to be inserted into the db. Add clinical, genomic, and trial details as specified
        in config.json
        """
        genomic_doc = self.cache.docs.setdefault(trial_match.match_reason.genomic_id, None)
        query = trial_match.match_reason.query_node.extract_raw_query()

        new_trial_match = dict()
        new_trial_match.update(format_trial_match_k_v(self.cache.docs[trial_match.match_reason.clinical_id]))
        new_trial_match['clinical_id'] = self.cache.docs[trial_match.match_reason.clinical_id]['_id']

        new_trial_match.update(
            {'match_level': trial_match.match_clause_data.match_clause_level,
             'internal_id': trial_match.match_clause_data.internal_id,
             'code': trial_match.match_clause_data.code,
             'trial_accrual_status': trial_match.match_clause_data.status,
             'coordinating_center': trial_match.match_clause_data.coordinating_center})

        # remove extra fields from trial_match output
        new_trial_match.update({
            k: v
            for k, v in trial_match.trial.items()
            if k not in {'treatment_list', '_summary', 'status', '_id', '_elasticsearch', 'match'}
        })

        if genomic_doc is None:
            new_trial_match.update(format_trial_match_k_v(format_exclusion_match(query)))
        else:
            new_trial_match.update(format_trial_match_k_v(get_genomic_details(genomic_doc, query)))

        sort_order = get_sort_order(self.config['trial_match_sorting'], new_trial_match)
        new_trial_match['sort_order'] = sort_order
        new_trial_match['query_hash'] = ComparableDict({'query': trial_match.match_criterion}).hash()
        new_trial_match['hash'] = ComparableDict(new_trial_match).hash()
        new_trial_match["is_disabled"] = False
        new_trial_match.update(
            {'match_path': '.'.join([str(item) for item in trial_match.match_clause_data.parent_path])})
        return new_trial_match


__export__ = ["DFCITrialMatchDocumentCreator"]
