from matchengine import ParentPath, Trial, TrialMatch

from matchengine_types import MongoQuery


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

    # TODO: regex

    if gene_key in query and query[gene_key] is not None:
        alteration = '!{}'.format(query[gene_key])

    # add mutation
    if query.setdefault(protein_change_key, None) is not None:
        alteration += ' {}'.format(query[protein_change_key])

    # add cnv call
    elif query.setdefault(cnv_key, None) is not None:
        alteration += ' {}'.format(query[cnv_key])

    # add variant classification
    elif query.setdefault(variant_classification_key, None) is not None:
        alteration += ' {}'.format(query[variant_classification_key])

    # add structural variation
    elif query.setdefault(sv_key, str()) == 'SV':
        alteration += ' Structural Variation'

    return {
        'match_type': is_variant,
        'genomic_alteration': alteration
    }


def format(clinical_doc):
    return {key.lower(): val for key, val in clinical_doc.items() if key != "_id"}
