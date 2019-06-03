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


def format_not_match(query):
    """Format the genomic alteration for genomic documents that matched a negative clause of a match tree"""

    GENE_KEY = 'TRUE_HUGO_SYMBOL'
    PROTEIN_CHANGE_KEY = 'TRUE_PROTEIN_CHANGE'
    CNV_KEY = 'CNV_CALL'
    VARIANT_CLASSIFICATION_KEY = 'TRUE_VARIANT_CLASSIFICATION'
    SV_KEY = 'VARIANT_CATEGORY'

    alteration = '!'

    is_variant = 'variant' if query.setdefault(PROTEIN_CHANGE_KEY, None) is not None else 'gene'

    # for clarity

    # TODO: regex

    if GENE_KEY in query and query[GENE_KEY] is not None:
        alteration = '!{}'.format(query[GENE_KEY])

    # add mutation
    if query.setdefault(PROTEIN_CHANGE_KEY, None) is not None:
        alteration += ' {}'.format(query[PROTEIN_CHANGE_KEY])

    # add cnv call
    elif query.setdefault(CNV_KEY, None) is not None:
        alteration += ' {}'.format(query[CNV_KEY])

    # add variant classification
    elif query.setdefault(VARIANT_CLASSIFICATION_KEY, None) is not None:
        alteration += ' {}'.format(query[VARIANT_CLASSIFICATION_KEY])

    # add structural variation
    elif query.setdefault(SV_KEY, str()) == 'SV':
        alteration += ' Structural Variation'

    return {
        'match_type': is_variant,
        'genomic_alteration': alteration
    }


def format(clinical_doc):
    return {key.lower(): val for key, val in clinical_doc.items() if key != "_id"}
