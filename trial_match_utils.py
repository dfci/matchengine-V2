from matchengine import ParentPath, Trial, TrialMatch


def get_genomic_details(genomic_doc, query):
    if genomic_doc is None:
        return {}

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
        **genomic_doc
    }


def format_details(clinical_doc):
    return {key.lower(): val for key, val in clinical_doc.items() if key != "_id"}


def get_trial_details(parent_path: ParentPath, trial: Trial) -> TrialMatch:
    """
    Extract relevant details from a trial curation to include in the trial_match document
    :param parent_path:
    :param trial:
    :return:
    """
    treatment_list = parent_path[0] if 'treatment_list' in parent_path else None
    step = parent_path[1] if 'step' in parent_path else None
    step_no = parent_path[2] if 'step' in parent_path else None
    arm = parent_path[3] if 'arm' in parent_path else None
    arm_no = parent_path[4] if 'arm' in parent_path else None
    dose = parent_path[5] if 'dose' in parent_path else None

    trial_match = dict()
    trial_match['protocol_no'] = trial['protocol_no']
    trial_match['coordinating_center'] = trial['_summary']['coordinating_center']
    trial_match['nct_id'] = trial['nct_id']

    if 'step' in parent_path and 'arm' in parent_path and 'dose' in parent_path:
        trial_match['code'] = trial[treatment_list][step][step_no][dose]['level_code']
        trial_match['internal_id'] = trial[treatment_list][step][step_no][dose]['level_internal_id']
    elif 'step' in parent_path and 'arm' in parent_path:
        trial_match['code'] = trial[treatment_list][step][step_no][arm][arm_no]['arm_code']
        trial_match['internal_id'] = trial[treatment_list][step][step_no][arm][arm_no]['arm_internal_id']
    elif 'step' in parent_path:
        trial_match['code'] = trial[treatment_list][step][step_no]['step_code']
        trial_match['internal_id'] = trial[treatment_list][step][step_no][dose]['step_internal_id']

    return TrialMatch(trial_match)
