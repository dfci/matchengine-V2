class Sort(object):
    def __init__(self, config):
        self.trial_match_sorting = config['trial_match_sorting']

    def sort(self, new_trial_match, trial_match):
        """
        Sort trial matches based on sorting order specified in config.json

        MMR > Tier 1 > Tier 2 > CNV > Tier 3 > Tier 4 > wild type
        Current thinking:
        Structured SVs are tiered, so include those in the existing Tier order
        TMB and signatures probably come between MMR & Tier 1
        Split out MMR-D vs MMR-P??
        Variant-level  > gene-level
        Exact cancer match > all solid/liquid
        Co-ordinating center: DFCI > others

        :param new_trial_match:
        :param trial_match:
        :return:
        """
        sort_string = ''
        for sort_level_keys in self.trial_match_sorting:
            for sort_key in sort_level_keys:
                for trial_key in new_trial_match:
                    if trial_key == sort_key:
                        new_trial_val = new_trial_match[trial_key]

                        # Exact cancer match > all solid/liquid
                        if trial_key == "oncotree_primary_diagnosis_name":
                            diagnosis_from_query = trial_match.match_criterion[0]['clinical'][
                                'oncotree_primary_diagnosis']
                            new_trial_val = '__default'
                            if diagnosis_from_query in ['_SOLID_', '_LIQUID_']:
                                new_trial_val = diagnosis_from_query

                        # lastly, sort on protocol_no
                        if new_trial_val is not None:
                            if trial_key == 'protocol_no':
                                to_append = new_trial_val
                            else:
                                to_append = sort_level_keys[sort_key][str(new_trial_val)]

                            sort_string = sort_string + to_append

        sort_string = ''.join([digit for digit in sort_string if digit.isdigit()])
        new_trial_match['sort_order'] = sort_string
        yield new_trial_match
