from typing import Dict

from matchengine_types import TrialMatch, Cache


class TrialMatchDocumentCreator(object):
    cache: Cache
    config: Dict

    def create_trial_matches(self, trial_match: TrialMatch) -> Dict:
        pass
