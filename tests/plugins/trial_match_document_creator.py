from typing import Dict

from TrialMatchDocumentCreator import TrialMatchDocumentCreator
from matchengine_types import TrialMatch


class TestTrialMatchDocumentCreator(TrialMatchDocumentCreator):
    def create_trial_matches(self, trial_match: TrialMatch) -> Dict:
        return {}


__export__ = ["TestTrialMatchDocumentCreator"]
