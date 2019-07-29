from typing import Dict

from matchengine.plugin_stub import TrialMatchDocumentCreator
from matchengine.utilities.matchengine_types import TrialMatch


class TestTrialMatchDocumentCreator(TrialMatchDocumentCreator):
    def create_trial_matches(self, trial_match: TrialMatch) -> Dict:
        return {}


__export__ = ["TestTrialMatchDocumentCreator"]
