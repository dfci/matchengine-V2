from typing import Dict

from matchengine.plugin_helpers.plugin_stub import TrialMatchDocumentCreator
from matchengine.typing.matchengine_types import TrialMatch


class TestTrialMatchDocumentCreator(TrialMatchDocumentCreator):
    def create_trial_matches(self, trial_match: TrialMatch) -> Dict:
        return {}


__export__ = ["TestTrialMatchDocumentCreator"]
