from typing import Dict

from match_criteria_transform import MatchCriteriaTransform
from matchengine_types import TrialMatch, Cache, Secrets


class TrialMatchDocumentCreator(object):
    cache: Cache
    config: Dict

    def create_trial_matches(self, trial_match: TrialMatch) -> Dict:
        pass


class QueryTransformerContainer(object):
    _: MatchCriteriaTransform


class DBSecrets(object):
    def get_secrets(self) -> Secrets:
        pass
