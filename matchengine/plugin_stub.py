from typing import Dict, NoReturn

from matchengine.match_criteria_transform import MatchCriteriaTransform
from matchengine.utilities.matchengine_types import TrialMatch, Cache, Secrets, QueryNode


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


class QueryNodeTransformer(object):
    def query_node_transform(self, query_node: QueryNode) -> NoReturn:
        pass
