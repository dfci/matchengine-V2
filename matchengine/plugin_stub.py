from __future__ import annotations
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from matchengine.match_criteria_transform import MatchCriteriaTransform
    from matchengine.utilities.matchengine_types import (
        Secrets,
        QueryNode,
        TrialMatch,
        Cache
    )
    from typing import (
        Dict,
        NoReturn
    )


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
