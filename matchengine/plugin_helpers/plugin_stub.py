from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from matchengine.match_criteria_transform import (
        MatchCriteriaTransform,
        TransformFunctions
    )
    from matchengine.typing.matchengine_types import (
        Secrets,
        QueryNode,
        TrialMatch,
        Cache,
        QueryNodeContainer,
        ClinicalID)
    from typing import (
        Dict,
        NoReturn,
        Set
    )
    from matchengine.engine import MatchEngine


class TrialMatchDocumentCreator(object):
    cache: Cache
    config: Dict

    def create_trial_matches(self: MatchEngine, trial_match: TrialMatch) -> Dict:
        pass


class QueryTransformerContainer(object):
    _: MatchCriteriaTransform
    transform: TransformFunctions
    resources: Dict


class DBSecrets(object):
    def get_secrets(self) -> Secrets:
        pass


class QueryNodeTransformer(object):
    def query_node_transform(self, query_node: QueryNode) -> NoReturn:
        pass


class QueryNodeClinicalIDsSubsetter(object):
    def genomic_query_node_clinical_ids_subsetter(self: MatchEngine,
                                                  query_node: QueryNode,
                                                  clinical_ids: Set[ClinicalID]) -> Set[ClinicalID]:
        pass

    def clinical_query_node_clinical_ids_subsetter(self: MatchEngine,
                                                   query_node: QueryNode,
                                                   clinical_ids: Set[ClinicalID]) -> Set[ClinicalID]:
        pass


class QueryNodeContainerTransformer(object):
    def query_container_transform(self: MatchEngine, query_container: QueryNodeContainer):
        pass
