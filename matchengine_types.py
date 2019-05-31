from dataclasses import dataclass
from typing import NewType, Tuple, Union, List, Dict, Any, Set
from bson import ObjectId
from networkx import DiGraph

from frozendict import comparable_dict

Trial = NewType("Trial", dict)
ParentPath = NewType("ParentPath", Tuple[Union[str, int]])
MatchClause = NewType("MatchClause", List[Dict[str, Any]])
MatchTree = NewType("MatchTree", DiGraph)
MatchCriterion = NewType("MatchPath", List[List[Dict[str, Any]]])
MultiCollectionQuery = NewType("MultiCollectionQuery", dict)
NodeID = NewType("NodeID", int)
MatchClauseLevel = NewType("MatchClauseLevel", str)
MongoQueryResult = NewType("MongoQueryResult", Dict[str, Any])
MongoQuery = NewType("MongoQuery", Dict[str, Any])
GenomicID = NewType("GenomicID", ObjectId)
ClinicalID = NewType("ClinicalID", ObjectId)
Collection = NewType("Collection", str)


@dataclass
class MatchClauseData:
    match_clause: MatchClause
    internal_id: str
    parent_path: ParentPath
    match_clause_level: MatchClauseLevel
    match_clause_additional_attributes: dict


@dataclass
class RawQueryResult:
    query: MultiCollectionQuery
    clinical_id: ClinicalID
    clinical_doc: MongoQueryResult
    genomic_docs: List[MongoQueryResult]


@dataclass
class TrialMatch:
    trial: Trial
    match_clause_data: MatchClauseData
    match_criterion: MatchCriterion
    multi_collection_queries: List[MultiCollectionQuery]
    raw_query_result: RawQueryResult


@dataclass
class Cache:
    genomic_hits: int
    clinical_hits: int
    genomic_non_hits: int
    clinical_non_hits: int
    docs: Dict[ObjectId, MongoQueryResult]
    queries: Dict[str, Dict[ObjectId, ObjectId]]
    matches: dict


@dataclass
class QueryTask:
    match_criteria_transform: object
    trial: Trial
    match_clause_data: MatchClauseData
    match_path: MatchCriterion
    queries: List[MultiCollectionQuery]
    clinical_ids: List[ClinicalID]
    cache: Cache


@dataclass
class MatchTask:
    query_task: QueryTask
    raw_result: RawQueryResult
