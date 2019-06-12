import datetime
from dataclasses import dataclass
from typing import NewType, Tuple, Union, List, Dict, Any, Set
from bson import ObjectId
from networkx import DiGraph
from threading import Lock

from frozendict import ComparableDict

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


class PoisonPill(object):
    pass


@dataclass
class QueryPart:
    query: Dict
    negate: bool
    render: bool

    def hash(self) -> str:
        return ComparableDict(self.query).hash()


@dataclass
class QueryNode:
    query_level: str
    query_parts: List[QueryPart]
    exclusion: Union[None, bool]

    def hash(self) -> str:
        return ComparableDict({
            "_tmp1": [query_part.hash()
                      for query_part in self.query_parts],
            '_tmp2': self.exclusion
        }).hash()

    def extract_raw_query(self):
        return {
            key: value
            for query_part in self.query_parts
            for key, value in query_part.query.items()
            if query_part.render
        }


@dataclass
class MultiCollectionQuery:
    genomic: List[QueryNode]
    clinical: List[QueryNode]


@dataclass
class RunLog:
    protocol_nos: list
    clinical_ids: list
    marked_available: list
    inserted: list
    marked_disabled: list
    _created: datetime.datetime


@dataclass
class MatchClauseData:
    match_clause: MatchClause
    internal_id: str
    parent_path: ParentPath
    match_clause_level: MatchClauseLevel
    match_clause_additional_attributes: dict
    protocol_no: str


@dataclass
class GenomicMatchReason:
    query_node: QueryNode
    clinical_id: ClinicalID
    genomic_id: Union[GenomicID, None]


@dataclass
class ClinicalMatchReason:
    query_node: QueryNode
    clinical_id: ClinicalID


MatchReason = NewType("MatchReason", Union[GenomicMatchReason, ClinicalMatchReason])


@dataclass
class TrialMatch:
    trial: Trial
    match_clause_data: MatchClauseData
    match_criterion: MatchCriterion
    multi_collection_query: MultiCollectionQuery
    match_reason: MatchReason


class Cache:
    docs: Dict[ObjectId, MongoQueryResult]
    ids: dict

    def __init__(self):
        self.docs = dict()
        self.ids = dict()


@dataclass
class QueryTask:
    trial: Trial
    match_clause_data: MatchClauseData
    match_path: MatchCriterion
    query: MultiCollectionQuery
    clinical_ids: Set[ClinicalID]


@dataclass
class UpdateTask:
    ops: List
    protocol_no: str
