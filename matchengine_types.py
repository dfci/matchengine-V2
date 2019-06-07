from collections import defaultdict
from dataclasses import dataclass
from typing import NewType, Tuple, Union, List, Dict, Any, Set
from bson import ObjectId
from networkx import DiGraph
from threading import Lock

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
class QueryPart:
    query: Dict
    negate: bool

    def hash(self) -> str:
        return comparable_dict(self.query).hash()


@dataclass
class QueryNode:
    query_level: str
    query_parts: List[QueryPart]
    exclusion: Union[None, bool]

    def hash(self) -> str:
        return comparable_dict({
            "_tmp1": [query_part.hash()
                      for query_part in self.query_parts],
            '_tmp2': self.exclusion
        }).hash()

    def query_parts_to_single_query(self):
        return {
            key: value
            for query_part in self.query_parts
            for key, value in query_part.query.items()
        }


@dataclass
class MultiCollectionQuery:
    genomic: List[QueryNode]
    clinical: List[QueryNode]


@dataclass
class MatchClauseData:
    match_clause: MatchClause
    internal_id: str
    parent_path: ParentPath
    match_clause_level: MatchClauseLevel
    match_clause_additional_attributes: dict
    protocol_no: str


@dataclass
class RawQueryResult:
    query: QueryNode
    clinical_id: ClinicalID
    clinical_doc: MongoQueryResult
    genomic_doc: Union[MongoQueryResult, None]


@dataclass
class TrialMatch:
    trial: Trial
    match_clause_data: MatchClauseData
    match_criterion: MatchCriterion
    multi_collection_query: MultiCollectionQuery
    raw_query_result: RawQueryResult


class Cache:
    docs: Dict[ObjectId, MongoQueryResult]
    matches: dict
    lock: Lock

    def __init__(self):
        self.docs = dict()
        self.ids = dict()
        self.lock = Lock()

    def get_clinical_ids_by_query_hash(self, query_hash: str) -> Set[ObjectId]:
        with self.lock:
            if query_hash not in self.ids:
                self.ids[query_hash] = dict()
            results = set(self.ids[query_hash].keys())
        return results

    def update_ids_by_query_hash(self, query_hash: str, update: Dict[ObjectId, ObjectId]):
        with self.lock:
            if query_hash not in self.ids:
                self.ids[query_hash] = update
            else:
                self.ids[query_hash].update(update)

    def get_id_by_query_hash(self, query_hash: str, object_id: ObjectId):
        with self.lock:
            result = self.ids.setdefault(query_hash, dict()).setdefault(object_id, None)
        return result

    def update_docs(self, update: Dict[ObjectId, MongoQueryResult]):
        with self.lock:
            self.docs.update(update)

    def get_doc_by_object_id(self, object_id):
        with self.lock:
            result = self.docs[object_id]
        return result


@dataclass
class QueryTask:
    match_criteria_transform: object
    trial: Trial
    match_clause_data: MatchClauseData
    match_path: MatchCriterion
    query: MultiCollectionQuery
    clinical_ids: List[ClinicalID]
    cache: Cache


@dataclass
class MatchReason:
    query_node: QueryNode
    clinical_id: ClinicalID
    genomic_id: Union[None, GenomicID]
