from __future__ import annotations

import copy
import datetime
from itertools import chain
from typing import (
    NewType,
    Tuple,
    Union,
    List,
    Dict,
    Any,
    Set
)

from bson import ObjectId
from networkx import DiGraph

from matchengine.internals.utilities.object_comparison import nested_object_hash

Trial = NewType("Trial", dict)
ParentPath = NewType("ParentPath", Tuple[Union[str, int]])
MatchClause = NewType("MatchClause", List[Dict[str, Any]])
MatchTree = NewType("MatchTree", DiGraph)
NodeID = NewType("NodeID", int)
MatchClauseLevel = NewType("MatchClauseLevel", str)
MongoQueryResult = NewType("MongoQueryResult", Dict[str, Any])
MongoQuery = NewType("MongoQuery", Dict[str, Any])
GenomicID = NewType("GenomicID", ObjectId)
ClinicalID = NewType("ClinicalID", ObjectId)
Collection = NewType("Collection", str)


class PoisonPill(object):
    __slots__ = ()


class CheckIndicesTask(object):
    __slots__ = ()


class IndexUpdateTask(object):
    __slots__ = (
        "collection", "index"
    )

    def __init__(
            self,
            collection: str,
            index: str
    ):
        self.index = index
        self.collection = collection


class QueryTask(object):
    __slots__ = (
        "trial", "match_clause_data", "match_path",
        "query", "clinical_ids"
    )

    def __init__(
            self,
            trial: Trial,
            match_clause_data: MatchClauseData,
            match_path: MatchCriterion,
            query: MultiCollectionQuery,
            clinical_ids: Set[ClinicalID]
    ):
        self.clinical_ids = clinical_ids
        self.query = query
        self.match_path = match_path
        self.match_clause_data = match_clause_data
        self.trial = trial


class UpdateTask(object):
    __slots__ = (
        "ops", "protocol_no"
    )

    def __init__(
            self,
            ops: List,
            protocol_no: str
    ):
        self.ops = ops
        self.protocol_no = protocol_no


class RunLogUpdateTask(object):
    __slots__ = (
        "protocol_no"
    )

    def __init__(
            self,
            protocol_no: str
    ):
        self.protocol_no = protocol_no


Task = NewType("Task", Union[PoisonPill, CheckIndicesTask, IndexUpdateTask, QueryTask, UpdateTask, RunLogUpdateTask])


class MatchCriteria(object):
    __slots__ = (
        "criteria", "depth", "node_id"
    )

    def __init__(
            self,
            criteria: Dict,
            depth: int,
            node_id: int
    ):
        self.criteria = criteria
        self.depth = depth
        self.node_id = node_id


class MatchCriterion(object):
    __slots__ = (
        "criteria_list", "_hash"
    )

    def __init__(
            self,
            criteria_list: List[MatchCriteria]
    ):
        self.criteria_list = criteria_list
        self._hash = None

    def add_criteria(self, criteria: MatchCriteria):
        self._hash = None
        self.criteria_list.append(criteria)

    def hash(self) -> str:
        if self._hash is None:
            self._hash = nested_object_hash({"query": [criteria.criteria for criteria in self.criteria_list]})
        return self._hash


class QueryPart(object):
    __slots__ = (
        "mcq_invalidating", "render", "negate",
        "_query", "_hash"
    )

    def __init__(
            self,
            query: Dict,
            negate: bool,
            render: bool,
            mcq_invalidating: bool,
            _hash: str = None
    ):
        self.mcq_invalidating = mcq_invalidating
        self.render = render
        self.negate = negate
        self._query = query
        self._hash = _hash

    def hash(self) -> str:
        if self._hash is None:
            self._hash = nested_object_hash(self.query)
        return self._hash

    def set_query_attr(
            self,
            key,
            value
    ):
        self._query[key] = value

    def __copy__(self):
        return QueryPart(
            self.query,
            self.negate,
            self.render,
            self.mcq_invalidating,
            self._hash
        )

    @property
    def query(self):
        return self._query


class QueryNode(object):
    __slots__ = (
        "query_level", "query_depth", "query_parts",
        "exclusion", "is_finalized", "_hash",
        "_raw_query", "_raw_query_hash", "sibling_nodes",
        "node_id", "criterion_ancestor"
    )

    def __init__(
            self,
            query_level: str,
            node_id: int,
            criterion_ancestor: MatchCriteria,
            query_depth: int,
            query_parts: List[QueryPart],
            exclusion: Union[None, bool] = None,
            is_finalized: bool = False,
            _hash: str = None,
            _raw_query: Dict = None,
            _raw_query_hash: str = None
    ):

        self.node_id = node_id
        self.criterion_ancestor = criterion_ancestor
        self.is_finalized = is_finalized
        self.query_level = query_level
        self.query_depth = query_depth
        self.query_parts = query_parts
        self.exclusion = exclusion
        self._hash = _hash
        self._raw_query = _raw_query
        self._raw_query_hash = _raw_query_hash
        self.sibling_nodes = None

    def hash(self) -> str:
        if self._hash is None:
            self._hash = nested_object_hash({
                "_tmp1": [query_part.hash()
                          for query_part in self.query_parts],
                '_tmp2': self.exclusion
            })
        return self._hash

    def add_query_part(self, query_part: QueryPart):
        self._hash = None
        self._raw_query = None
        self._raw_query_hash = None
        self.query_parts.append(query_part)

    def _extract_raw_query(self):
        return {
            key: value
            for query_part in self.query_parts
            for key, value in query_part.query.items()
            if query_part.render
        }

    def extract_raw_query(self):
        if self.is_finalized:
            if self._raw_query is None:
                self._raw_query = self._extract_raw_query()
            return self._raw_query
        else:
            return self._extract_raw_query()

    def raw_query_hash(self):
        if self._raw_query_hash is None:
            if not self.is_finalized:
                raise Exception("Query node is not finalized")
            else:
                self._raw_query_hash = nested_object_hash(self.extract_raw_query())
        return self._raw_query_hash

    def finalize(self):
        self.is_finalized = True

    def get_query_part_by_key(self, key: str) -> QueryPart:
        return next(chain((query_part
                           for query_part in self.query_parts
                           if key in query_part.query),
                          iter([None])))

    def get_query_part_value_by_key(self, key: str, default: Any = None) -> Any:
        query_part = self.get_query_part_by_key(key)
        if query_part is not None:
            return query_part.query.get(key, default)

    @property
    def mcq_invalidating(self):
        return True if any([query_part.mcq_invalidating for query_part in self.query_parts]) else False

    def __copy__(self):
        return QueryNode(
            self.query_level,
            self.node_id,
            self.criterion_ancestor,
            self.query_depth,
            [query_part.__copy__()
             for query_part
             in self.query_parts],
            self.exclusion,
            self.is_finalized,
            self._hash,
            self._raw_query,
            self._raw_query_hash
        )


class QueryNodeContainer(object):
    __slots__ = (
        "query_nodes"
    )

    def __init__(
            self,
            query_nodes: List[QueryNode]
    ):
        self.query_nodes = query_nodes

    def __copy__(self):
        return QueryNodeContainer(
            [query_node.__copy__()
             for query_node
             in self.query_nodes]
        )


class MultiCollectionQuery(object):
    __slots__ = (
        "extended_attributes", "clinical"
    )

    def __init__(
            self,
            extended_attributes: List[QueryNodeContainer],
            clinical: List[QueryNodeContainer]
    ):
        self.extended_attributes = extended_attributes
        self.clinical = clinical

    def __copy__(self):
        return MultiCollectionQuery(
            [query_node_container.__copy__()
             for query_node_container
             in self.extended_attributes],
            [query_node_container.__copy__()
             for query_node_container
             in self.clinical],
        )


class MatchClauseData(object):
    __slots__ = (
        "match_clause", "internal_id", "code",
        "coordinating_center", "is_suspended", "status",
        "parent_path", "match_clause_level", "match_clause_additional_attributes",
        "protocol_no"
    )

    def __init__(self,
                 match_clause: MatchClause,
                 internal_id: str,
                 code: str,
                 coordinating_center: str,
                 is_suspended: bool,
                 status: str,
                 parent_path: ParentPath,
                 match_clause_level: MatchClauseLevel,
                 match_clause_additional_attributes: dict,
                 protocol_no: str):
        self.code = code
        self.coordinating_center = coordinating_center
        self.is_suspended = is_suspended
        self.status = status
        self.parent_path = parent_path
        self.match_clause_level = match_clause_level
        self.internal_id = internal_id
        self.match_clause_additional_attributes = match_clause_additional_attributes
        self.protocol_no = protocol_no
        self.match_clause = match_clause


class ExtendedMatchReason(object):
    __slots__ = (
        "query_node", "width", "clinical_id",
        "reference_id", "clinical_width", "depth",
        "show_in_ui", "reason_name"
    )

    def __init__(
            self,
            query_node: QueryNode,
            width: int,
            clinical_width: int,
            clinical_id: ClinicalID,
            reference_id: Union[GenomicID, None],
            show_in_ui: bool,
    ):
        self.show_in_ui = show_in_ui
        self.clinical_width = clinical_width
        self.reference_id = reference_id
        self.clinical_id = clinical_id
        self.width = width
        self.query_node = query_node
        self.depth = query_node.query_depth
        self.reason_name = query_node.query_level

    def extract_raw_query(self):
        return self.query_node.extract_raw_query()


class ClinicalMatchReason(object):
    __slots__ = (
        "query_part", "clinical_id", "depth",
        "show_in_ui"
    )
    reason_name = "clinical"
    width = 1

    def __init__(
            self,
            query_part: QueryPart,
            clinical_id: ClinicalID,
            depth: int,
            show_in_ui: bool
    ):
        self.show_in_ui = show_in_ui
        self.clinical_id = clinical_id
        self.query_part = query_part
        self.depth = depth

    def extract_raw_query(self):
        return self.query_part.query


MatchReason = NewType("MatchReason", Union[ExtendedMatchReason, ClinicalMatchReason])


class TrialMatch(object):
    __slots__ = (
        "trial", "match_clause_data", "match_criterion",
        "match_clause_data", "multi_collection_query", "match_reason",
        "run_log"
    )

    def __init__(
            self,
            trial: Trial,
            match_clause_data: MatchClauseData,
            match_criterion: MatchCriterion,
            multi_collection_query: MultiCollectionQuery,
            match_reason: MatchReason,
            run_log: datetime.datetime,
    ):
        self.run_log = run_log
        self.match_reason = match_reason
        self.multi_collection_query = multi_collection_query
        self.match_criterion = match_criterion
        self.match_clause_data = match_clause_data
        self.trial = trial


class Cache(object):
    __slots__ = (
        "docs", "ids", "in_process"
    )
    docs: Dict
    ids: Dict
    in_process: Dict

    def __init__(self):
        self.docs = dict()
        self.ids = dict()
        self.in_process = dict()


class Secrets(object):
    __slots__ = (
        "HOST", "PORT", "DB",
        "AUTH_DB", "RO_USERNAME", "RO_PASSWORD",
        "RW_USERNAME", "RW_PASSWORD", "REPLICA_SET",
        "MAX_POOL_SIZE", "MIN_POOL_SIZE"
    )

    def __init__(
            self,
            host: str,
            port: int,
            db: str,
            auth_db: str,
            ro_username: str,
            ro_password: str,
            rw_username: str,
            rw_password: str,
            replica_set: str,
            max_pool_size: str,
            min_pool_size: str
    ):
        self.MIN_POOL_SIZE = min_pool_size
        self.MAX_POOL_SIZE = max_pool_size
        self.REPLICA_SET = replica_set
        self.RW_PASSWORD = rw_password
        self.RW_USERNAME = rw_username
        self.RO_PASSWORD = ro_password
        self.RO_USERNAME = ro_username
        self.AUTH_DB = auth_db
        self.DB = db
        self.PORT = port
        self.HOST = host


class QueryTransformerResult(object):
    __slots__ = (
        "results"
    )
    results: List[QueryPart]

    def __init__(
            self,
            query_clause: Dict = None,
            negate: bool = None,
            render: bool = True,
            mcq_invalidating: bool = False
    ):
        self.results = list()
        if query_clause is not None:
            if negate is not None:
                self.results.append(QueryPart(query_clause, negate, render, mcq_invalidating))
            else:
                raise Exception("If adding query result directly to results container, "
                                "both Negate and Query must be specified")

    def add_result(
            self,
            query_clause: Dict,
            negate: bool,
            render: bool = True,
            mcq_invalidating: bool = False
    ):
        self.results.append(QueryPart(query_clause, negate, render, mcq_invalidating))
