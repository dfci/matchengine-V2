from __future__ import annotations

import logging
from collections import deque
from itertools import cycle, chain
from typing import TYPE_CHECKING

import networkx as nx

from matchengine.internals.typing.matchengine_types import (
    MatchClauseData,
    ParentPath,
    MatchClauseLevel,
    MatchTree,
    NodeID,
    MatchCriteria,
    MatchCriterion,
    MultiCollectionQuery,
    QueryNode,
    QueryTransformerResult,
    QueryNodeContainer
)

if TYPE_CHECKING:
    from typing import (
        Generator,
        Dict,
        Any,
        Tuple
    )
    from matchengine.internals.engine import MatchEngine

log = logging.getLogger("matchengine")


def extract_match_clauses_from_trial(matchengine: MatchEngine, protocol_no: str) -> Generator[MatchClauseData]:
    """
    Pull out all of the matches from a trial curation.
    Return the parent path and the values of that match clause.

    Default to only extracting match clauses on steps, arms or dose levels which are open to accrual unless
    otherwise specified.
    """

    trial = matchengine.trials[protocol_no]
    trial_status = trial.get('_summary', dict()).get('status', [dict()])
    site_status = trial_status[0].get('value', 'open to accrual').lower()
    status_for_match_clause = 'open' if site_status.lower() == 'open to accrual' else 'closed'
    coordinating_center = trial.get('_summary', dict()).get('coordinating_center', 'unknown')
    process_q = deque()
    for key, val in trial.items():

        # include top level match clauses
        if key == 'match':
            parent_path = ParentPath(tuple())
            match_clause_data = MatchClauseData(val,
                                                None,
                                                None,
                                                None,
                                                None,
                                                None,
                                                parent_path,
                                                None,
                                                None,
                                                trial[matchengine.match_criteria_transform.trial_identifier])
            yield match_clause_data
        else:
            process_q.append((tuple(), key, val))

    # process nested dicts to find more match clauses
    while process_q:
        path, parent_key, parent_value = process_q.pop()
        if parent_value.__class__ is dict:
            for inner_key, inner_value in parent_value.items():
                parent_path = ParentPath(path + (parent_key, inner_key))
                # this funky logic is so that level/internal is None if node is not a match clause
                level = MatchClauseLevel(
                    matchengine.match_criteria_transform.level_mapping.get(
                        next(
                            chain(
                                [item for item in parent_path[::-1] if item.__class__ is not int and item != 'match'],
                                [None]
                            )
                        )))

                internal_id = parent_value.get(
                    matchengine.match_criteria_transform.internal_id_mapping.get(level, None), None)
                code = parent_value.get(matchengine.match_criteria_transform.code_mapping.get(level, None), None)
                if inner_key == 'match':
                    is_suspended = False
                    match_level = path[-1]
                    if match_level == 'step':
                        if all([arm.get('arm_suspended', 'n').lower().strip() == 'y'
                                for arm in parent_value.get('arm', list())]):
                            if not matchengine.match_on_closed:
                                log.info(f"{protocol_no} {match_level} {internal_id} has no open arms, skipping...")
                                continue
                            is_suspended = True
                    elif match_level == 'arm':
                        if parent_value.get('arm_suspended', 'n').lower().strip() == 'y':
                            if not matchengine.match_on_closed:
                                log.info(f"{protocol_no} {match_level} {internal_id} is suspended, skipping...")
                                continue
                            is_suspended = True
                    elif match_level == 'dose_level':
                        if parent_value.get('level_suspended', 'n').lower().strip() == 'y':
                            if not matchengine.match_on_closed:
                                log.info(f"{protocol_no} {match_level} {internal_id} is suspended, skipping...")
                                continue
                            is_suspended = True

                    yield MatchClauseData(inner_value,
                                          internal_id,
                                          code,
                                          coordinating_center,
                                          is_suspended,
                                          status_for_match_clause,
                                          parent_path,
                                          level,
                                          parent_value,
                                          trial[matchengine.match_criteria_transform.trial_identifier])
                else:
                    process_q.append((path + (parent_key,), inner_key, inner_value))
        elif parent_value.__class__ is list:
            for index, item in enumerate(parent_value):
                process_q.append((path + (parent_key,), index, item))


def create_match_tree(matchengine, match_clause_data: MatchClauseData) -> MatchTree:
    """
    Turn a match clause from a trial curation into a digraph.
    """
    match_clause = match_clause_data.match_clause
    process_q: deque[Tuple[NodeID, Dict[str, Any]]] = deque()
    graph = nx.DiGraph()
    node_id: NodeID = NodeID(1)
    graph.add_node(0)  # root node is 0
    graph.nodes[0]['criteria_list'] = list()
    graph.nodes[0]['is_and'] = True
    graph.nodes[0]['or_nodes'] = set()
    graph.nodes[0]['label'] = '0 - ROOT and'
    graph.nodes[0]['label_list'] = list()
    for item in match_clause:
        if any([k.startswith('or') for k in item.keys()]):
            process_q.appendleft((NodeID(0), item))
        else:
            process_q.append((NodeID(0), item))

    def graph_match_clause():
        """
        A debugging function used if the --visualize-match-paths flag is passed. This function will output images
        of the digraphs which are an intermediate data structure used to generate mongo queries later.
        """
        import matplotlib.pyplot as plt
        from networkx.drawing.nx_agraph import graphviz_layout
        import os
        labels = {node: graph.nodes[node]['label'] for node in graph.nodes}
        for node in graph.nodes:
            if graph.nodes[node]['label_list']:
                labels[node] = labels[node] + ' [' + ','.join(graph.nodes[node]['label_list']) + ']'
        pos = graphviz_layout(graph, prog="dot", root=0)
        plt.figure(figsize=(30, 30))
        nx.draw_networkx(graph, pos, with_labels=True, node_size=[600 for _ in graph.nodes], labels=labels)
        plt.savefig(os.path.join(matchengine.fig_dir, (f'{match_clause_data.protocol_no}-'
                                                       f'{match_clause_data.match_clause_level}-'
                                                       f'{match_clause_data.internal_id}.png')))
        return plt

    while process_q:
        parent_id, values = process_q.pop()
        parent_is_and = True if graph.nodes[parent_id].get('is_and', False) else False

        # label is 'and', 'or', 'extended_attributes' or 'clinical'
        for label, value in values.items():
            if label.startswith('and'):
                criteria_list = list()
                label_list = list()
                for item in value:
                    for inner_label, inner_value in item.items():
                        if inner_label.startswith("or"):
                            process_q.appendleft(
                                (parent_id if parent_is_and else node_id, {inner_label: inner_value}))
                        elif inner_label.startswith("and"):
                            process_q.append((parent_id if parent_is_and else node_id, {inner_label: inner_value}))
                        else:
                            criteria_list.append({inner_label: inner_value})
                            label_list.append(inner_label)
                if parent_is_and:
                    graph.nodes[parent_id]['criteria_list'].extend(criteria_list)
                    graph.nodes[parent_id]['label_list'].extend(label_list)
                else:
                    graph.add_edges_from([(parent_id, node_id)])
                    graph.nodes[node_id].update({
                        'criteria_list': criteria_list,
                        'is_and': True,
                        'is_or': False,
                        'or_nodes': set(),
                        'label': str(node_id) + ' - ' + label,
                        'label_list': label_list
                    })
                    node_id += 1
            elif label.startswith("or"):
                or_node_id = node_id
                graph.add_node(or_node_id)
                graph.nodes[or_node_id].update({
                    'criteria_list': list(),
                    'is_and': False,
                    'is_or': True,
                    'label': str(or_node_id) + ' - ' + label,
                    'label_list': list()
                })
                node_id += 1
                for item in value:
                    process_q.append((or_node_id, item))
                if parent_is_and:
                    parent_or_nodes = graph.nodes[parent_id]['or_nodes']
                    if not parent_or_nodes:
                        graph.add_edges_from([(parent_id, or_node_id)])
                        graph.nodes[parent_id]['or_nodes'] = {or_node_id}
                    else:
                        successors = [
                            (successor, or_node_id)
                            for parent_or_node in parent_or_nodes
                            for successor in nx.descendants(graph, parent_or_node)
                            if graph.out_degree(successor) == 0
                        ]
                        graph.add_edges_from(successors)
                else:
                    graph.add_edge(parent_id, or_node_id)
            else:
                if parent_is_and:
                    graph.nodes[parent_id]['criteria_list'].append(values)
                    graph.nodes[parent_id]['label_list'].append(label)
                else:
                    graph.add_node(node_id)
                    graph.nodes[node_id].update({
                        'criteria_list': [values],
                        'is_or': False,
                        'is_and': True,
                        'label': str(node_id) + ' - ' + label,
                        'label_list': list()
                    })
                    graph.add_edge(parent_id, node_id)
                    node_id += 1

    if matchengine.visualize_match_paths:
        graph_match_clause()
    return MatchTree(graph)


def get_match_paths(match_tree: MatchTree) -> Generator[MatchCriterion]:
    """
    Takes a MatchTree (from create_match_tree) and yields the criteria from each possible path on the tree,
    from the root node to each leaf node
    """
    leaves = list()
    for node in match_tree.nodes:
        if match_tree.out_degree(node) == 0:
            leaves.append(node)
    for leaf in leaves:
        for path in nx.all_simple_paths(match_tree, 0, leaf) if leaf != 0 else [[leaf]]:
            match_path = MatchCriterion(list())
            for depth, node in enumerate(path):
                if match_tree.nodes[node]['criteria_list']:
                    match_path.add_criteria(MatchCriteria(match_tree.nodes[node]['criteria_list'], depth, node))
            if match_path:
                yield match_path


def translate_match_path(matchengine,
                         match_clause_data: MatchClauseData,
                         match_criterion: MatchCriterion) -> MultiCollectionQuery:
    """
    Translate the keys/values from the trial curation into keys/values used in a extended_attributes/clinical document.
    Uses an external config file ./config/config.json

    """
    multi_collection_query = MultiCollectionQuery(list(), list())
    query_cache = set()
    for node in match_criterion.criteria_list:
        for criteria in node.criteria:
            for node_name, values in criteria.items():
                trial_key_mappings = matchengine.match_criteria_transform.ctml_collection_mappings[node_name][
                    'trial_key_mappings']
                initial_query_node = QueryNode(node_name, node.node_id, criteria, node.depth, list(), None)
                query_nodes = list()
                query_nodes.append(initial_query_node)
                for trial_key, trial_value in values.items():
                    trial_key_settings = trial_key_mappings.get(trial_key.upper(), dict())

                    if trial_key_settings.get('ignore', False):
                        continue

                    sample_value_function_name = trial_key_settings.get('sample_value', 'nomap')
                    sample_function = getattr(matchengine.match_criteria_transform.query_transformers,
                                              sample_value_function_name)
                    sample_function_args = dict(sample_key=trial_key.upper(),
                                                trial_value=trial_value,
                                                parent_path=match_clause_data.parent_path,
                                                trial_path=node_name,
                                                trial_key=trial_key)
                    sample_function_args.update(trial_key_settings)
                    result: QueryTransformerResult = sample_function(**sample_function_args)
                    to_create = len(result.results) - 1
                    created_nodes = [query_node.__copy__()
                                     for _
                                     in range(0, to_create)
                                     for query_node
                                     in query_nodes]
                    for initial_query_node in query_nodes:
                        query_part = result.results[0]
                        initial_query_node.add_query_part(query_part)
                    for new_query_node, query_part in zip(created_nodes, cycle(result.results[1::])):
                        new_query_node.add_query_part(query_part)
                        query_nodes.append(new_query_node)
                    for query_node in query_nodes:
                        query_node.exclusion = (True
                                                if (query_node.query_parts[-1].negate
                                                    or query_node.exclusion)
                                                else False)
                query_node_container = QueryNodeContainer(list())
                sibling_nodes = len(query_nodes)
                for query_node in query_nodes:
                    if query_node.exclusion is not None:
                        query_node.sibling_nodes = sibling_nodes
                        matchengine.query_node_transform(query_node)
                        query_node.finalize()
                        query_node_hash = query_node.hash()
                        if query_node_hash not in query_cache:
                            query_cache.add(query_node_hash)
                            query_node_container.query_nodes.append(query_node)
                matchengine.query_node_container_transform(query_node_container)
                node_type = 'clinical' if node_name == 'clinical' else 'extended_attributes'
                getattr(multi_collection_query, node_type).append(query_node_container)
    return multi_collection_query
