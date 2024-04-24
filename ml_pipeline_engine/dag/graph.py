import itertools
import typing as t

import networkx as nx

from ml_pipeline_engine.types import NodeId


class DiGraph(nx.DiGraph):

    def __init__(self, is_recurrent: bool = False, is_oneof: bool = False, **kwargs: t.Any) -> None:
        super().__init__(**kwargs)

        self.is_recurrent = is_recurrent
        self.is_oneof = is_oneof

    def __hash__(self) -> int:
        return hash(tuple(sorted(itertools.chain(*self.nodes.keys(), *self.edges.keys()))))


def get_connected_subgraph(
    dag: nx.Graph,
    source: NodeId,
    dest: NodeId,
    is_recurrent: bool = False,
    is_oneof: bool = False,
) -> DiGraph:
    """
    Get a connected subgraph between two nodes
    """

    if len(dag) == 1:
        return t.cast(DiGraph, dag)

    subgraph: DiGraph = dag.subgraph({node_id for path in nx.all_simple_paths(dag, source, dest) for node_id in path})
    subgraph.is_recurrent = is_recurrent
    subgraph.is_oneof = is_oneof

    return subgraph
