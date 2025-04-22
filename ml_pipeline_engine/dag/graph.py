import itertools
import typing as t

import networkx as nx

from ml_pipeline_engine.types import NodeId


class DiGraph(nx.DiGraph):

    def __init__(
        self,
        is_recurrent: bool = False,
        is_oneof: bool = False,
        is_nested_oneof: bool = False,
        **kwargs: t.Any,
    ) -> None:
        super().__init__(**kwargs)

        self.is_recurrent = is_recurrent
        self.is_oneof = is_oneof
        self.is_nested_oneof = is_nested_oneof

        self.source: t.Optional[NodeId] = None
        self.dest: t.Optional[NodeId] = None

        self.__hash_value: t.Optional[int] = None

    def __hash__(self) -> int:
        if self.__hash_value is None:
            self.__hash_value = hash(tuple(sorted(itertools.chain(*self.nodes.keys(), *self.edges.keys()))))
        return self.__hash_value


def get_connected_subgraph(
    dag: nx.DiGraph,
    source: NodeId,
    dest: NodeId,
    is_recurrent: bool = False,
    is_oneof: bool = False,
    is_nested_oneof: bool = False,
) -> DiGraph:
    """
    Get a connected subgraph between two nodes
    """

    if len(dag) == 1:
        return t.cast(DiGraph, dag)

    nodes = {node_id for path in nx.all_simple_paths(dag, source, dest) for node_id in path}
    subgraph = DiGraph(
        is_recurrent=is_recurrent,
        is_oneof=is_oneof,
        is_nested_oneof=is_nested_oneof,
    )
    subgraph.add_nodes_from((n, dag.nodes[n]) for n in nodes)
    subgraph.add_edges_from((u, v, dag.edges[u, v]) for u, v in dag.edges if u in nodes and v in nodes)

    subgraph.source = source
    subgraph.dest = dest
    subgraph.name = f'{source} —> {dest}, rec={is_recurrent}, oneof={is_oneof}, nested_oneof={is_nested_oneof}'

    return subgraph
