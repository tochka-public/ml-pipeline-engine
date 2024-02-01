import typing as t

import networkx as nx

from ml_pipeline_engine.dag.graph import DiGraph
from ml_pipeline_engine.types import NodeId


def get_connected_subgraph(dag: nx.Graph, source: NodeId, dest: NodeId, is_recurrent: bool = False) -> DiGraph:
    """
    Получить связный подграф между двумя заданными узлами графа
    """

    if len(dag) == 1:
        return t.cast(DiGraph, dag)

    subgraph: DiGraph = dag.subgraph({node_id for path in nx.all_simple_paths(dag, source, dest) for node_id in path})
    subgraph.is_recurrent = is_recurrent

    return subgraph
