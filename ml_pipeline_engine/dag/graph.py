import networkx as nx
import itertools


class DiGraph(nx.DiGraph):

    def __init__(self, is_recurrent: bool = False, **kwargs):
        super().__init__(**kwargs)

        self.is_recurrent = is_recurrent

    def __hash__(self):
        return hash(tuple(sorted(itertools.chain(*self.nodes.keys(), *self.edges.keys()))))
