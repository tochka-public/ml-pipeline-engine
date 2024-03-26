import itertools
from typing import Optional

import networkx as nx
from pyvis.network import Network


class DiGraph(nx.DiGraph):

    def __init__(self, is_recurrent: bool = False, **kwargs):
        super().__init__(**kwargs)

        self.is_recurrent = is_recurrent

    def __hash__(self):
        return hash(tuple(sorted(itertools.chain(*self.nodes.keys(), *self.edges.keys()))))

    def create_interactive_graph(self, height: Optional[str] = None, path: Optional[str] = None) -> None:
        """
        Creating an interactive graph via pyvis.
        As a result, you will receive an HTML file.
        """

        net = Network(
            height=height or '800px',
            directed=True,
            select_menu=True,
            filter_menu=True,
            cdn_resources='remote',
        )
        net.toggle_physics(False)
        net.show_buttons()
        net.from_nx(self)
        net.show(path or 'interactive_graph.html', notebook=False)
