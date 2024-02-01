import networkx as nx


class DiGraph(nx.DiGraph):

    def __init__(self, is_recurrent: bool = False, **kwargs):
        super().__init__(**kwargs)

        self.is_recurrent = is_recurrent
