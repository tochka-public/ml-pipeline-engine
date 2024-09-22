import inspect
import json
import pathlib
import typing as t
import warnings

from ml_pipeline_viewer.visualization import schema
from ml_pipeline_viewer.visualization.utils import copy_resources

from ml_pipeline_engine import const
from ml_pipeline_engine.node import get_callable_run_method
from ml_pipeline_engine.node.enums import NodeType
from ml_pipeline_engine.types import DAGLike
from ml_pipeline_engine.types import NodeBase
from ml_pipeline_engine.types import NodeId

_HexColorT = t.TypeVar('_HexColorT', bound=str)
_NodeTypeT = t.TypeVar('_NodeTypeT', bound=t.Union[str, NodeType])
_NodeColorsT = t.Dict[_NodeTypeT, _HexColorT]


class GraphConfigImpl:

    def __init__(self, dag: DAGLike) -> None:
        self._dag = dag

    def _get_node(self, node_id: NodeId) -> t.Type[NodeBase]:
        """
        Get a node object. Sometimes it can be None if we work with an artificial node
        """
        return self._dag.node_map.get(node_id)

    @staticmethod
    def _get_node_relative_path(node: t.Type[NodeBase]) -> str:
        """
        Generate relative path for a node
        """

        generic_class = getattr(node, '__generic_class__', None)

        if generic_class is None:
            file_path = '/'.join(node.__module__.split('.'))

        else:
            file_path = '/'.join(generic_class.__module__.split('.'))
            node = generic_class

        line_number = inspect.getsourcelines(node)[-1]
        return f'{file_path}.py#L{line_number}'

    def _generate_nodes(self) -> t.List[schema.Node]:
        """
        Generate physical and artificial nodes
        """

        nodes = []

        for node_id in self._dag.graph.nodes:
            node = self._get_node(node_id)

            if node is None:
                nodes.append(
                    schema.Node(
                        id=node_id,
                        is_virtual=True,
                        is_generic=False,
                        type=NodeType.by_prefix(node_id).value,
                    ),
                )

            else:
                method = get_callable_run_method(node)

                nodes.append(
                    schema.Node(
                        id=node_id,
                        type=node.node_type,
                        is_virtual=False,
                        is_generic=NodeType.is_generic(node.__name__),
                        data=schema.NodeAttributes(
                            name=node.name,
                            verbose_name=node.verbose_name,
                            doc=inspect.getdoc(method) or inspect.getdoc(node),
                            code_source=self._get_node_relative_path(node),
                        ),
                    ),
                )

        return nodes

    def _generate_edges(self) -> t.List[schema.Edge]:
        """
        Generate edges between physical and artificial nodes
        """
        return [
            schema.Edge(source=source, target=target)
            for source, target in self._dag.graph.edges
        ]

    def _generate_node_types(self, node_colors: t.Optional[_NodeColorsT] = None) -> t.Dict[str, schema.NodeType]:
        """
        Generate all node types.
        Will skip nodes without type.
        """
        node_colors = node_colors or {}
        node_types = {}

        for node_id in self._dag.graph.nodes:
            node = self._get_node(node_id)

            if node is None:
                node_type = NodeType.by_prefix(node_id)

            elif node.node_type is None:
                node_type = None
                warnings.warn(f'Node {node_id} without node type.', stacklevel=1)

            else:
                node_type = NodeType(node.node_type)

            if node_type in node_types or node_type is None:
                continue

            node_types[node_type.value] = schema.NodeType(
                name=node_type.value,
                hex_bgr_color=node_colors.get(node_type.value),
            )

        return node_types

    def generate(
        self,
        name: str,
        verbose_name: t.Optional[str] = None,
        repo_link: t.Optional[str] = None,
        node_colors: t.Optional[_NodeColorsT] = None,
        **kwargs: t.Any,
    ) -> schema.GraphConfig:
        """
        Generate a config for graph visualizer

        Args:
            name: Tech name for the graph
            verbose_name: Name for the graph
            repo_link: Repo link with commit or without
            node_colors: Mapping of node type colors
            **kwargs: Attrs for the graph
        """

        return schema.GraphConfig(
            nodes=self._generate_nodes(),
            edges=self._generate_edges(),
            node_types=self._generate_node_types(node_colors),
            attributes=schema.GraphAttributes(
                verbose_name=verbose_name or name,
                repo_link=repo_link,
                name=name,
                **kwargs,
            ),
        )


def build_static(config: schema.GraphConfig, target_dir: pathlib.Path) -> None:
    """
    Generate static using config and place it in the target dir
    """

    copy_resources(
        const.VISUALIZATION_LIB_ANCHOR, 'visualization', 'viewer',
        target_dir=str(target_dir),
    )

    config = json.dumps(config.as_dict(), ensure_ascii=False, indent=2)

    with pathlib.Path(target_dir / 'data.js').open('w', encoding='utf-8') as file:
        file.write(f'window.__GRAPH_DATA__ = {config}')
