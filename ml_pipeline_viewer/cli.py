import pathlib
from typing import List
from typing import Optional
from typing import Tuple

import click

from ml_pipeline_engine.node.enums import NodeType


@click.group()
def main() -> None:
    """Cli"""


@main.command()
@click.option('--dag_path', help='Dotted path to a dag', required=True)
@click.option('--dag_name', default='Dag', help='Tech name for the dag')
@click.option('--dag_verbose_name', default='Dag', help='Title for the dag')
@click.option('--target_dir', default='public', help='Target fir for static', type=pathlib.Path)
@click.option('--repo_link', help='Link for your repository')
@click.option('--color', type=(NodeType, str), multiple=True, help='Color for node types')
def build_static(
    dag_path: str,
    dag_name: str,
    dag_verbose_name: str,
    target_dir: pathlib.Path,
    repo_link: Optional[str] = None,
    color: Optional[List[Tuple[NodeType, str]]] = None,
) -> None:
    """Build static for a dag by path"""

    import importlib

    from ml_pipeline_viewer.visualization.dag import GraphConfigImpl
    from ml_pipeline_viewer.visualization.dag import build_static

    module, dag_object_name = dag_path.rsplit(':', 1)
    module = importlib.import_module(module)

    config = GraphConfigImpl(getattr(module, dag_object_name)).generate(
        node_colors=dict(color),
        repo_link=repo_link,
        verbose_name=dag_verbose_name,
        name=dag_name,
    )

    build_static(config, target_dir=target_dir)


if __name__ == '__main__':
    main()
