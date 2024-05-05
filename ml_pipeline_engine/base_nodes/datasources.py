import typing as t

from ml_pipeline_engine.node.enums import NodeType
from ml_pipeline_engine.types import NodeBase


class DataSource(NodeBase):
    """
    Базовый класс для источников данных
    """

    node_type = NodeType.datasource.value

    def collect(self, *args: t.Any, **kwargs: t.Any) -> t.Any:
        raise NotImplementedError('Method collect() is not implemented')
