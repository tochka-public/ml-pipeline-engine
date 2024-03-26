import typing as t

from ml_pipeline_engine.types import NodeBase
from ml_pipeline_engine.node.enums import NodeType


class DataSource(NodeBase):
    """
    Базовый класс для источников данных
    """

    node_type = NodeType.datasource.value

    def collect(self, *args, **kwargs) -> t.Any:
        raise NotImplementedError('Method collect() is not implemented')
