import typing as t

from ml_pipeline_engine.types import NodeBase


class DataSource(NodeBase):
    """
    Базовый класс для источников данных
    """

    node_type = 'datasource'

    def collect(self, *args, **kwargs) -> t.Any:
        raise NotImplementedError('Method collect() is not implemented')
