import typing as t

from ml_pipeline_engine.node.enums import NodeType
from ml_pipeline_engine.types import NodeBase


class MLModelBase(NodeBase):
    """
    Базовый класс для ML-моделей
    """

    node_type = NodeType.ml_model.value

    def predict(self, *args: t.Any, **kwargs: t.Any) -> t.Any:
        raise NotImplementedError('Method predict() is not implemented')
