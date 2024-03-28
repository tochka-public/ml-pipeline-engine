from ml_pipeline_engine.types import NodeBase
from ml_pipeline_engine.node.enums import NodeType


class MLModelBase(NodeBase):
    """
    Базовый класс для ML-моделей
    """

    node_type = NodeType.ml_model.value

    def predict(self, *args, **kwargs):
        raise NotImplementedError('Method predict() is not implemented')
