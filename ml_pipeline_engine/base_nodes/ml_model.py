from ml_pipeline_engine.types import NodeBase


class MLModelBase(NodeBase):
    """
    Базовый класс для ML-моделей
    """

    node_type = 'ml_model'

    def predict(self, *args, **kwargs):
        raise NotImplementedError('Method predict() is not implemented')
