from ml_pipeline_engine.types import NodeBase


class FeatureBase(NodeBase):
    """
    Базовый класс для набора фичей
    """

    node_type = 'feature'

    def extract(self, *args, **kwargs):
        raise NotImplementedError('Method extract() is not implemented')


class FallbackFeatureBase(NodeBase):
    """
    Базовый класс для набора fallback фичей
    """

    node_type = 'feature_fallback'

    def extract(self, *args, **kwargs):
        raise NotImplementedError('Method extract() is not implemented')


class FeatureGenericBase(NodeBase):
    """
    Базовый класс для набора generic фичей
    """

    node_type = 'feature_generic'

    def __init__(self):
        self.value = None

    def extract(self, *args, **kwargs):
        raise NotImplementedError
