from ml_pipeline_engine.types import NodeBase
from ml_pipeline_engine.node.enums import NodeType


class FeatureVectorizerBase(NodeBase):
    """
    Базовый класс для векторизаторов
    """

    node_type = NodeType.vectorizer.value

    def vectorize(self, *args, **kwargs):
        raise NotImplementedError('Method vectorize() is not implemented')
