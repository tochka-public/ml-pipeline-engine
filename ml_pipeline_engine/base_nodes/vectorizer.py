import typing as t

from ml_pipeline_engine.node.enums import NodeType
from ml_pipeline_engine.types import NodeBase


class FeatureVectorizerBase(NodeBase):
    """
    Базовый класс для векторизаторов
    """

    node_type = NodeType.vectorizer.value

    def vectorize(self, *args: t.Any, **kwargs: t.Any) -> t.Any:
        raise NotImplementedError('Method vectorize() is not implemented')
