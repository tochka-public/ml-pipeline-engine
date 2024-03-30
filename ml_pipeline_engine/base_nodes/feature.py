from ml_pipeline_engine.node.enums import NodeType
from ml_pipeline_engine.types import NodeBase


class FeatureBase(NodeBase):
    """
    Базовый класс для набора фичей
    """

    node_type = NodeType.feature.value

    def extract(self, *args, **kwargs):
        raise NotImplementedError('Method extract() is not implemented')
