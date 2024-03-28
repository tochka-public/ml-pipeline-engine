from ml_pipeline_engine.types import NodeBase
from ml_pipeline_engine.node.enums import NodeType


class FeatureBase(NodeBase):
    """
    Базовый класс для набора фичей
    """

    node_type = NodeType.feature.value

    def extract(self, *args, **kwargs):
        raise NotImplementedError('Method extract() is not implemented')
