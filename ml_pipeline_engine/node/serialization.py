from dataclasses import dataclass
from typing import Callable, Optional, Union

import cloudpickle

from ml_pipeline_engine.node.enums import SerializationNodeKind
from ml_pipeline_engine.types import NodeLike, NodeSerializerLike

_SERIALIZATION_MAPPING = {
    SerializationNodeKind.cloudpickle: cloudpickle,
}


@dataclass
class NodeSerializer(NodeSerializerLike):
    node: Union[NodeLike, bytes]
    serialization_kind: SerializationNodeKind

    @classmethod
    def serialize(cls, node: NodeLike, serialization_kind: Optional[SerializationNodeKind] = None) -> 'NodeSerializer':
        """
        Автоматическая сериализация объекта узла в бинарный вид для возможности передачи между процессами и тредами

        :param node: Объект узла
        :param serialization_kind: Модуль для сериализации
        """
        if serialization_kind is None:
            serialization_kind = SerializationNodeKind.cloudpickle

        return cls(
            node=cls._dumps(serialization_kind)(node),
            serialization_kind=serialization_kind,
        )

    def get_node(self) -> NodeLike:
        """
        Получение десериализованной ноды
        """
        return self._loads(self.serialization_kind)(self.node)

    @staticmethod
    def _dumps(serialization_kind: SerializationNodeKind) -> Callable:
        return _SERIALIZATION_MAPPING[serialization_kind].dumps

    @staticmethod
    def _loads(serialization_kind: SerializationNodeKind) -> Callable:
        return _SERIALIZATION_MAPPING[serialization_kind].loads
