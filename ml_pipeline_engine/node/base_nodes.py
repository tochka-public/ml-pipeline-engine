import abc
import typing as t

from ml_pipeline_engine.node.enums import NodeType
from ml_pipeline_engine.types import AdditionalDataT
from ml_pipeline_engine.types import NodeBase
from ml_pipeline_engine.types import NodeResultT
from ml_pipeline_engine.types import Recurrent
from ml_pipeline_engine.types import RecurrentProtocol


class ProcessorBase(NodeBase):
    """
    Базовый класс для обработчиков общего назначения
    """

    node_type = NodeType.processor.value

    @abc.abstractmethod
    def process(self, *args: t.Any, **kwargs: t.Any) -> NodeResultT: ...


class RecurrentProcessor(ProcessorBase, RecurrentProtocol):
    """
    Узел процессора, который может быть исполнен в рекуррентном подграфе
    """

    def next_iteration(self, data: AdditionalDataT) -> Recurrent:
        return Recurrent(data=data)
