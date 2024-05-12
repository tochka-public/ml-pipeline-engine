import typing as t

from ml_pipeline_engine.node.enums import NodeType
from ml_pipeline_engine.types import AdditionalDataT
from ml_pipeline_engine.types import NodeBase
from ml_pipeline_engine.types import Recurrent
from ml_pipeline_engine.types import RecurrentProtocol


class ProcessorBase(NodeBase):
    """
    Базовый класс для обработчиков общего назначения
    """

    node_type = NodeType.processor.value

    def process(self, *args: t.Any, **kwargs: t.Any) -> t.Any:
        raise NotImplementedError('Method process() is not implemented')


class RecurrentProcessor(ProcessorBase, RecurrentProtocol):
    """
    Узел процессора, который может быть исполнен в рекуррентном подграфе
    """

    def next_iteration(self, data: AdditionalDataT) -> Recurrent:
        return Recurrent(data=data)
