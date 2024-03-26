from ml_pipeline_engine.types import (
    AdditionalDataT,
    NodeBase,
    Recurrent,
    RecurrentProtocol,
)
from ml_pipeline_engine.node.enums import NodeType


class ProcessorBase(NodeBase):
    """
    Базовый класс для обработчиков общего назначения
    """

    node_type = NodeType.processor.value

    def process(self, *args, **kwargs):
        raise NotImplementedError('Method process() is not implemented')


class RecurrentProcessor(ProcessorBase, RecurrentProtocol):
    """
    Узел процессора, который может быть исполнен в рекуррентном подграфе
    """

    def next_iteration(self, data: AdditionalDataT) -> Recurrent:
        return Recurrent(data=data)
