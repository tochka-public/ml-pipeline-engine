from ml_pipeline_engine.types import (
    AdditionalDataT,
    NodeBase,
    Recurrent,
    RecurrentProtocol,
)


class ProcessorBase(NodeBase):
    """
    Базовый класс для обработчиков общего назначения
    """

    node_type = 'processor'

    def process(self, *args, **kwargs):
        raise NotImplementedError('Method process() is not implemented')


class RecurrentProcessor(ProcessorBase, RecurrentProtocol):
    """
    Узел процессора, который может быть исполнен в рекуррентном подграфе
    """

    def next_iteration(self, data: AdditionalDataT) -> Recurrent:
        return Recurrent(data=data)
