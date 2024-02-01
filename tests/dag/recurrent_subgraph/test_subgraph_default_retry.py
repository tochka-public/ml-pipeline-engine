import typing as t

from tests.helpers import FactoryMocker, call_object
from ml_pipeline_engine.base_nodes.processors import (
    ProcessorBase,
    RecurrentProcessor,
)
from ml_pipeline_engine.dag_builders.annotation.marks import (
    Input,
    RecurrentSubGraph,
)
from ml_pipeline_engine.types import AdditionalDataT

invert_process_mocker = FactoryMocker()
double_default_mocker = FactoryMocker()


class InvertNumber(RecurrentProcessor):
    attempts = 2
    use_default = True

    def get_default(self) -> int:
        return -5

    @invert_process_mocker
    def process(
        self,
        num: float,
        additional_data: t.Optional[AdditionalDataT] = None,
    ) -> float:
        raise Exception('error')


class AddZero(RecurrentProcessor):
    def process(self, num: Input(InvertNumber)):
        return num + 0


class DoubleNumber(RecurrentProcessor):
    use_default = True

    @double_default_mocker
    def get_default(self):
        return -6

    async def process(self, num: Input(AddZero)):

        if num == -5:
            return self.next_iteration(num)

        return num * 2


recurrent_double_number = RecurrentSubGraph(
    dest_node=DoubleNumber,
    start_node=InvertNumber,
    max_iterations=2,
)


class AddConst(ProcessorBase):
    def process(self, num: float = -3.0, const: float = 0.2):
        return num + const


class AddNumbers(ProcessorBase):
    def process(self, num1: Input(AddConst), num2: recurrent_double_number):
        return num1 + num2


async def test_dag(build_dag, pipeline_context):
    assert await build_dag(input_node=InvertNumber, output_node=AddNumbers).run(pipeline_context(num=3.0)) == -8.8

    assert double_default_mocker.mock.get_default.call_count == 1
    assert invert_process_mocker.mock.process.mock_calls == [
        # Основной запуск графа
        # Первый запуск dag
        call_object(num=3.0),
        # Вторая попытка из-за ошибки (Retry протокол)
        call_object(num=3.0),

        # Первая итерация подграфа
        # Первый круг рекуррентного запуска
        call_object(num=3.0, additional_data=-5),
        # Первый круг рекуррентного запуска вместе с ретраем
        call_object(num=3.0, additional_data=-5),

        # Вторая итерация подграфа
        # Второй круг рекуррентного запуска
        call_object(num=3.0, additional_data=-5),
        # Второй круг рекуррентного запуска вместе с ретраем
        call_object(num=3.0, additional_data=-5),
    ]
