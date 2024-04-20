import typing as t

from tests.helpers import FactoryMocker
from tests.helpers import call_object

from ml_pipeline_engine.base_nodes.processors import ProcessorBase
from ml_pipeline_engine.base_nodes.processors import RecurrentProcessor
from ml_pipeline_engine.dag_builders.annotation.marks import Input
from ml_pipeline_engine.dag_builders.annotation.marks import RecurrentSubGraph
from ml_pipeline_engine.types import AdditionalDataT
from ml_pipeline_engine.types import Recurrent

invert_process_mocker = FactoryMocker()
double_default_mocker = FactoryMocker()


class InvertNumber(RecurrentProcessor):

    @invert_process_mocker
    def process(
        self,
        num: float,
        additional_data: t.Optional[AdditionalDataT] = None,
    ) -> float:
        return -5


class AddZero(RecurrentProcessor):
    def process(self, num: Input(InvertNumber)) -> float:
        return num + 0


class DoubleNumber(RecurrentProcessor):
    use_default = True

    @double_default_mocker
    def get_default(self, **kwargs) -> float:
        return -6

    async def process(self, num: Input(AddZero)) -> t.Union[Recurrent, float]:

        if num == -5:
            return self.next_iteration(num)

        return num * 2


recurrent_double_number = RecurrentSubGraph(
    dest_node=DoubleNumber,
    start_node=InvertNumber,
    max_iterations=3,
)


class AddConst(ProcessorBase):
    def process(self, num: float = -3.0, const: float = 0.2) -> float:
        return num + const


class AddNumbers(ProcessorBase):
    def process(self, num1: Input(AddConst), num2: recurrent_double_number) -> float:
        return num1 + num2


async def test_dag(build_dag, pipeline_context) -> None:
    assert await build_dag(input_node=InvertNumber, output_node=AddNumbers).run(pipeline_context(num=3.0)) == -8.8

    assert double_default_mocker.mock.get_default.call_count == 1
    assert invert_process_mocker.mock.process.mock_calls == [
        call_object(num=3.0),
        call_object(num=3.0, additional_data=-5),
        call_object(num=3.0, additional_data=-5),
        call_object(num=3.0, additional_data=-5),
    ]
