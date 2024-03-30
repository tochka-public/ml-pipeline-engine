import typing as t

from ml_pipeline_engine.base_nodes.processors import ProcessorBase
from ml_pipeline_engine.base_nodes.processors import RecurrentProcessor
from ml_pipeline_engine.dag_builders.annotation.marks import Input
from ml_pipeline_engine.dag_builders.annotation.marks import RecurrentSubGraph
from ml_pipeline_engine.types import AdditionalDataT


class InvertNumber(RecurrentProcessor):

    def process(
        self,
        num: float,
        additional_data: t.Optional[AdditionalDataT] = None,
    ) -> float:

        if additional_data == -5:
            return -3
        return -5


class AddZero(RecurrentProcessor):
    def process(self, num: Input(InvertNumber)):
        return num + 0


class AnotherDoubleNumber(RecurrentProcessor):
    use_default = True

    def get_default(self, **__):
        return 1

    def process(self, num: Input(InvertNumber)):
        return self.next_iteration(num)


another_recurrent_double_number = RecurrentSubGraph(
    dest_node=AnotherDoubleNumber,
    start_node=InvertNumber,
    max_iterations=3,
)


class DoubleNumber(RecurrentProcessor):
    use_default = True

    def get_default(self, **__):
        return {}

    async def process(self, num: Input(AddZero), num2: another_recurrent_double_number):

        if num == -5:
            return self.next_iteration(num)

        return (num * 2) + num2


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
    assert await build_dag(input_node=InvertNumber, output_node=AddNumbers).run(pipeline_context(num=3.0)) == -7.8
