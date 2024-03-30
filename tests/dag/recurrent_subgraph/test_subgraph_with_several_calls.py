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

        if additional_data is None:
            return num

        if additional_data == 5:
            return 5

        if additional_data == 7:
            return 11

        raise Exception


class JustPassNum(RecurrentProcessor):
    def process(self, num: Input(InvertNumber)):
        return num


class DoubleNumber(RecurrentProcessor):
    use_default = True

    def get_default(self):
        ...

    async def process(self, num: Input(JustPassNum)):

        if num == 3:
            return self.next_iteration(5)

        if num == 5:
            return self.next_iteration(7)

        if num == 11:
            return 11

        return num


recurrent_double_number = RecurrentSubGraph(
    start_node=InvertNumber,
    dest_node=DoubleNumber,
    max_iterations=3,
)


class JustANode(ProcessorBase):
    def process(self, num2: recurrent_double_number):
        return num2


async def test_dag(build_dag, pipeline_context, caplog_debug):
    assert await build_dag(input_node=InvertNumber, output_node=JustANode).run(pipeline_context(num=3)) == 11
