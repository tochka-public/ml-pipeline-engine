import typing as t

import pytest

from ml_pipeline_engine.dag_builders.annotation.marks import Input
from ml_pipeline_engine.dag_builders.annotation.marks import RecurrentSubGraph
from ml_pipeline_engine.node import ProcessorBase
from ml_pipeline_engine.node import RecurrentProcessor
from ml_pipeline_engine.types import AdditionalDataT
from ml_pipeline_engine.types import PipelineChartLike
from ml_pipeline_engine.types import Recurrent


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
    def process(self, num: Input(InvertNumber)) -> float:
        return num


class DoubleNumber(RecurrentProcessor):

    async def process(self, num: Input(JustPassNum)) -> t.Union[Recurrent, float]:

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
    def process(self, num2: recurrent_double_number) -> float:
        return num2


async def test_dag(
    build_chart: t.Callable[..., PipelineChartLike],
    caplog_debug: pytest.LogCaptureFixture,
) -> None:
    chart = build_chart(input_node=InvertNumber, output_node=JustANode)
    result = await chart.run(input_kwargs=dict(num=3))
    assert result.value == 11
