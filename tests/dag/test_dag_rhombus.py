import typing as t

from ml_pipeline_engine.dag_builders.annotation.marks import Input
from ml_pipeline_engine.node import ProcessorBase
from ml_pipeline_engine.types import PipelineChartLike


class InvertNumber(ProcessorBase):
    def process(self, num: float) -> float:
        return -num


class AddConst(ProcessorBase):
    def process(self, num: Input(InvertNumber), const: float = 0.2) -> float:
        return num + const


class DoubleNumber(ProcessorBase):
    def process(self, num: Input(InvertNumber)) -> float:
        return num * 2


class AddNumbers(ProcessorBase):
    def process(self, num1: Input(AddConst), num2: Input(DoubleNumber)) -> float:
        return num1 + num2


async def test_dag_rhombus(
    build_chart: t.Callable[..., PipelineChartLike],
) -> None:
    chart = build_chart(input_node=InvertNumber, output_node=AddNumbers)
    result = await chart.run(input_kwargs=dict(num=3.0))

    assert result.value == -8.8
