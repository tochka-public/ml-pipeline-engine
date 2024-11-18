import typing as t

from ml_pipeline_engine.dag_builders.annotation.marks import Input
from ml_pipeline_engine.dag_builders.annotation.marks import RecurrentSubGraph
from ml_pipeline_engine.node import ProcessorBase
from ml_pipeline_engine.node import RecurrentProcessor
from ml_pipeline_engine.types import AdditionalDataT
from ml_pipeline_engine.types import PipelineChartLike
from ml_pipeline_engine.types import Recurrent


class InputNumber(ProcessorBase):
    def process(
        self,
        num: float,
        additional_data: t.Optional[AdditionalDataT] = None,  # noqa: ARG002
    ) -> float:
        return num


class PassNum(ProcessorBase):
    def process(self, _: Input(InputNumber)) -> float:
        raise Exception('AnErrorFromPassNum')


class DoubleNumber(RecurrentProcessor):
    async def process(self, num: Input(PassNum)) -> Recurrent:
        pass


double_number = RecurrentSubGraph(
    start_node=InputNumber,
    dest_node=DoubleNumber,
    max_iterations=3,
)


class ANode(ProcessorBase):
    async def process(
        self,
        potential_results: double_number,
    ) -> t.Any:
        return potential_results


async def test_dag(
    build_chart: t.Callable[..., PipelineChartLike],
) -> None:
    chart = build_chart(input_node=InputNumber, output_node=ANode)
    result = await chart.run(input_kwargs=dict(num=3))

    assert result.error.__class__ == Exception
    assert result.error.args == ('AnErrorFromPassNum', )
    assert result.value is None
