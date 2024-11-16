from ml_pipeline_engine.chart import PipelineChart
from ml_pipeline_engine.dag_builders.annotation import build_dag_single
from ml_pipeline_engine.node import ProcessorBase


class DoubleNumber(ProcessorBase):
    def process(self, num: float) -> float:
        return num * 2


async def test_basic() -> None:
    chart = PipelineChart(
        'pipeline_name',
        build_dag_single(DoubleNumber),
    )
    result = await chart.run(input_kwargs=dict(num=2.5))
    assert result.value == 5.0
    assert result.error is None
