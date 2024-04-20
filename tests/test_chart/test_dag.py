import pytest

from ml_pipeline_engine.base_nodes.processors import ProcessorBase
from ml_pipeline_engine.chart import PipelineChart
from ml_pipeline_engine.dag_builders.annotation import build_dag_single


async def test_pipeline_chart_run_success(model_name_op) -> None:
    class SomeNode(ProcessorBase):
        def process(self, x: int) -> float:
            return x * 10

    some_model_pipeline = PipelineChart(
        model_name=model_name_op,
        entrypoint=build_dag_single(SomeNode),
    )

    result = await some_model_pipeline.run(input_kwargs=dict(x=2))

    assert result.value == 20
    assert result.error is None


async def test_pipeline_chart_run_error(model_name_op) -> None:
    class SomeErrorNode(ProcessorBase):
        def process(self, x: int) -> float:
            return x / 0

    some_model_pipeline = PipelineChart(
        model_name=model_name_op,
        entrypoint=build_dag_single(SomeErrorNode),
    )

    result = await some_model_pipeline.run(input_kwargs=dict(x=1))

    assert result.value is None
    assert isinstance(result.error, ZeroDivisionError)

    with pytest.raises(ZeroDivisionError):
        result.raise_on_error()
