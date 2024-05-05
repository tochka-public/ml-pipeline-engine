import typing as t

from ml_pipeline_engine.base_nodes.processors import ProcessorBase
from ml_pipeline_engine.context.dag import DAGPipelineContext
from ml_pipeline_engine.dag_builders.annotation import build_dag_single


class DoubleNumber(ProcessorBase):
    def process(self, num: float) -> float:
        return num * 2


async def test_basic(
    pipeline_context: t.Callable[..., DAGPipelineContext],
) -> None:
    assert await build_dag_single(DoubleNumber).run(pipeline_context(num=2.5)) == 5.0
