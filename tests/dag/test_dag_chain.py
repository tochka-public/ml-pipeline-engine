import typing as t

from ml_pipeline_engine.context.dag import DAGPipelineContext
from ml_pipeline_engine.dag_builders.annotation.marks import Input
from ml_pipeline_engine.node import ProcessorBase
from ml_pipeline_engine.types import DAGLike


class InvertNumber(ProcessorBase):
    def process(self, num: float) -> float:
        return -num


class AddConst(ProcessorBase):
    def process(self, num: Input(InvertNumber), const: float = 0.1) -> float:
        return num + const


class DoubleNumber(ProcessorBase):
    def process(self, num: Input(AddConst)) -> float:
        return num * 2


async def test_dag_chain(
    pipeline_context: t.Callable[..., DAGPipelineContext],
    build_dag: t.Callable[..., DAGLike],
) -> None:
    assert await build_dag(input_node=InvertNumber, output_node=DoubleNumber).run(pipeline_context(num=2.5)) == -4.8
