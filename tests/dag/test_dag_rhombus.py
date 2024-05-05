import typing as t

from ml_pipeline_engine.base_nodes.processors import ProcessorBase
from ml_pipeline_engine.context.dag import DAGPipelineContext
from ml_pipeline_engine.dag_builders.annotation.marks import Input
from ml_pipeline_engine.types import DAGLike


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
    pipeline_context: t.Callable[..., DAGPipelineContext],
    build_dag: t.Callable[..., DAGLike],
) -> None:
    assert await build_dag(input_node=InvertNumber, output_node=AddNumbers).run(pipeline_context(num=3.0)) == -8.8
