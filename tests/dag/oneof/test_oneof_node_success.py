import typing as t

from ml_pipeline_engine.base_nodes.processors import ProcessorBase
from ml_pipeline_engine.context.dag import DAGPipelineContext
from ml_pipeline_engine.dag_builders.annotation.marks import Input
from ml_pipeline_engine.dag_builders.annotation.marks import InputOneOf
from ml_pipeline_engine.types import DAGLike


class PassNumber(ProcessorBase):
    def process(self, num: float) -> float:
        return num


class NodeWithError(ProcessorBase):
    def process(self, _: Input(PassNumber)) -> t.Any:
        raise Exception('An error')


class AnotherNodeWithError(ProcessorBase):
    def process(self, _: Input(PassNumber)) -> t.Any:
        raise Exception('An error from different place')


class FallbackDefault(ProcessorBase):
    name = 'fallback'

    def process(self) -> t.Any:
        return 11


class OneOfNode(ProcessorBase):
    async def process(
        self,
        potential_results: InputOneOf([NodeWithError, AnotherNodeWithError, FallbackDefault]),
    ) -> t.Any:
        return potential_results


async def test_dag(
    pipeline_context: t.Callable[..., DAGPipelineContext],
    build_dag: t.Callable[..., DAGLike],
    caplog_debug
) -> None:
    assert await build_dag(input_node=PassNumber, output_node=OneOfNode).run(pipeline_context(num=3)) == 11
