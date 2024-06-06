import typing as t

import pytest

from ml_pipeline_engine.base_nodes.processors import ProcessorBase
from ml_pipeline_engine.context.dag import DAGPipelineContext
from ml_pipeline_engine.dag import OneOfDoesNotHaveResultError
from ml_pipeline_engine.dag_builders.annotation.marks import Input
from ml_pipeline_engine.dag_builders.annotation.marks import InputOneOf
from ml_pipeline_engine.types import DAGLike


class PassNumber(ProcessorBase):
    def process(self, num: float) -> float:
        return num


class NodeWithError1(ProcessorBase):
    def process(self, _: Input(PassNumber)) -> t.Any:
        raise Exception('Error 1')


class NodeWithError2(ProcessorBase):
    def process(self, _: Input(PassNumber)) -> t.Any:
        raise Exception('Error 2')


class NodeWithError3(ProcessorBase):
    def process(self, _: Input(PassNumber)) -> t.Any:
        raise Exception('Error 3')


class OneOfNode(ProcessorBase):
    name = 'error-oneof-node'

    async def process(
        self,
        potential_results: InputOneOf([NodeWithError1, NodeWithError2, NodeWithError3]),
    ) -> t.Any:
        return potential_results


async def test_dag(
    pipeline_context: t.Callable[..., DAGPipelineContext],
    build_dag: t.Callable[..., DAGLike],
    caplog_debug: pytest.LogCaptureFixture,
) -> None:
    with pytest.raises(OneOfDoesNotHaveResultError, match='error-oneof-node'):
        assert await build_dag(input_node=PassNumber, output_node=OneOfNode).run(pipeline_context(num=3)) == 11

    assert all(f'Error {i}' in str(caplog_debug.messages) for i in (1, 2, 3))
