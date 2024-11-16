import typing as t

import pytest

from ml_pipeline_engine.dag import OneOfDoesNotHaveResultError
from ml_pipeline_engine.dag_builders.annotation.marks import Input
from ml_pipeline_engine.dag_builders.annotation.marks import InputOneOf
from ml_pipeline_engine.node import ProcessorBase
from ml_pipeline_engine.types import PipelineChartLike


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
    build_chart: t.Callable[..., PipelineChartLike],
    caplog_debug: pytest.LogCaptureFixture,
) -> None:
    chart = build_chart(input_node=PassNumber, output_node=OneOfNode)
    result = await chart.run(input_kwargs=dict(num=3))

    assert result.error.__class__ == OneOfDoesNotHaveResultError
    assert result.error.args == ('input_one_of__0___processor__error-oneof-node', )
    assert result.value is None

    assert all(f'Error {i}' in str(caplog_debug.messages) for i in (1, 2, 3))
