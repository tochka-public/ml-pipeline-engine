import typing as t

import pytest
import pytest_mock

from ml_pipeline_engine.base_nodes.datasources import DataSource
from ml_pipeline_engine.base_nodes.processors import ProcessorBase
from ml_pipeline_engine.context.dag import DAGPipelineContext
from ml_pipeline_engine.dag_builders.annotation.marks import Input
from ml_pipeline_engine.types import DAGLike


class ExternalDatasource:
    @staticmethod
    def external_func() -> float:
        return 0.1


class SomeNode(DataSource):
    def collect(self):  # noqa
        return ExternalDatasource().external_func()


class InvertNumber(ProcessorBase):
    def process(self, num: float) -> float:
        return -num


class AddConst(ProcessorBase):
    def process(self, num: Input(InvertNumber), const: Input(SomeNode)) -> float:
        return num + const


class DoubleNumber(ProcessorBase):
    def process(self, num: Input(AddConst)) -> float:
        return num * 2


async def test_dag_retry__error(
    pipeline_context: t.Callable[..., DAGPipelineContext],
    build_dag: t.Callable[..., DAGLike],
    mocker: pytest_mock.MockerFixture,
) -> None:

    collect_spy = mocker.spy(SomeNode, 'collect')
    external_func_patch = mocker.patch.object(
        ExternalDatasource,
        'external_func',
        side_effect=[
            Exception,
            Exception,
            Exception('CustomError'),
        ],
    )

    with pytest.raises(Exception, match='CustomError'):
        assert await build_dag(input_node=InvertNumber, output_node=DoubleNumber).run(pipeline_context(num=2.5))

    assert external_func_patch.call_count == 3
    assert collect_spy.call_count == 3
