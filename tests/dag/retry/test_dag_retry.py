import typing as t

import pytest_mock

from ml_pipeline_engine.context.dag import DAGPipelineContext
from ml_pipeline_engine.dag_builders.annotation.marks import Input
from ml_pipeline_engine.node.base_nodes import ProcessorBase
from ml_pipeline_engine.types import DAGLike


class BaseExecutionError(Exception):
    pass


class FirstError(BaseExecutionError):
    pass


class SecondError(BaseExecutionError):
    pass


class ExternalDatasource:
    @staticmethod
    def external_func() -> float:
        return 0.1


class SomeNode(ProcessorBase):
    exceptions = (BaseExecutionError,)

    def process(self) -> float:
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


async def test_dag_retry(
    pipeline_context: t.Callable[..., DAGPipelineContext],
    build_dag: t.Callable[..., DAGLike],
    mocker: pytest_mock.MockerFixture,
) -> None:

    collect_spy = mocker.spy(SomeNode, 'process')
    external_func_patch = mocker.patch.object(
        ExternalDatasource,
        'external_func',
        side_effect=[
            FirstError,
            SecondError,
            0.1,
        ],
    )

    assert await build_dag(input_node=InvertNumber, output_node=DoubleNumber).run(pipeline_context(num=2.5)) == -4.8
    assert external_func_patch.call_count == 3
    assert collect_spy.call_count == 3
