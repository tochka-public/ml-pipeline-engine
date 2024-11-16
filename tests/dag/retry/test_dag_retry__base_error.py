import typing as t

import pytest
import pytest_mock

from ml_pipeline_engine.dag_builders.annotation.marks import Input
from ml_pipeline_engine.node import ProcessorBase
from ml_pipeline_engine.types import PipelineChartLike


class SomeNode(ProcessorBase):
    exceptions = (Exception,)

    def process(self):  # noqa
        raise BaseException('CustomError')


class InvertNumber(ProcessorBase):
    def process(self, num: float) -> float:
        return -num


class AddConst(ProcessorBase):
    def process(self, num: Input(InvertNumber), const: Input(SomeNode)) -> float:
        return num + const


class DoubleNumber(ProcessorBase):
    def process(self, num: Input(AddConst)) -> float:
        return num * 2


async def test_dag_retry__base_error(
    build_chart: t.Callable[..., PipelineChartLike],
    mocker: pytest_mock.MockerFixture,
) -> None:
    collect_spy = mocker.spy(SomeNode, 'process')

    chart = build_chart(input_node=InvertNumber, output_node=DoubleNumber)

    with pytest.raises(BaseException, match='CustomError'):
        await chart.run(input_kwargs=dict(num=2.5))

    assert collect_spy.call_count == 1
