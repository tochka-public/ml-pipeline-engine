import typing as t

import pytest

from ml_pipeline_engine.dag_builders.annotation.marks import Input
from ml_pipeline_engine.dag_builders.annotation.marks import SwitchCase
from ml_pipeline_engine.node import ProcessorBase
from ml_pipeline_engine.types import PipelineChartLike


class Ident(ProcessorBase):
    def process(self, num: float) -> float:
        return num


class SwitchNode(ProcessorBase):
    def process(self, num: Input(Ident)) -> str:
        if num < 0.0:
            return 'invert'
        return 'nested_switch'


class Invert(ProcessorBase):
    def process(self, num: Input(Ident)) -> float:
        return -num


class NestedSwitchNode(ProcessorBase):
    def process(self, num: Input(Ident)) -> str:
        if num == 1.0:
            return 'double'
        return 'triple'


class DoubleNumber(ProcessorBase):
    def process(self, num: Input(Ident)) -> float:
        return num * 2


class TripleNumber(ProcessorBase):
    def process(self, num: Input(Ident)) -> float:
        return num * 3


NestedSwitchCase = SwitchCase(
    switch=NestedSwitchNode,
    cases=[
        ('double', DoubleNumber),
        ('triple', TripleNumber),
    ],
)


class NestedCaseNode(ProcessorBase):
    def process(self, num: NestedSwitchCase) -> float:
        return num


SomeSwitchCase = SwitchCase(
    switch=SwitchNode,
    cases=[
        ('invert', Invert),
        ('nested_switch', NestedCaseNode),
    ],
)


class CaseNode(ProcessorBase):
    def process(self, num: SomeSwitchCase, num2: Input(Ident)) -> float:
        return num + num2


@pytest.mark.parametrize(
    'input_num, expect',
    [
        (-1.0, 0.0),
        (1.0, 3.0),
        (2.0, 8.0),
    ],
)
async def test_dag_nested_switch_case(
    input_num: float,
    expect: float,
    build_chart: t.Callable[..., PipelineChartLike],
    caplog_debug: pytest.LogCaptureFixture,
) -> None:
    chart = build_chart(input_node=Ident, output_node=CaseNode)
    result = await chart.run(input_kwargs=dict(num=input_num))

    assert result.value == expect
    assert result.error is None
