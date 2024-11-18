import typing as t

import pytest

from ml_pipeline_engine.dag_builders.annotation.marks import Input
from ml_pipeline_engine.dag_builders.annotation.marks import SwitchCase
from ml_pipeline_engine.node import ProcessorBase
from ml_pipeline_engine.types import PipelineChartLike


class Ident(ProcessorBase):
    def process(self, num: float) -> float:
        return num


class Invert(ProcessorBase):
    def process(self, num: Input(Ident)) -> float:
        return -num


class DoubleNumber(ProcessorBase):
    def process(self, num: Input(Ident)) -> float:
        return num * 2


class TripleNumber(ProcessorBase):
    def process(self, num: Input(Ident)) -> float:
        return num * 3


class FirstSwitchNode(ProcessorBase):
    def process(self, num: Input(Ident)) -> str:
        if num < 0.0:
            return 'invert'
        return 'ident'


FirstSwitchCase = SwitchCase(
    switch=FirstSwitchNode,
    cases=[
        ('invert', Invert),
        ('ident', Ident),
    ],
)


class FirstCaseNode(ProcessorBase):
    def process(self, num: FirstSwitchCase) -> float:
        return num


class SecondSwitchNode(ProcessorBase):
    def process(self, num: Input(Ident)) -> str:
        if num == 1.0:
            return 'double'
        return 'triple'


SecondSwitchCase = SwitchCase(
    switch=SecondSwitchNode,
    cases=[
        ('double', DoubleNumber),
        ('triple', TripleNumber),
    ],
)


class SecondCaseNode(ProcessorBase):
    def process(self, num: SecondSwitchCase, num2: Input(Ident)) -> float:
        return num + num2


class Out(ProcessorBase):
    def process(self, num1: Input(FirstCaseNode), num2: Input(SecondCaseNode)) -> float:
        return num1 + num2


@pytest.mark.parametrize(
    'input_num, expect',
    [
        (-1.0, -3.0),
        (1.0, 4.0),
        (2.0, 10.0),
    ],
)
async def test_dag_multiple_switch_case(
    input_num: float,
    expect: float,
    build_chart: t.Callable[..., PipelineChartLike],
) -> None:
    chart = build_chart(input_node=Ident, output_node=Out)
    result = await chart.run(input_kwargs=dict(num=input_num))

    assert result.value == expect
    assert result.error is None
