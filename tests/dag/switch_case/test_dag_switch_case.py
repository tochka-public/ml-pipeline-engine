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
        if num == 0.0:
            return 'const'
        if num == 1.0:
            return 'double'
        return 'add_sub_chain'


class ConstNoInput(ProcessorBase):
    def process(self) -> float:
        return 10.0


class Add100(ProcessorBase):
    def process(self, num: Input(Ident)) -> float:
        return num + 100


class AddAConst(ProcessorBase):
    def process(self, num: Input(Ident), const: float = 9.0) -> float:
        return num + const


class InvertNumber(ProcessorBase):
    def process(self, num: Input(Ident)) -> float:
        return -num


class SubIdent(ProcessorBase):
    def process(self, num: Input(AddAConst), num_ident: Input(Ident)) -> float:
        return num - num_ident


class DoubleNumber(ProcessorBase):
    def process(self, num: Input(Ident)) -> float:
        return num * 2


SomeSwitchCase = SwitchCase(
    name='some_switch_case',
    switch=SwitchNode,
    cases=[
        ('const', ConstNoInput),
        ('double', DoubleNumber),
        ('invert', InvertNumber),
        ('add_sub_chain', SubIdent),
    ],
)


class CaseNode(ProcessorBase):
    def process(self, num: SomeSwitchCase, num2: Input(Ident), num3: Input(Add100)) -> float:
        return num + num2 + num3


class Out(ProcessorBase):
    def process(self, num: Input(CaseNode)) -> float:
        return num


@pytest.mark.parametrize(
    'input_num, expect',
    [
        (-1.0, 99.0),
        (0.0, 110.0),
        (1.0, 104.0),
        (3.0, 115),
    ],
)
async def test_dag_switch_case(
    input_num: float,
    expect: float,
    build_chart: t.Callable[..., PipelineChartLike],
) -> None:
    chart = build_chart(input_node=Ident, output_node=Out)
    result = await chart.run(input_kwargs=dict(num=input_num))

    assert result.value == expect
    assert result.error is None
