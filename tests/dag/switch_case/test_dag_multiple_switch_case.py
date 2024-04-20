import pytest

from ml_pipeline_engine.base_nodes.processors import ProcessorBase
from ml_pipeline_engine.dag_builders.annotation.marks import Input
from ml_pipeline_engine.dag_builders.annotation.marks import SwitchCase


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
async def test_dag_multiple_switch_case(input_num, expect, build_dag, pipeline_context) -> None:
    assert await build_dag(input_node=Ident, output_node=Out).run(pipeline_context(num=input_num)) == expect
