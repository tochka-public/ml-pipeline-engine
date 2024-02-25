import pytest

from ml_pipeline_engine.base_nodes.processors import ProcessorBase
from ml_pipeline_engine.dag_builders.annotation.marks import Input, SwitchCase


class Ident(ProcessorBase):
    def process(self, num: float):
        return num


class Invert(ProcessorBase):
    def process(self, num: Input(Ident)):
        return -num


class DoubleNumber(ProcessorBase):
    def process(self, num: Input(Ident)):
        return num * 2


class TripleNumber(ProcessorBase):
    def process(self, num: Input(Ident)):
        return num * 3


class FirstSwitchNode(ProcessorBase):
    def process(self, num: Input(Ident)):
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
    def process(self, num: FirstSwitchCase):
        return num


class SecondSwitchNode(ProcessorBase):
    def process(self, num: Input(Ident)):
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
    def process(self, num: SecondSwitchCase, num2: Input(Ident)):
        return num + num2


class Out(ProcessorBase):
    def process(self, num1: Input(FirstCaseNode), num2: Input(SecondCaseNode)):
        return num1 + num2


@pytest.mark.parametrize(
    'input_num, expect',
    [
        (-1.0, -3.0),
        (1.0, 4.0),
        (2.0, 10.0),
    ],
)
async def test_dag_multiple_switch_case(input_num, expect, build_dag, pipeline_context):
    assert await build_dag(input_node=Ident, output_node=Out).run(pipeline_context(num=input_num)) == expect
