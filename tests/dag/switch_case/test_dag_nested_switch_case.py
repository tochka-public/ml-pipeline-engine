import pytest

from ml_pipeline_engine.base_nodes.processors import ProcessorBase
from ml_pipeline_engine.dag_builders.annotation.marks import Input, SwitchCase


class Ident(ProcessorBase):
    def process(self, num: float):
        return num


class SwitchNode(ProcessorBase):
    def process(self, num: Input(Ident)):
        if num < 0.0:
            return 'invert'
        return 'nested_switch'


class Invert(ProcessorBase):
    def process(self, num: Input(Ident)):
        return -num


class NestedSwitchNode(ProcessorBase):
    def process(self, num: Input(Ident)):
        if num == 1.0:
            return 'double'
        return 'triple'


class DoubleNumber(ProcessorBase):
    def process(self, num: Input(Ident)):
        return num * 2


class TripleNumber(ProcessorBase):
    def process(self, num: Input(Ident)):
        return num * 3


NestedSwitchCase = SwitchCase(
    switch=NestedSwitchNode,
    cases=[
        ('double', DoubleNumber),
        ('triple', TripleNumber),
    ],
)


class NestedCaseNode(ProcessorBase):
    def process(self, num: NestedSwitchCase):
        return num


SomeSwitchCase = SwitchCase(
    switch=SwitchNode,
    cases=[
        ('invert', Invert),
        ('nested_switch', NestedCaseNode),
    ],
)


class CaseNode(ProcessorBase):
    def process(self, num: SomeSwitchCase, num2: Input(Ident)):
        return num + num2


@pytest.mark.parametrize(
    'input_num, expect',
    [
        (-1.0, 0.0),
        (1.0, 3.0),
        (2.0, 8.0),
    ],
)
async def test_dag_nested_switch_case(input_num, expect, build_dag, pipeline_context):
    assert await build_dag(input_node=Ident, output_node=CaseNode).run(pipeline_context(num=input_num)) == expect
