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
        if num == 0.0:
            return 'const'
        if num == 1.0:
            return 'double'
        return 'add_sub_chain'


class ConstNoInput(ProcessorBase):
    def process(self):
        return 10.0


class Add100(ProcessorBase):
    def process(self, num: Input(Ident)):
        return num + 100


class AddAConst(ProcessorBase):
    def process(self, num: Input(Ident), const: float = 9.0):
        return num + const


class InvertNumber(ProcessorBase):
    def process(self, num: Input(Ident)):
        return -num


class SubIdent(ProcessorBase):
    def process(self, num: Input(AddAConst), num_ident: Input(Ident)):
        return num - num_ident


class DoubleNumber(ProcessorBase):
    def process(self, num: Input(Ident)):
        return num * 2


SomeSwitchCase = SwitchCase(
    switch=SwitchNode,
    cases=[
        ('const', ConstNoInput),
        ('double', DoubleNumber),
        ('invert', InvertNumber),
        ('add_sub_chain', SubIdent),
    ],
)


class CaseNode(ProcessorBase):
    def process(self, num: SomeSwitchCase, num2: Input(Ident), num3: Input(Add100)):
        return num + num2 + num3


class Out(ProcessorBase):
    def process(self, num: Input(CaseNode)):
        return num


@pytest.mark.skip('Мультипроцессинг временно не поддерживается')
@pytest.mark.parametrize(
    'input_num, expect',
    [
        (-1.0, 99.0),
        (0.0, 110.0),
        (1.0, 104.0),
        (3.0, 115),
    ],
)
def test_dag_switch_case_multiprocess(input_num, expect, build_multiprocess_dag, pipeline_multiprocess_context):
    dag = build_multiprocess_dag(input_node=Ident, output_node=Out)
    assert dag.run(pipeline_multiprocess_context(num=input_num)) == expect


@pytest.mark.parametrize(
    'input_num, expect',
    [
        (-1.0, 99.0),
        (0.0, 110.0),
        (1.0, 104.0),
        (3.0, 115),
    ],
)
async def test_dag_switch_case(input_num, expect, build_dag, pipeline_context):
    assert await build_dag(input_node=Ident, output_node=Out).run(pipeline_context(num=input_num)) == expect
