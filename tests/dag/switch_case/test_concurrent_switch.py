from ml_pipeline_engine.base_nodes.processors import ProcessorBase
from ml_pipeline_engine.dag_builders.annotation.marks import Input
from ml_pipeline_engine.dag_builders.annotation.marks import SwitchCase


class Ident(ProcessorBase):
    async def process(self, num: float):
        return num


class ThirdSwitchNode(ProcessorBase):
    async def process(self, num: Input(Ident)):
        return num


class ThirdSwitchCase(ProcessorBase):
    async def process(self, num: Input(Ident)):
        return 'ident'


ThirdSwitch = SwitchCase(
    switch=ThirdSwitchCase,
    cases=[
        ('ident', ThirdSwitchNode),
    ],
)


class IdentSub(ProcessorBase):
    async def process(self, num: ThirdSwitch):
        return num


class FirstSwitchCase(ProcessorBase):
    async def process(self, num: Input(Ident)):
        return 'ident'


FirstSwitch = SwitchCase(
    switch=FirstSwitchCase,
    cases=[
        ('ident', IdentSub),
    ],
)


class DoubleNumber(ProcessorBase):
    async def process(self, num: ThirdSwitch):
        return num * 2


class SecondSwitchCase(ProcessorBase):
    async def process(self, num: Input(Ident)):
        return 'double'


SecondSwitch = SwitchCase(
    switch=SecondSwitchCase,
    cases=[
        ('double', DoubleNumber),
    ],
)


class Out(ProcessorBase):
    async def process(self, num1: FirstSwitch, num2: SecondSwitch):
        return num1 + num2


async def test_dag_multiple_switch_cases(build_dag, pipeline_context, caplog_debug):
    assert await build_dag(input_node=Ident, output_node=Out).run(pipeline_context(num=1)) == 3

    assert (
        'Node processor__tests_dag_switch_case_test_concurrent_switch_ThirdSwitchNode '
        'has been executed. Stop new execution'
    ) in caplog_debug.text
