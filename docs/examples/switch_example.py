"""
Пример, который реализует переключение нод по условию (аналог switch)
"""

import asyncio

from ml_pipeline_engine.chart import PipelineChart
from ml_pipeline_engine.dag_builders.annotation import build_dag
from ml_pipeline_engine.dag_builders.annotation.marks import Input
from ml_pipeline_engine.dag_builders.annotation.marks import SwitchCase
from ml_pipeline_engine.node.base_nodes import ProcessorBase
from ml_pipeline_engine.parallelism import threads_pool_registry


class Indent(ProcessorBase):
    def process(self, num: float) -> float:
        return num


class SwitchNode(ProcessorBase):
    def process(self, num: Input(Indent)) -> str:
        if num < 0.0:
            return 'invert'
        if num == 0.0:
            return 'const'
        if num == 1.0:
            return 'double'
        return 'add_sub_chain'


class ConstNoinput(ProcessorBase):
    def process(self) -> float:
        return 10.0


class Add100(ProcessorBase):
    def process(self, num: Input(Indent)) -> float:
        return num + 100


class AddSomeConst(ProcessorBase):
    def process(self, num: Input(Indent), const: float = 9.0) -> float:
        return num + const


class InvertNumber(ProcessorBase):
    def process(self, num: Input(Indent)) -> float:
        return -num


class SubIdent(ProcessorBase):
    def process(self, num: Input(AddSomeConst), num_ident: Input(Indent)) -> float:
        return num - num_ident


class DoubleNumber(ProcessorBase):
    def process(self, num: Input(Indent)) -> float:
        return num * 2


SomeSwitchCase = SwitchCase(
    name='SomeSwitchCase',
    switch=SwitchNode,
    cases=[
        ('const', ConstNoinput),
        ('double', DoubleNumber),
        ('invert', InvertNumber),
        ('add_sub_chain', SubIdent),
    ],
)


class CaseNode(ProcessorBase):
    def process(self, num: SomeSwitchCase, num2: Input(Indent), num3: Input(Add100)) -> float:
        return num + num2 + num3


class Out(ProcessorBase):
    def process(self, num: Input(CaseNode)) -> float:
        return num


async def main() -> None:
    threads_pool_registry.auto_init()
    pipeline = PipelineChart(
        'example_pipeline_with_switch',
        build_dag(input_node=Indent, output_node=Out),
    )
    result = await pipeline.run(input_kwargs=dict(num=10))
    assert result.value == 129  # noqa: PLR2004


if __name__ == '__main__':
    asyncio.run(main())
