import pytest

from ml_pipeline_engine.base_nodes.processors import ProcessorBase
from ml_pipeline_engine.dag_builders.annotation.marks import Input


class InvertNumber(ProcessorBase):
    def process(self, num: float):
        return -num


class AddConst(ProcessorBase):
    def process(self, num: Input(InvertNumber), const: float = 0.2):
        return num + const


class DoubleNumber(ProcessorBase):
    def process(self, num: Input(InvertNumber)):
        return num * 2


class AddNumbers(ProcessorBase):
    def process(self, num1: Input(AddConst), num2: Input(DoubleNumber)):
        return num1 + num2


async def test_dag_rhombus(build_dag, pipeline_context):
    assert await build_dag(input_node=InvertNumber, output_node=AddNumbers).run(pipeline_context(num=3.0)) == -8.8


@pytest.mark.skip('Мультипроцессинг временно не поддерживается')
def test_dag_rhombus_multiprocess(build_multiprocess_dag, pipeline_multiprocess_context):
    dag = build_multiprocess_dag(input_node=InvertNumber, output_node=AddNumbers)
    assert dag.run(pipeline_multiprocess_context(num=3.0)) == -8.8
