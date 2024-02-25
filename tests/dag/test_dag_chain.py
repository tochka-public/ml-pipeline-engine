from ml_pipeline_engine.base_nodes.processors import ProcessorBase
from ml_pipeline_engine.dag_builders.annotation.marks import Input


class InvertNumber(ProcessorBase):
    def process(self, num: float):
        return -num


class AddConst(ProcessorBase):
    def process(self, num: Input(InvertNumber), const: float = 0.1):
        return num + const


class DoubleNumber(ProcessorBase):
    def process(self, num: Input(AddConst)):
        return num * 2


async def test_dag_chain(build_dag, pipeline_context):
    assert await build_dag(input_node=InvertNumber, output_node=DoubleNumber).run(pipeline_context(num=2.5)) == -4.8
