import pytest

from ml_pipeline_engine.base_nodes.datasources import DataSource
from ml_pipeline_engine.base_nodes.processors import ProcessorBase
from ml_pipeline_engine.dag_builders.annotation.marks import Input


class SomeNode(DataSource):
    exceptions = (Exception,)

    def collect(self):  # noqa
        raise BaseException('CustomError')  # noqa


class InvertNumber(ProcessorBase):
    def process(self, num: float):
        return -num


class AddConst(ProcessorBase):
    def process(self, num: Input(InvertNumber), const: Input(SomeNode)):
        return num + const


class DoubleNumber(ProcessorBase):
    def process(self, num: Input(AddConst)):
        return num * 2


async def test_dag_retry__base_error(pipeline_context, build_dag, mocker):

    collect_spy = mocker.spy(SomeNode, 'collect')

    with pytest.raises(BaseException, match='CustomError'):
        assert await build_dag(input_node=InvertNumber, output_node=DoubleNumber).run(pipeline_context(num=2.5))

    assert collect_spy.call_count == 1
