import typing as t

import pytest_mock

from ml_pipeline_engine.context.dag import DAGPipelineContext
from ml_pipeline_engine.dag_builders.annotation.marks import Input
from ml_pipeline_engine.node.base_nodes import ProcessorBase
from ml_pipeline_engine.node.enums import NodeTag
from ml_pipeline_engine.parallelism import processes
from ml_pipeline_engine.parallelism import threads
from ml_pipeline_engine.types import DAGLike


class SomeInput(ProcessorBase):
    name = 'input'
    tags = (NodeTag.process,)

    def process(self, base_num: int, other_num: int) -> dict:
        return {
            'base_num': base_num,
            'other_num': other_num,
        }


class SomeDataSource(ProcessorBase):
    name = 'some_data_source'

    def process(self, inp: Input(SomeInput)) -> int:
        return inp['base_num'] + 100


class SomeFeature(ProcessorBase):
    name = 'some_feature'
    tags = (NodeTag.process,)

    def process(self, ds_value: Input(SomeDataSource), inp: Input(SomeInput)) -> int:
        return ds_value + inp['other_num'] + 10


class SomeVectorizer(ProcessorBase):
    name = 'some_vectorizer'
    tags = (NodeTag.non_async,)

    def process(self, feature_value: Input(SomeFeature)) -> int:
        return feature_value + 20


class SomeMLModel(ProcessorBase):
    name = 'some_model'

    async def process(self, vec_value: Input(SomeVectorizer)) -> float:
        return (vec_value + 30) / 100


async def test_tags__with_thread_process(
    pipeline_context: t.Callable[..., DAGPipelineContext],
    build_dag: t.Callable[..., DAGLike],
    mocker: pytest_mock.MockerFixture,
) -> None:
    threads_get_pool_executor = mocker.spy(threads.PoolExecutorRegistry, 'get_pool_executor')
    processes_get_pool_executor = mocker.spy(processes.PoolExecutorRegistry, 'get_pool_executor')

    dag = build_dag(input_node=SomeInput, output_node=SomeMLModel)
    assert await dag.run(pipeline_context(base_num=10, other_num=5)) == 1.75

    assert threads_get_pool_executor.call_count == 1
    assert processes_get_pool_executor.call_count == 2
