from ml_pipeline_engine.dag_builders.annotation.marks import Input
from ml_pipeline_engine.node.enums import NodeTag
from ml_pipeline_engine.parallelism import processes, threads
from ml_pipeline_engine.types import NodeBase


class SomeInput(NodeBase):
    name = 'input'
    tags = (NodeTag.process,)

    def process(self, base_num: int, other_num: int) -> dict:
        return {
            'base_num': base_num,
            'other_num': other_num,
        }


class SomeDataSource(NodeBase):
    name = 'some_data_source'

    def collect(self, inp: Input(SomeInput)) -> int:
        return inp['base_num'] + 100


class SomeFeature(NodeBase):
    name = 'some_feature'
    tags = (NodeTag.process,)

    def extract(self, ds_value: Input(SomeDataSource), inp: Input(SomeInput)) -> int:
        return ds_value + inp['other_num'] + 10


class SomeVectorizer(NodeBase):
    name = 'some_vectorizer'
    tags = (NodeTag.thread,)

    def vectorize(self, feature_value: Input(SomeFeature)) -> int:
        return feature_value + 20


class SomeMLModel(NodeBase):
    name = 'some_model'

    async def predict(self, vec_value: Input(SomeVectorizer)):
        return (vec_value + 30) / 100


async def test_tags__with_thread_process(
    pipeline_context,
    build_dag,
    mocker,
):
    threads_get_pool_executor = mocker.spy(threads.PoolExecutorRegistry, 'get_pool_executor')
    processes_get_pool_executor = mocker.spy(processes.PoolExecutorRegistry, 'get_pool_executor')

    dag = build_dag(input_node=SomeInput, output_node=SomeMLModel)
    assert await dag.run(pipeline_context(base_num=10, other_num=5)) == 1.75

    assert threads_get_pool_executor.call_count == 2
    assert processes_get_pool_executor.call_count == 2
