"""
Пример с использованием Generic-нод, которые под собой хранят общее поведение и не зависимы от конкретной модели
"""

from ml_pipeline_engine.chart import PipelineChart
from ml_pipeline_engine.dag_builders.annotation import build_dag
from ml_pipeline_engine.dag_builders.annotation.marks import Input
from ml_pipeline_engine.dag_builders.annotation.marks import InputGeneric
from ml_pipeline_engine.node import build_node
from ml_pipeline_engine.parallelism import threads_pool_registry
from ml_pipeline_engine.types import NodeBase


class SomeInput(NodeBase):
    name = 'input'

    def process(self, base_num: int, other_num: int) -> dict:
        return {
            'base_num': base_num,
            'other_num': other_num,
        }


class SomeDataSource(NodeBase):
    name = 'some_data_source'

    def process(self, inp: Input(SomeInput)) -> int:
        return inp['base_num'] + 100


class SomeCommonFeature(NodeBase):
    name = 'some_feature'

    def process(self, ds_value: Input(SomeDataSource), inp: Input(SomeInput)) -> int:
        return ds_value + inp['other_num'] + 10


class GenericVectorizer(NodeBase):
    """
    Пример Generic-ноды
    """

    name = 'some_vectorizer'

    def process(self, feature_value: InputGeneric(NodeBase)) -> int:
        return feature_value + 20


class AnotherFeature(NodeBase):
    name = 'another_feature'

    def process(self, inp: Input(SomeInput)) -> int:
        return inp['base_num'] + 100_000


# Первый переопределенный подграф
SomeParticularVectorizer = build_node(
    GenericVectorizer,
    feature_value=Input(SomeCommonFeature),
)

# Второй переопределенный подграф
AnotherParticularVectorizer = build_node(
    GenericVectorizer,
    feature_value=Input(AnotherFeature),
)


class SomeMLModel(NodeBase):
    name = 'some_model'

    def process(self, vec_value: Input(SomeParticularVectorizer)) -> float:
        return (vec_value + 30) / 100


class AnotherMlModel(NodeBase):
    name = 'another_model'

    def process(self, vec_value: Input(AnotherParticularVectorizer)) -> float:
        return (vec_value + 30) / 100


async def main() -> None:
    threads_pool_registry.auto_init()

    first_pipeline = PipelineChart(
        'first_pipeline',
        build_dag(input_node=SomeInput, output_node=SomeMLModel),
    )

    second_pipeline = PipelineChart(
        'second_pipeline',
        build_dag(input_node=SomeInput, output_node=AnotherMlModel),
    )

    tasks = [
        first_pipeline.run(input_kwargs=dict(base_num=10, other_num=20)),
        second_pipeline.run(input_kwargs=dict(base_num=100, other_num=200)),
    ]
    await asyncio.gather(*tasks)


if __name__ == '__main__':
    import asyncio

    asyncio.run(main())
