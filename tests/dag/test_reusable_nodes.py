import typing as t

from ml_pipeline_engine.dag_builders.annotation.marks import Input
from ml_pipeline_engine.dag_builders.annotation.marks import InputGeneric
from ml_pipeline_engine.node import ProcessorBase
from ml_pipeline_engine.node import build_node
from ml_pipeline_engine.types import NodeBase
from ml_pipeline_engine.types import PipelineChartLike


class SomeInput(ProcessorBase):
    name = 'input'

    def process(self, base_num: int, other_num: int) -> dict:
        return {
            'base_num': base_num,
            'other_num': other_num,
        }


class SomeDataSource(ProcessorBase):
    name = 'some_data_source'

    def process(self, inp: Input(SomeInput)) -> int:
        return inp['base_num'] + 100


class SomeCommonFeature(ProcessorBase):
    name = 'some_feature'

    def process(self, ds_value: Input(SomeDataSource), inp: Input(SomeInput)) -> int:
        return ds_value + inp['other_num'] + 10


# Пример Generic-ноды
class GenericVectorizer(ProcessorBase):
    name = 'some_vectorizer'

    async def process(self, feature_value: InputGeneric(NodeBase), const: int) -> int:
        return feature_value + 20 + const


class AnotherFeature(ProcessorBase):
    name = 'another_feature'

    def process(self, inp: Input(SomeInput)) -> int:
        return inp['base_num'] + 100_000


# Первый переопределенный подграф
SomeParticularVectorizer = build_node(
    GenericVectorizer,
    feature_value=Input(SomeCommonFeature),
    dependencies_default=dict(
        const=1,
    ),
)

# Второй переопределенный подграф
AnotherParticularVectorizer = build_node(
    GenericVectorizer,
    feature_value=Input(AnotherFeature),
    dependencies_default=dict(
        const=0,
    ),
)


class SomeMLModel(ProcessorBase):
    name = 'some_model'

    def process(self, vec_value: Input(SomeParticularVectorizer)) -> float:
        return (vec_value + 30) / 100


class AnotherMlModel(ProcessorBase):
    name = 'another_model'

    def process(self, vec_value: Input(AnotherParticularVectorizer)) -> float:
        return (vec_value + 30) / 100


async def test_reusable_nodes(
    build_chart: t.Callable[..., PipelineChartLike],
) -> None:

    # Проверяем корректность первого графа
    chart = build_chart(input_node=SomeInput, output_node=SomeMLModel)
    result = await chart.run(input_kwargs=dict(base_num=10, other_num=5))
    assert result.value == 1.76

    # Проверяем корректность второго графа
    chart = build_chart(input_node=SomeInput, output_node=AnotherMlModel)
    result = await chart.run(input_kwargs=dict(base_num=10, other_num=5))
    assert result.value == 1_000.6
