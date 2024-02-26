import pytest

from ml_pipeline_engine.dag_builders.annotation.marks import Input, InputGeneric
from ml_pipeline_engine.node import build_node
from ml_pipeline_engine.types import NodeBase, NodeLike


class SomeInput(NodeBase):
    name = 'input'

    def process(self, base_num: int, other_num: int) -> dict:
        return {
            'base_num': base_num,
            'other_num': other_num,
        }


class SomeDataSource(NodeBase):
    name = 'some_data_source'

    def collect(self, inp: Input(SomeInput)) -> int:
        return inp['base_num'] + 100


class SomeCommonFeature(NodeBase):
    name = 'some_feature'

    def extract(self, ds_value: Input(SomeDataSource), inp: Input(SomeInput)) -> int:
        return ds_value + inp['other_num'] + 10


# Пример Generic-ноды
class GenericVectorizer(NodeBase):
    name = 'some_vectorizer'

    async def vectorize(self, feature_value: InputGeneric(NodeLike), const: int) -> int:
        return feature_value + 20 + const


class AnotherFeature(NodeBase):
    name = 'another_feature'

    def extract(self, inp: Input(SomeInput)) -> int:
        return inp['base_num'] + 100_000


# Первый переопределенный подграф
SomeParticularVectorizer = build_node(  # noqa
    GenericVectorizer,
    feature_value=Input(SomeCommonFeature),
    dependencies_default=dict(
        const=1,
    ),
)

# Второй переопределенный подграф
AnotherParticularVectorizer = build_node(  # noqa
    GenericVectorizer,
    feature_value=Input(AnotherFeature),
    dependencies_default=dict(
        const=0,
    ),
)


class SomeMLModel(NodeBase):
    name = 'some_model'

    def predict(self, vec_value: Input(SomeParticularVectorizer)):
        return (vec_value + 30) / 100


class AnotherMlModel(NodeBase):
    name = 'another_model'

    def predict(self, vec_value: Input(AnotherParticularVectorizer)):
        return (vec_value + 30) / 100


async def test_reusable_nodes(build_dag, pipeline_context):

    # Проверяем корректность первого графа
    some_dag = build_dag(input_node=SomeInput, output_node=SomeMLModel)
    assert await some_dag.run(pipeline_context(base_num=10, other_num=5)) == 1.76

    # Проверяем корректность второго графа
    some_dag = build_dag(input_node=SomeInput, output_node=AnotherMlModel)
    assert await some_dag.run(pipeline_context(base_num=10, other_num=5)) == 1_000.6


@pytest.mark.skip('Мультипроцессинг временно не поддерживается')
def test_reusable_nodes_multiprocess(pipeline_multiprocess_context, build_multiprocess_dag):

    # Проверяем корректность первого графа
    some_dag = build_multiprocess_dag(input_node=SomeInput, output_node=SomeMLModel)
    assert some_dag.run(pipeline_multiprocess_context(base_num=10, other_num=5)) == 1.75

    # Проверяем корректность второго графа
    some_dag = build_multiprocess_dag(input_node=SomeInput, output_node=AnotherMlModel)
    assert some_dag.run(pipeline_multiprocess_context(base_num=10, other_num=5)) == 1_000.6
