import typing as t

from ml_pipeline_engine.context.dag import DAGPipelineContext
from ml_pipeline_engine.dag_builders.annotation.marks import Input
from ml_pipeline_engine.dag_builders.annotation.marks import InputGeneric
from ml_pipeline_engine.node import build_node
from ml_pipeline_engine.types import DAGLike
from ml_pipeline_engine.types import NodeBase
from ml_pipeline_engine.types import NodeLike


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


class SomeMLModel(NodeBase):
    name = 'some_model'

    def predict(self, vec_value: Input(SomeParticularVectorizer)) -> float:
        return (vec_value + 30) / 100


class AnotherMlModel(NodeBase):
    name = 'another_model'

    def predict(self, vec_value: Input(AnotherParticularVectorizer)) -> float:
        return (vec_value + 30) / 100


async def test_reusable_nodes(
    pipeline_context: t.Callable[..., DAGPipelineContext],
    build_dag: t.Callable[..., DAGLike],
) -> None:

    # Проверяем корректность первого графа
    some_dag = build_dag(input_node=SomeInput, output_node=SomeMLModel)
    assert await some_dag.run(pipeline_context(base_num=10, other_num=5)) == 1.76

    # Проверяем корректность второго графа
    some_dag = build_dag(input_node=SomeInput, output_node=AnotherMlModel)
    assert await some_dag.run(pipeline_context(base_num=10, other_num=5)) == 1_000.6
