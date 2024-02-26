from ml_pipeline_engine.dag_builders.annotation.marks import Input
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

    def collect(self, inp: Input(SomeInput)) -> int:
        return inp['base_num'] + 100


class SomeFeature(NodeBase):
    name = 'some_feature'

    def extract(self, ds_value: Input(SomeDataSource), inp: Input(SomeInput)) -> int:
        return ds_value + inp['other_num'] + 10


class SomeVectorizer(NodeBase):
    name = 'some_vectorizer'

    def vectorize(self, feature_value: Input(SomeFeature)) -> int:
        return feature_value + 20


class SomeMLModel(NodeBase):
    name = 'some_model'

    def predict(self, vec_value: Input(SomeVectorizer)):
        return (vec_value + 30) / 100


async def test_demo_ml_model_dag(pipeline_context, build_dag):
    dag = build_dag(input_node=SomeInput, output_node=SomeMLModel)
    assert await dag.run(pipeline_context(base_num=10, other_num=5)) == 1.75
