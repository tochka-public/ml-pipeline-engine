from ml_pipeline_engine.base_nodes.datasources import DataSource
from ml_pipeline_engine.dag_builders.annotation.marks import Input
from ml_pipeline_engine.dag_builders.annotation.marks import InputOneOf
from ml_pipeline_engine.decorators import guard_datasource_error
from ml_pipeline_engine.types import NodeBase


class SomeInput(NodeBase):
    name = 'input'

    def process(self, base_num: int, other_num: int) -> dict:
        return {
            'base_num': base_num,
            'other_num': other_num,
        }


class SomeDataSource(DataSource):
    name = 'some_data_source'
    title = 'SomeDataSource'

    def collect(self, inp: Input(SomeInput)) -> int:
        return 110


class ErrorDataSource(DataSource):
    name = 'some_data_source_second'
    title = 'SomeDataSource'

    @guard_datasource_error()
    def collect(self, inp: Input(SomeInput)) -> int:
        raise Exception


class SomeFeature(NodeBase):
    name = 'some_feature'

    def extract(self, ds_value: Input(SomeDataSource), inp: Input(SomeInput)) -> int:
        return ds_value + inp['other_num'] + 10


class SomeFeatureSecond(NodeBase):
    name = 'some_feature_second'

    def extract(self, ds_value: Input(ErrorDataSource), inp: Input(SomeInput)) -> int:
        return ds_value + inp['other_num'] + 10


class FallbackFeature(NodeBase):
    name = 'fallback_feature'

    def extract(self) -> int:
        return 125


class SomeVectorizer(NodeBase):
    name = 'some_vectorizer'

    def vectorize(self, feature_value: InputOneOf([SomeFeature, SomeFeatureSecond, FallbackFeature])) -> int:
        return feature_value + 20


class NoneVectorizer(NodeBase):
    name = 'none_vectorizer'

    def vectorize(self) -> None:
        return None


class SomeMLModel(NodeBase):
    name = 'some_model'

    def predict(self, vec_value: Input(SomeVectorizer), none_value: Input(NoneVectorizer)):
        assert none_value is None
        return (vec_value + 30) / 100


async def test_input_one_of_first_success_dag(pipeline_context, build_dag):
    dag = build_dag(input_node=SomeInput, output_node=SomeMLModel)
    assert await dag.run(pipeline_context(base_num=10, other_num=5)) == 1.75
