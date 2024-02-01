from typing import Type

import pytest

from ml_pipeline_engine.base_nodes.datasources import DataSource
from ml_pipeline_engine.dag_builders.annotation.marks import Input, InputOneOf
from ml_pipeline_engine.decorators import guard_datasource_error
from ml_pipeline_engine.types import NodeBase


class SomeInput(NodeBase):
    name = 'input'

    def process(self, base_num: int, other_num: int) -> dict:
        return {
            'base_num': base_num,
            'other_num': other_num,
        }


class ErrorDataSource(DataSource):
    name = 'some_data_source'
    title = 'SomeDataSource'

    @guard_datasource_error()
    def collect(self, inp: Input(SomeInput)):
        raise Exception


class ErrorDataSourceSecond(DataSource):
    name = 'some_data_source_second'
    title = 'SomeDataSource'

    @guard_datasource_error()
    def collect(self, inp: Input(SomeInput)):
        raise Exception


class SomeFeature(NodeBase):
    name = 'some_feature'

    def extract(self, ds_value: Input(ErrorDataSource), inp: Input(SomeInput)) -> int:
        return ds_value + inp['other_num'] + 10


class SomeFeatureSecond(NodeBase):
    name = 'some_feature_second'

    def extract(self, ds_value: Input(ErrorDataSourceSecond), inp: Input(SomeInput)) -> int:
        return ds_value + inp['other_num'] + 10


class FallbackFeature(NodeBase):
    name = 'fallback_feature'

    def extract(self) -> Type[Exception]:
        return Exception


class SomeVectorizer(NodeBase):
    name = 'some_vectorizer'

    def vectorize(self, feature_value: InputOneOf([SomeFeature, SomeFeatureSecond, FallbackFeature])) -> int:
        return feature_value + 20


class SomeMLModel(NodeBase):
    name = 'some_model'

    def predict(self, vec_value: Input(SomeVectorizer)):
        return (vec_value + 30) / 100


async def test_input_one_of_fails_dag(pipeline_context, build_dag):

    with pytest.raises(TypeError):
        await build_dag(input_node=SomeInput, output_node=SomeMLModel).run(pipeline_context(base_num=10, other_num=5))


@pytest.mark.skip('Мультипроцессинг временно не поддерживается')
def test_input_one_of_fails_dag_multiprocess(pipeline_multiprocess_context, build_multiprocess_dag):

    with pytest.raises(TypeError):
        dag = build_multiprocess_dag(input_node=SomeInput, output_node=SomeMLModel)
        dag.run(pipeline_multiprocess_context(base_num=10, other_num=5))
