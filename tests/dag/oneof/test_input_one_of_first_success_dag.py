import typing as t

from ml_pipeline_engine.context.dag import DAGPipelineContext
from ml_pipeline_engine.dag_builders.annotation.marks import Input
from ml_pipeline_engine.dag_builders.annotation.marks import InputOneOf
from ml_pipeline_engine.decorators import guard_datasource_error
from ml_pipeline_engine.node.base_nodes import ProcessorBase
from ml_pipeline_engine.types import DAGLike


class SomeInput(ProcessorBase):
    name = 'input'

    def process(self, base_num: int, other_num: int) -> dict:
        return {
            'base_num': base_num,
            'other_num': other_num,
        }


class SomeDataSource(ProcessorBase):
    name = 'some_data_source'

    def process(self, _: Input(SomeInput)) -> int:
        return 110


class ErrorDataSource(ProcessorBase):
    name = 'some_data_source_second'

    @guard_datasource_error()
    def process(self, _: Input(SomeInput)) -> int:
        raise Exception


class SomeFeature(ProcessorBase):
    name = 'some_feature'

    def process(self, ds_value: Input(SomeDataSource), inp: Input(SomeInput)) -> int:
        return ds_value + inp['other_num'] + 10


class SomeFeatureSecond(ProcessorBase):
    name = 'some_feature_second'

    def process(self, ds_value: Input(ErrorDataSource), inp: Input(SomeInput)) -> int:
        return ds_value + inp['other_num'] + 10


class FallbackFeature(ProcessorBase):
    name = 'fallback_feature'

    def process(self) -> int:
        return 125


class SomeVectorizer(ProcessorBase):
    name = 'some_vectorizer'

    def process(self, feature_value: InputOneOf([SomeFeature, SomeFeatureSecond, FallbackFeature])) -> int:
        return feature_value + 20


class NoneVectorizer(ProcessorBase):
    name = 'none_vectorizer'

    def process(self) -> None:
        return None


class SomeMLModel(ProcessorBase):
    name = 'some_model'

    def process(self, vec_value: Input(SomeVectorizer), none_value: Input(NoneVectorizer)) -> float:
        assert none_value is None
        return (vec_value + 30) / 100


async def test_input_one_of_first_success_dag(
    pipeline_context: t.Callable[..., DAGPipelineContext],
    build_dag: t.Callable[..., DAGLike],
) -> None:
    dag = build_dag(input_node=SomeInput, output_node=SomeMLModel)
    assert await dag.run(pipeline_context(base_num=10, other_num=5)) == 1.75
