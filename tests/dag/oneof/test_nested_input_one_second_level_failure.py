import typing as t

from ml_pipeline_engine.dag_builders.annotation.marks import Input
from ml_pipeline_engine.dag_builders.annotation.marks import InputOneOf
from ml_pipeline_engine.node import ProcessorBase
from ml_pipeline_engine.types import PipelineChartLike
from ml_pipeline_engine.node import ProcessorBase
from ml_pipeline_engine.dag import OneOfDoesNotHaveResultError


class SomeInput(ProcessorBase):
    name = 'input'

    def process(self, base_num: int, other_num: int) -> dict:
        return {
            'base_num': base_num,
            'other_num': other_num,
        }

class SomeFeature0(ProcessorBase):
    name = 'some_feature0'

    async def process(self, ds_value: Input(SomeInput)) -> int:
        return ds_value

class FirstDataSource(ProcessorBase):
    name = 'some_data_source'

    def process(self, _: Input(SomeInput), inp: Input(SomeFeature0)) -> int:
        raise Exception
        
class SecondDataSource(ProcessorBase):
    name = 'some_data_source_second'

    def process(self, _: Input(SomeInput)) -> int:
        raise Exception

class SomeFeature(ProcessorBase):
    name = 'some_feature'

    def process(self, ds_value: InputOneOf([FirstDataSource, SecondDataSource]), inp: Input(SomeInput)) -> int:
        return ds_value

class SomeFeature2(ProcessorBase):
    name = 'some_feature2'

    async def process(self, ds_value: Input(SomeFeature)) -> int:
        return ds_value

class FallbackFeature(ProcessorBase):
    name = 'fallback_feature'

    def process(self) -> int:
        raise Exception

class SomeVectorizer(ProcessorBase):
    name = 'vectorizer'

    def process(self, feature_value: InputOneOf([SomeFeature, FallbackFeature])) -> int:
        return feature_value + 20

class SomeMLModel(ProcessorBase):
    name = 'some_model'

    def process(self, vec_value: Input(SomeVectorizer)) -> float:
        return (vec_value + 30) / 100
    

async def test_nested_input_one_of_second_level_failure_dag(
    build_chart: t.Callable[..., PipelineChartLike],
) -> None:
    chart = build_chart(input_node=SomeInput, output_node=SomeMLModel)
    result = await chart.run(input_kwargs=dict(base_num=10, other_num=5))

    assert result.error.__class__ == OneOfDoesNotHaveResultError

