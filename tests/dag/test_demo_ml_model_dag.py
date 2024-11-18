import typing as t

from ml_pipeline_engine.dag_builders.annotation.marks import Input
from ml_pipeline_engine.node import ProcessorBase
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


class SomeFeature(ProcessorBase):
    name = 'some_feature'

    def process(self, ds_value: Input(SomeDataSource), inp: Input(SomeInput)) -> int:
        return ds_value + inp['other_num'] + 10


class SomeVectorizer(ProcessorBase):
    name = 'some_vectorizer'

    def process(self, feature_value: Input(SomeFeature)) -> int:
        return feature_value + 20


class SomeMLModel(ProcessorBase):
    name = 'some_model'

    def process(self, vec_value: Input(SomeVectorizer)) -> float:
        return (vec_value + 30) / 100


async def test_demo_ml_model_dag(
    build_chart: t.Callable[..., PipelineChartLike],
) -> None:
    chart = build_chart(input_node=SomeInput, output_node=SomeMLModel)
    result = await chart.run(input_kwargs=dict(base_num=10, other_num=5))
    assert result.value == 1.75
