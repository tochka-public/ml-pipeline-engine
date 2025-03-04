import typing as t

from ml_pipeline_engine.dag_builders.annotation.marks import Input
from ml_pipeline_engine.dag_builders.annotation.marks import InputOneOf
from ml_pipeline_engine.node import ProcessorBase
from ml_pipeline_engine.types import PipelineChartLike


class StartNode(ProcessorBase):
    name = 'start_node'

    def process(self, base_num: int) -> int:
        return base_num


class FeatureWithError(ProcessorBase):
    name = 'feature_with_error'

    def process(self, _: Input(StartNode)) -> int:
        raise Exception


class FallbackDataSource(ProcessorBase):
    name = 'fallback_data_source'

    def process(self, inp: Input(StartNode)) -> int:
        return inp


class FallbackDataSourceWithError(ProcessorBase):
    name = 'fallback_data_source_with_error'

    def process(self, _: Input(StartNode)) -> int:
        raise Exception


class FallbackFeatureWithSecondLevelInputOneOf(ProcessorBase):
    name = 'fallback_feature_with_second_level_input_one_of'

    def process(self, ds_value: InputOneOf([FallbackDataSource, FallbackDataSourceWithError])) -> int:
        return ds_value + 100000000


class Summary(ProcessorBase):
    name = 'summary_with_first_level_input_one_of'

    def process(self, feature_value: InputOneOf([FeatureWithError, FallbackFeatureWithSecondLevelInputOneOf])) -> int:
        return feature_value + 1


class FinishNode(ProcessorBase):
    name = 'finish_node'

    def process(self, summary_value: Input(Summary)) -> int:
        return summary_value + 1


async def test_oneof_with_second_level_fallback_nested_oneof_success_dag(
    build_chart: t.Callable[..., PipelineChartLike],
) -> None:
    chart = build_chart(input_node=StartNode, output_node=FinishNode)
    res = await chart.run(input_kwargs=dict(base_num=1))

    assert res.value == 100000003
