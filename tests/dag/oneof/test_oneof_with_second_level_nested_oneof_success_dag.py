import typing as t

from ml_pipeline_engine.dag_builders.annotation.marks import Input
from ml_pipeline_engine.dag_builders.annotation.marks import InputOneOf
from ml_pipeline_engine.node import ProcessorBase
from ml_pipeline_engine.types import PipelineChartLike


class StartNode(ProcessorBase):
    name = 'start_node'

    def process(self, base_num: int) -> int:
        return base_num


class DataSource(ProcessorBase):
    name = 'data_source'

    def process(self, _: Input(StartNode)) -> int:
        return 1


class DataSourceWithError(ProcessorBase):
    name = 'data_source_with_error'

    def process(self, _: Input(StartNode)) -> int:
        raise Exception


class FeatureWithSecondLevelInputOneOf(ProcessorBase):
    name = 'feature_with_second_level_input_one_of'

    def process(self, ds_value: InputOneOf([DataSource, DataSourceWithError]), inp: Input(StartNode)) -> int:
        return ds_value + inp


class FallbackFeature(ProcessorBase):
    name = 'fallback_feature'

    def process(self) -> int:
        return 100000000


class Summary(ProcessorBase):
    name = 'summary_with_first_level_input_one_of'

    def process(self, feature_value: InputOneOf([FeatureWithSecondLevelInputOneOf, FallbackFeature])) -> int:
        return feature_value + 1


class FinishNode(ProcessorBase):
    name = 'finish_node'

    def process(self, summary_value: Input(Summary)) -> int:
        return summary_value + 1


async def test_oneof_with_second_level_nested_oneof_success_dag(
    build_chart: t.Callable[..., PipelineChartLike],
) -> None:
    chart = build_chart(input_node=StartNode, output_node=FinishNode)
    result = await chart.run(input_kwargs=dict(base_num=1))

    assert result.value == 4
