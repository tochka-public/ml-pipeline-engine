import typing as t

from tests.helpers import FactoryMocker
from tests.helpers import call_object

from ml_pipeline_engine.dag_builders.annotation.marks import Input
from ml_pipeline_engine.dag_builders.annotation.marks import InputOneOf
from ml_pipeline_engine.node import ProcessorBase
from ml_pipeline_engine.types import PipelineChartLike

case_oneof_node_mocker = FactoryMocker()


class StartNode(ProcessorBase):
    name = 'start_node'

    @case_oneof_node_mocker
    def process(self, base_num: int) -> int:
        return base_num


class DataSource(ProcessorBase):
    name = 'data_source'

    @case_oneof_node_mocker
    def process(self, _: Input(StartNode)) -> int:
        return 1


class DataSourceWithError(ProcessorBase):
    name = 'data_source_with_error'

    @case_oneof_node_mocker
    def process(self, _: Input(StartNode)) -> int:
        raise Exception


class InnerFeatureWithThirdLevelInputOneOf(ProcessorBase):
    name = 'inner_feature_with_third_level_input_one_of'

    @case_oneof_node_mocker
    def process(self, ds_value: InputOneOf([DataSource, DataSourceWithError]), inp: Input(StartNode)) -> int:
        return ds_value + inp


class InnerFeatureWithError(ProcessorBase):
    name = 'inner_feature_with_error'

    @case_oneof_node_mocker
    def process(self, _: Input(StartNode)) -> int:
        raise Exception


class FeatureWithSecondLevelInputOneOf(ProcessorBase):
    name = 'feature_with_second_level_input_one_of'

    @case_oneof_node_mocker
    def process(
        self,
        ds_value: InputOneOf([InnerFeatureWithThirdLevelInputOneOf, InnerFeatureWithError]),
        inp: Input(StartNode),
    ) -> int:
        return ds_value + inp


class FallbackFeature(ProcessorBase):
    name = 'fallback_feature'

    @case_oneof_node_mocker
    def process(self) -> int:
        return 100000000


class Summary(ProcessorBase):
    name = 'summary_with_first_level_input_one_of'

    @case_oneof_node_mocker
    def process(self, feature_value: InputOneOf([FeatureWithSecondLevelInputOneOf, FallbackFeature])) -> int:
        return feature_value + 1


class FinishNode(ProcessorBase):
    name = 'finish_node'

    @case_oneof_node_mocker
    def process(self, summary_value: Input(Summary)) -> int:
        return summary_value + 1


async def test_oneof_with_third_level_nested_oneof_success_dag(
    build_chart: t.Callable[..., PipelineChartLike],
) -> None:
    case_oneof_node_mocker.mock.reset_mock()

    chart = build_chart(input_node=StartNode, output_node=FinishNode)
    result = await chart.run(input_kwargs=dict(base_num=1))

    assert result.value == 5

    assert case_oneof_node_mocker.mock.process.mock_calls == [
        call_object(base_num=1),
        call_object(_=1),
        call_object(ds_value=1, inp=1),
        call_object(ds_value=2, inp=1),
        call_object(feature_value=3),
        call_object(summary_value=4),
    ]

    assert case_oneof_node_mocker.mock.process.mock_calls[0].args[0].name == StartNode.name
    assert case_oneof_node_mocker.mock.process.mock_calls[1].args[0].name == DataSource.name
    assert case_oneof_node_mocker.mock.process.mock_calls[2].args[0].name == InnerFeatureWithThirdLevelInputOneOf.name
    assert case_oneof_node_mocker.mock.process.mock_calls[3].args[0].name == FeatureWithSecondLevelInputOneOf.name
    assert case_oneof_node_mocker.mock.process.mock_calls[4].args[0].name == Summary.name
    assert case_oneof_node_mocker.mock.process.mock_calls[5].args[0].name == FinishNode.name
