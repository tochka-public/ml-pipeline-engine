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


class FeatureWithError(ProcessorBase):
    name = 'feature_with_error'

    @case_oneof_node_mocker
    def process(self, _: Input(StartNode)) -> int:
        raise Exception


class FallbackDataSource(ProcessorBase):
    name = 'fallback_data_source'

    @case_oneof_node_mocker
    def process(self, inp: Input(StartNode)) -> int:
        return inp


class FallbackDataSourceWithError(ProcessorBase):
    name = 'fallback_data_source_with_error'

    @case_oneof_node_mocker
    def process(self, _: Input(StartNode)) -> int:
        raise Exception


class FallbackFeatureWithSecondLevelInputOneOf(ProcessorBase):
    name = 'fallback_feature_with_second_level_input_one_of'

    @case_oneof_node_mocker
    def process(self, ds_value: InputOneOf([FallbackDataSource, FallbackDataSourceWithError])) -> int:
        return ds_value + 100000000


class Summary(ProcessorBase):
    name = 'summary_with_first_level_input_one_of'

    @case_oneof_node_mocker
    def process(self, feature_value: InputOneOf([FeatureWithError, FallbackFeatureWithSecondLevelInputOneOf])) -> int:
        return feature_value + 1


class FinishNode(ProcessorBase):
    name = 'finish_node'

    @case_oneof_node_mocker
    def process(self, summary_value: Input(Summary)) -> int:
        return summary_value + 1


async def test_oneof_with_second_level_fallback_nested_oneof_success_dag(
    build_chart: t.Callable[..., PipelineChartLike],
) -> None:
    case_oneof_node_mocker.mock.reset_mock()

    chart = build_chart(input_node=StartNode, output_node=FinishNode)
    res = await chart.run(input_kwargs=dict(base_num=1))

    assert res.value == 100000003

    assert case_oneof_node_mocker.mock.process.mock_calls == [
        call_object(base_num=1),
        call_object(_=1),
        call_object(inp=1),
        call_object(ds_value=1),
        call_object(feature_value=100000001),
        call_object(summary_value=100000002),
    ]

    assert case_oneof_node_mocker.mock.process.mock_calls[0].args[0].name == StartNode.name
    assert case_oneof_node_mocker.mock.process.mock_calls[1].args[0].name == FeatureWithError.name
    assert case_oneof_node_mocker.mock.process.mock_calls[2].args[0].name == FallbackDataSource.name
    assert (
        case_oneof_node_mocker.mock.process.mock_calls[3].args[0].name == FallbackFeatureWithSecondLevelInputOneOf.name
    )
    assert case_oneof_node_mocker.mock.process.mock_calls[4].args[0].name == Summary.name
    assert case_oneof_node_mocker.mock.process.mock_calls[5].args[0].name == FinishNode.name
