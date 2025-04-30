import typing as t

import pytest

from ml_pipeline_engine.dag import OneOfDoesNotHaveResultError
from ml_pipeline_engine.dag_builders.annotation.marks import GenericInput
from ml_pipeline_engine.dag_builders.annotation.marks import Input
from ml_pipeline_engine.dag_builders.annotation.marks import InputOneOf
from ml_pipeline_engine.dag_builders.annotation.marks import RecurrentSubGraph
from ml_pipeline_engine.node import NodeTag
from ml_pipeline_engine.node import ProcessorBase
from ml_pipeline_engine.node import RecurrentProcessor
from ml_pipeline_engine.node import build_node
from ml_pipeline_engine.types import PipelineChartLike
from ml_pipeline_engine.types import Recurrent

pytest.test_retry_attempts = 0


class ProxyNodeGeneric(ProcessorBase):

    async def process(self, value: GenericInput) -> t.Any:
        return value


class InputNode(ProxyNodeGeneric):
    ...


class OutputNode(ProxyNodeGeneric):
    ...


class BaseProducer(ProcessorBase):
    async def process(self, value: GenericInput, **__: t.Any) -> t.Any:
        return value


class ComponentCompiler(RecurrentProcessor):
    async def process(self, value: GenericInput) -> t.Union[Recurrent, str]:
        name = self.name.split('__')[-1]
        expected_status: str = value[name]

        if expected_status == 'fail':
            raise ValueError('fail')

        if expected_status.startswith('retry'):
            retry = int(expected_status.split(':')[-1])
            if retry > 0:
                value[name] = f'retry:{retry - 1}'
                pytest.test_retry_attempts += 1
                return self.next_iteration(value)

        return 'ok'


class GatherOneOf(ProcessorBase):
    async def process(self, value: InputOneOf) -> t.Any:
        return value


class ReduceResults(ProcessorBase):
    tags = (NodeTag.non_async,)

    def process(self, **kwargs: GenericInput) -> t.List[str]:
        return list(kwargs.values())


def build_chain_nodes(chain_name: str) -> Input:
    """Build example recurrent subgraph."""
    prepare = build_node(ProxyNodeGeneric, node_name=f'prepare__{chain_name}', value=Input(InputNode))
    produce = build_node(BaseProducer, node_name=f'produce__{chain_name}', value=Input(prepare), additional_data=None)
    some_action = build_node(ProxyNodeGeneric, node_name=f'some_action__{chain_name}', value=Input(produce))
    subgraph_dest = build_node(ComponentCompiler, node_name=f'subgraph_dest__{chain_name}', value=Input(some_action))
    recurrent_subgraph = RecurrentSubGraph(start_node=produce, dest_node=subgraph_dest, max_iterations=2)
    return build_node(ProxyNodeGeneric, node_name=f'post__{chain_name}', value=recurrent_subgraph)


@pytest.mark.parametrize(
    'chart_input, attempts, error_type',
    (
        ({'1st': 'ok', '2nd': 'ok'}, 0, type(None)),
        ({'1st': 'retry:1', '2nd': 'retry:2'}, 3, type(None)),
        ({'1st': 'fail', '2nd': 'ok'}, 0, OneOfDoesNotHaveResultError),
        ({'1st': 'ok', '2nd': 'fail'}, 0, OneOfDoesNotHaveResultError),
        ({'1st': 'retry:3', '2nd': 'ok'}, 3, OneOfDoesNotHaveResultError),
    ),
)
async def test_oneof_recurrent_with_reduce(
        chart_input: dict,
        attempts: int,
        error_type: t.Type,
        build_chart: t.Callable[..., PipelineChartLike],
) -> None:
    pytest.test_retry_attempts = 0

    aggregated_result = build_node(
        ReduceResults,
        chain_1st=Input(build_chain_nodes('1st')),
        chain_2nd=Input(build_chain_nodes('2nd')),
        class_name='ReduceAsAggregatedResult',
        node_name='aggregate',
    )
    one_of_chains = build_node(GatherOneOf, value=InputOneOf([
        build_node(ProxyNodeGeneric, node_name='1st', value=Input(aggregated_result)),
        build_node(ProxyNodeGeneric, node_name='2nd', value=Input(aggregated_result)),
    ]))
    output_node = build_node(OutputNode, value=Input(one_of_chains))
    chart = build_chart(input_node=InputNode, output_node=output_node)
    result = await chart.run(input_kwargs={'value': chart_input})

    assert isinstance(result.error, error_type)
    assert pytest.test_retry_attempts == attempts
