import asyncio
import random
import typing as t

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


class ProxyNodeGeneric(ProcessorBase):
    async def process(self, state: GenericInput) -> t.Any:
        return state


class InputNode(ProxyNodeGeneric): ...


class ChainPrepare(ProcessorBase):
    async def process(self, state: GenericInput) -> t.Any:
        name = self.name.split('__')[-1]
        return {'value': state[name], 'path': self.name}


class BaseProducer(ProcessorBase):
    async def process(self, state: GenericInput, additional_data: t.Any = None) -> t.Any:
        state['path'] += f' > {self.name}'

        if additional_data:
            return additional_data

        return state


class ComponentCompiler(RecurrentProcessor):
    async def process(self, state: GenericInput) -> t.Union[Recurrent, str]:
        if state['value'] == 'fail':
            raise ValueError('fail')

        if state['value'].startswith('retry'):
            retry = int(state['value'].split(':')[-1])
            if retry > 0:
                await asyncio.sleep(random.random() / 10)
                state['value'] = f'retry:{retry - 1}'
                state['path'] += ' [retry]'
                return self.next_iteration(state)

        return state


class GatherOneOf(ProcessorBase):
    async def process(self, state: InputOneOf) -> t.Any:
        return state


class ReduceResults(ProcessorBase):
    tags = (NodeTag.non_async,)

    def process(self, **kwargs: GenericInput) -> t.Dict[str, t.Dict[str, str]]:
        return kwargs


def build_chain_nodes(chain_name: str) -> Input:
    """Build example recurrent subgraph."""
    prepare = build_node(ChainPrepare, node_name=f'prepare__{chain_name}', state=Input(InputNode))
    produce = build_node(BaseProducer, node_name=f'produce__{chain_name}', state=Input(prepare), additional_data=None)
    subgraph_dest = build_node(ComponentCompiler, node_name=f'subgraph_dest__{chain_name}', state=Input(produce))
    return RecurrentSubGraph(start_node=produce, dest_node=subgraph_dest, max_iterations=3)


def build_oneof_nodes(result: GenericInput, chain_name: str) -> Input:
    branch1 = build_node(ProxyNodeGeneric, node_name=f'branch1__{chain_name}', state=result)
    branch2 = build_node(ProxyNodeGeneric, node_name=f'branch2__{chain_name}', state=result)
    return Input(build_node(GatherOneOf, node_name=f'gather__{chain_name}', state=InputOneOf([branch1, branch2])))


async def test_oneof_many_recurrent_subgraph(build_chart: t.Callable[..., PipelineChartLike]) -> None:
    aggregated_input = build_node(
        ReduceResults,
        chain_1st=build_chain_nodes('1st'),
        chain_2nd=build_chain_nodes('2nd'),
        class_name='ReduceAsAggregatedResult',
        node_name='aggregate_input',
    )

    output_node = build_node(
        ReduceResults,
        oneof_1st=build_oneof_nodes(result=Input(aggregated_input), chain_name='oneof_1st'),
        oneof_2nd=build_oneof_nodes(result=Input(aggregated_input), chain_name='oneof_2nd'),
        node_name='aggregate_oneof',
    )

    chart = build_chart(input_node=InputNode, output_node=output_node)

    result = await chart.run(input_kwargs={'state': {'1st': 'ok', '2nd': 'retry:1'}})
    result.raise_on_error()

    assert result.value['oneof_1st'] == result.value['oneof_2nd']
    assert result.value['oneof_1st'] == {
        'chain_1st': {'path': 'prepare__1st > produce__1st', 'value': 'ok'},
        'chain_2nd': {'path': 'prepare__2nd > produce__2nd [retry] > produce__2nd', 'value': 'retry:0'},
    }
