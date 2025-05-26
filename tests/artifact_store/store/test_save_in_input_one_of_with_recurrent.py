import typing as t

from ml_pipeline_engine.artifact_store.enums import DataFormat
from ml_pipeline_engine.artifact_store.store.base import SerializedArtifactStore
from ml_pipeline_engine.dag_builders.annotation.marks import GenericInput
from ml_pipeline_engine.dag_builders.annotation.marks import Input
from ml_pipeline_engine.dag_builders.annotation.marks import InputOneOf
from ml_pipeline_engine.dag_builders.annotation.marks import RecurrentSubGraph
from ml_pipeline_engine.node import NodeTag
from ml_pipeline_engine.node import ProcessorBase
from ml_pipeline_engine.node import RecurrentProcessor
from ml_pipeline_engine.node import build_node
from ml_pipeline_engine.types import NodeBase
from ml_pipeline_engine.types import NodeId
from ml_pipeline_engine.types import NodeResultT
from ml_pipeline_engine.types import PipelineChartLike
from ml_pipeline_engine.types import PipelineContextLike


class TestArtifactStore(SerializedArtifactStore):
    def __init__(self, ctx: PipelineContextLike, store_log: t.List[NodeId]) -> None:
        super().__init__(ctx)
        self._log = store_log

    async def save(self, node_id: NodeId, data: t.Any, fmt: DataFormat = None) -> None:  # noqa: ARG002
        if node_id in self._log:
            raise Exception('Should not write duplicates')
        self._log.append(node_id)

    async def load(self, node_id: NodeId) -> NodeResultT:
        raise NotImplementedError


class ProxyNodeGeneric(ProcessorBase):
    async def process(self, num: GenericInput) -> t.Any:
        return num


class InputNode(ProxyNodeGeneric): ...


class BaseProducer(ProcessorBase):
    async def process(self, state: GenericInput, additional_data: t.Any = None) -> t.Any:  # noqa: ARG002
        return state


class RecurrentNode(RecurrentProcessor):
    async def process(self, state: GenericInput) -> t.Any:
        return state


class GatherOneOf(ProcessorBase):
    async def process(self, result: InputOneOf) -> t.Any:
        return result


class ReduceResults(ProcessorBase):
    tags = (NodeTag.non_async,)

    def process(self, **kwargs: GenericInput) -> t.Dict[str, t.Dict[str, str]]:
        return kwargs


def build_recurrent_subgraph(chain_name: str) -> Input:
    produce = build_node(BaseProducer, node_name=f'produce__{chain_name}', state=Input(InputNode), additional_data=None)
    subgraph_dest = build_node(RecurrentNode, node_name=f'subgraph_dest__{chain_name}', state=Input(produce))
    return RecurrentSubGraph(start_node=produce, dest_node=subgraph_dest, max_iterations=3)


def build_one_of_nodes(result: GenericInput, chain_name: str) -> Input:
    return Input(
        build_node(
            GatherOneOf,
            node_name=f'gather__{chain_name}',
            result=InputOneOf([
                build_node(ProxyNodeGeneric, node_name=f'branch1__{chain_name}', num=result),
                build_node(ProxyNodeGeneric, node_name=f'branch2__{chain_name}', num=result),
            ]),
        ),
    )


def build_output_node() -> t.Type[NodeBase]:
    aggregated_input = build_node(
        ReduceResults,
        chain_1st=build_recurrent_subgraph('1st'),
        chain_2nd=build_recurrent_subgraph('2nd'),
        class_name='ReduceAsAggregatedResult',
        node_name='aggregate_input',
    )

    return build_node(
        ReduceResults,
        oneof_1st=build_one_of_nodes(result=Input(aggregated_input), chain_name='oneof_1st'),
        oneof_2nd=build_one_of_nodes(result=Input(aggregated_input), chain_name='oneof_2nd'),
        node_name='aggregate_oneof',
    )


async def test_save_in_input_one_of(
    build_chart: t.Callable[..., PipelineChartLike],
) -> None:
    store_log = []

    chart = build_chart(
        input_node=InputNode,
        output_node=build_output_node(),
        artifact_store=lambda ctx: TestArtifactStore(ctx, store_log),
    )
    await chart.run(input_kwargs=dict(num=1))

    for node_id in (
        'processor__tests_artifact_store_store_test_save_in_input_one_of_with_recurrent_InputNode',
        'processor__produce__1st',
        'processor__produce__2nd',
        'processor__subgraph_dest__1st',
        'processor__subgraph_dest__2nd',
        'processor__aggregate_input',
        'processor__branch1__oneof_1st',
        'processor__branch1__oneof_2nd',
        'processor__gather__oneof_1st',
        'processor__gather__oneof_2nd',
        'processor__aggregate_oneof',
    ):
        assert node_id in store_log
