import typing as t

from ml_pipeline_engine.context.dag import DAGPipelineContext
from ml_pipeline_engine.dag_builders.annotation.marks import GenericInput
from ml_pipeline_engine.dag_builders.annotation.marks import Input
from ml_pipeline_engine.dag_builders.annotation.marks import InputOneOf
from ml_pipeline_engine.dag_builders.annotation.marks import RecurrentSubGraph
from ml_pipeline_engine.node import ProcessorBase
from ml_pipeline_engine.node import RecurrentProcessor
from ml_pipeline_engine.node import build_node
from ml_pipeline_engine.node.enums import NodeTag
from ml_pipeline_engine.types import DAGLike
from ml_pipeline_engine.types import Recurrent


class MainProducer(ProcessorBase):
    tags = (NodeTag.non_async,)

    def process(self, num: int) -> int:
        return num


class Producer(ProcessorBase):
    tags = (NodeTag.non_async,)

    def process(self, num: Input(MainProducer), additional_data: t.Any = None) -> t.Any:
        if additional_data:
            return additional_data + 1

        return num


class ProcessLikeFactory(RecurrentProcessor):
    name = 'like_factory'
    tags = (NodeTag.non_async,)
    kind = None

    def process(self, something: GenericInput(t.Type[ProcessorBase])) -> t.Union[Recurrent, int]:

        if self.kind is None:
            raise Exception

        if self.kind == 'first':
            return self.next_iteration(100_000)

        if self.kind == 'second':
            return self.next_iteration(200_000)

        return something + 3


class ProxyNodeGeneric(ProcessorBase):
    tags = (NodeTag.non_async,)

    def process(self, arg: GenericInput(t.Type[ProcessorBase])) -> t.Any:
        return arg


def create_proxy_recurrent_node(name_prefix: str) -> t.Type[ProxyNodeGeneric]:
    """
    Shortcut to create a subgraph
    """

    process_node = build_node(
        ProcessLikeFactory,
        node_name=f'{name_prefix}__{ProcessLikeFactory.name}',
        something=Input(Producer),
        attrs=dict(
            kind=name_prefix,
        ),
    )

    rec_process = RecurrentSubGraph(
        start_node=Producer,
        dest_node=process_node,
        max_iterations=3,
    )

    a_node = build_node(
        ProxyNodeGeneric,
        node_name=f'rec_proxy__{process_node.name}',
        arg=rec_process,
    )

    return build_node(
        ProxyNodeGeneric,
        node_name=f'another_level_rec_proxy__{process_node.name}',
        arg=Input(a_node),
    )


proxy_first_process = create_proxy_recurrent_node('first')
proxy_second_process = create_proxy_recurrent_node('second')
proxy_third_process = create_proxy_recurrent_node('third')


class JustAnotherNode(ProcessorBase):
    tags = (NodeTag.non_async,)

    def process(self, third_process: Input(proxy_third_process)) -> int:
        return third_process + 1


class OneOfGathering(ProcessorBase):
    tags = (NodeTag.non_async,)

    def process(self, arg: InputOneOf([t.Type[ProcessorBase]])) -> t.Any:
        return arg


oneof_gathering = build_node(
    OneOfGathering,
    node_name='proxy_oneof',
    arg=InputOneOf([proxy_first_process, proxy_second_process, JustAnotherNode]),
)


class Result(ProcessorBase):
    tags = (NodeTag.non_async,)

    def process(self, arg: Input(oneof_gathering)) -> t.Any:
        return arg


async def test_dag(
        pipeline_context: t.Callable[..., DAGPipelineContext],
        build_dag: t.Callable[..., DAGLike],
) -> None:
    dag = build_dag(input_node=MainProducer, output_node=Result)
    assert await dag.run(pipeline_context(num=3)) == 200_005
