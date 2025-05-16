import typing as t

from ml_pipeline_engine.artifact_store.enums import DataFormat
from ml_pipeline_engine.artifact_store.store.base import SerializedArtifactStore
from ml_pipeline_engine.dag_builders.annotation.marks import Input
from ml_pipeline_engine.node import ProcessorBase
from ml_pipeline_engine.node import get_node_id
from ml_pipeline_engine.node.enums import NodeTag
from ml_pipeline_engine.types import NodeId
from ml_pipeline_engine.types import NodeResultT
from ml_pipeline_engine.types import PipelineChartLike
from ml_pipeline_engine.types import PipelineContextLike


class TestArtifactStore(SerializedArtifactStore):
    def __init__(self, ctx: PipelineContextLike, store_log: t.List[NodeId]) -> None:
        super().__init__(ctx)
        self._log = store_log

    async def save(self, node_id: NodeId, data: t.Any, fmt: DataFormat = None) -> None:  # noqa: ARG002
        self._log.append(node_id)

    async def load(self, node_id: NodeId) -> NodeResultT:
        raise NotImplementedError


class InvertNumber(ProcessorBase):
    tags = (NodeTag.skip_store,)

    def process(self, num: float) -> float:
        return -num


class AddConst(ProcessorBase):
    tags = (NodeTag.non_async,)

    def process(self, num: Input(InvertNumber), const: float = 0.1) -> float:
        return num + const


class DoubleNumber(ProcessorBase):
    tags = (NodeTag.non_async,)

    def process(self, num: Input(AddConst)) -> float:
        return num * 2


async def test_skip_tag_check(
    build_chart: t.Callable[..., PipelineChartLike],
) -> None:
    store_log = []

    chart = build_chart(
        input_node=InvertNumber,
        output_node=DoubleNumber,
        artifact_store=lambda ctx: TestArtifactStore(ctx, store_log),
    )
    result = await chart.run(input_kwargs=dict(num=2.5))

    assert result.value == -4.8
    assert result.error is None

    for node in (
        AddConst,
        DoubleNumber,
    ):
        assert get_node_id(node) in store_log

    assert get_node_id(InvertNumber) not in store_log
