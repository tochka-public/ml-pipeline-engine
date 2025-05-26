import typing as t

import pytest

from ml_pipeline_engine.artifact_store.enums import DataFormat
from ml_pipeline_engine.artifact_store.store.base import SerializedArtifactStore
from ml_pipeline_engine.dag_builders.annotation.marks import Input
from ml_pipeline_engine.dag_builders.annotation.marks import RecurrentSubGraph
from ml_pipeline_engine.node import ProcessorBase
from ml_pipeline_engine.node import RecurrentProcessor
from ml_pipeline_engine.types import AdditionalDataT
from ml_pipeline_engine.types import NodeId
from ml_pipeline_engine.types import NodeResultT
from ml_pipeline_engine.types import PipelineChartLike
from ml_pipeline_engine.types import PipelineContextLike
from ml_pipeline_engine.types import Recurrent


class TestArtifactStore(SerializedArtifactStore):
    def __init__(self, ctx: PipelineContextLike, store_log: t.List[NodeId]) -> None:
        super().__init__(ctx)
        self._log = store_log

    async def save(self, node_id: NodeId, data: t.Any, fmt: DataFormat = None) -> None:  # noqa: ARG002
        original_id = node_id
        iteration = 1
        while node_id in self._log:
            iteration += 1
            node_id = f'{original_id}__iter{iteration}'
        self._log.append(node_id)

    async def load(self, node_id: NodeId) -> NodeResultT:
        raise NotImplementedError


class InvertNumber(RecurrentProcessor):
    def process(
        self,
        num: float,
        additional_data: t.Optional[AdditionalDataT] = None,
    ) -> float:
        if additional_data is None:
            return num

        if additional_data == 5:
            return 5

        if additional_data == 7:
            return 11

        raise Exception


class JustPassNum(RecurrentProcessor):
    def process(self, num: Input(InvertNumber)) -> float:
        return num


class DoubleNumber(RecurrentProcessor):
    async def process(self, num: Input(JustPassNum)) -> t.Union[Recurrent, float]:
        if num == 3:
            return self.next_iteration(5)

        if num == 5:
            return self.next_iteration(7)

        if num == 11:
            return 11

        return num


recurrent_double_number = RecurrentSubGraph(
    start_node=InvertNumber,
    dest_node=DoubleNumber,
    max_iterations=3,
)


class JustANode(ProcessorBase):
    def process(self, num2: recurrent_double_number) -> float:
        return num2


async def test_dag(
    build_chart: t.Callable[..., PipelineChartLike],
    caplog_debug: pytest.LogCaptureFixture,
) -> None:
    store_log = []

    chart = build_chart(
        input_node=InvertNumber,
        output_node=JustANode,
        artifact_store=lambda ctx: TestArtifactStore(ctx, store_log),
    )
    result = await chart.run(input_kwargs=dict(num=3))
    assert result.value == 11

    for node_id in (
        'processor__tests_artifact_store_store_test_save_in_recurrent_InvertNumber',
        'processor__tests_artifact_store_store_test_save_in_recurrent_JustPassNum',
        'processor__tests_artifact_store_store_test_save_in_recurrent_DoubleNumber',
        'processor__tests_artifact_store_store_test_save_in_recurrent_InvertNumber__iter2',
        'processor__tests_artifact_store_store_test_save_in_recurrent_JustPassNum__iter2',
        'processor__tests_artifact_store_store_test_save_in_recurrent_DoubleNumber__iter2',
        'processor__tests_artifact_store_store_test_save_in_recurrent_InvertNumber__iter3',
        'processor__tests_artifact_store_store_test_save_in_recurrent_JustPassNum__iter3',
        'processor__tests_artifact_store_store_test_save_in_recurrent_DoubleNumber__iter3',
        'processor__tests_artifact_store_store_test_save_in_recurrent_JustANode',
    ):
        assert node_id in store_log
