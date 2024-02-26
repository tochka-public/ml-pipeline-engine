import typing as t

from ml_pipeline_engine.artifact_store.store.no_op import NoOpArtifactStore
from ml_pipeline_engine.cache import Cache
from ml_pipeline_engine.events import EventSourceMixin
from ml_pipeline_engine.module_loading import get_instance
from ml_pipeline_engine.node import generate_pipeline_id
from ml_pipeline_engine.types import (
    ArtifactStoreLike,
    CaseResult,
    EventManagerLike,
    ModelName,
    NodeId,
    PipelineChartLike,
    PipelineId,
)


class DAGPipelineContext(EventSourceMixin):
    """
    Контекст выполнения пайплайна ML-модели
    """

    def __init__(
        self,
        chart: PipelineChartLike,
        pipeline_id: PipelineId = None,
        input_kwargs: t.Dict[str, t.Any] = None,
        meta: t.Dict[str, t.Any] = None,
    ):
        self.chart = chart
        self.pipeline_id = pipeline_id if pipeline_id is not None else generate_pipeline_id()
        self.input_kwargs = input_kwargs if input_kwargs is not None else {}
        self.meta = meta if meta is not None else {}
        self.artifact_store: ArtifactStoreLike = get_instance(
            cls=self.chart.artifact_store or NoOpArtifactStore, ctx=self
        )

        self._local_cache = self.get_cache_object()
        self._event_managers = [get_instance(cls) for cls in self.chart.event_managers]
        self._case_results = {}
        self._nodes_in_run = set()
        self._active_recurrence_subgraph = {}

    @classmethod
    def get_cache_object(cls):
        return Cache()

    async def save_node_result(self, node_id: NodeId, data: t.Any) -> None:
        self._local_cache.save(node_id=node_id, data=data)
        await self.artifact_store.save(node_id=node_id, data=data)

    async def load_node_result(self, node_id: NodeId) -> t.Any:
        return self._local_cache.load(node_id=node_id)

    async def add_case_result(self, switch_node_id: NodeId, selection: CaseResult) -> None:
        self._case_results[switch_node_id] = selection

    async def get_case_result(self, switch_node_id: NodeId) -> CaseResult:
        return self._case_results[switch_node_id]

    async def add_node_in_run(self, node_id: NodeId) -> None:
        self._nodes_in_run.add(node_id)

    async def is_node_in_run(self, node_id: NodeId) -> bool:
        return node_id in self._nodes_in_run

    async def exists_node_result(self, node_id: NodeId) -> bool:
        return self._local_cache.exists(node_id)

    async def is_active_recurrence_subgraph(self, source: NodeId, dest: NodeId) -> bool:
        return (source, dest) in self._active_recurrence_subgraph

    async def set_active_recurrence_subgraph(self, source: NodeId, dest: NodeId) -> None:
        self._active_recurrence_subgraph[(source, dest)] = 1

    async def remove_recurrence_subgraph(self, source: NodeId, dest: NodeId) -> None:
        self._active_recurrence_subgraph.pop((source, dest))

    def delete_node_results(self, node_ids: t.Iterable[NodeId]) -> None:
        """
        Удаление всей информации, связанной с узлами
        """

        for node_id in node_ids:
            self._local_cache.remove(node_id)
            self._nodes_in_run.discard(node_id)

    @property
    def model_name(self) -> ModelName:
        return self.chart.model_name

    def _get_event_managers(self) -> t.List[EventManagerLike]:
        return self._event_managers

    def __repr__(self):
        return f'<{self.__class__.__name__} model_name="{self.chart.model_name}" pipeline_id="{self.pipeline_id}">'


def create_context_from_chart(
    chart: PipelineChartLike,
    input_kwargs: t.Dict[str, t.Any],
    pipeline_id: PipelineId = None,
    meta: t.Dict[str, t.Any] = None,
) -> DAGPipelineContext:
    """
    Создать контекст выполнения пайплайна ML-модели
    """

    return DAGPipelineContext(
        pipeline_id=pipeline_id, chart=chart, input_kwargs=input_kwargs, meta=meta,
    )
