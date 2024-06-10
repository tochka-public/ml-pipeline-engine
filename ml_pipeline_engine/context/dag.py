import typing as t

from ml_pipeline_engine.artifact_store.store.no_op import NoOpArtifactStore
from ml_pipeline_engine.events import EventSourceMixin
from ml_pipeline_engine.module_loading import get_instance
from ml_pipeline_engine.node import generate_pipeline_id
from ml_pipeline_engine.types import ArtifactStoreLike
from ml_pipeline_engine.types import EventManagerLike
from ml_pipeline_engine.types import ModelName
from ml_pipeline_engine.types import NodeId
from ml_pipeline_engine.types import PipelineChartLike
from ml_pipeline_engine.types import PipelineContextLike
from ml_pipeline_engine.types import PipelineId


class DAGPipelineContext(EventSourceMixin, PipelineContextLike):
    """
    Контекст выполнения пайплайна ML-модели
    """

    def __init__(
        self,
        chart: PipelineChartLike,
        pipeline_id: PipelineId = None,
        input_kwargs: t.Optional[t.Dict[str, t.Any]] = None,
        meta: t.Optional[t.Dict[str, t.Any]] = None,
    ) -> None:
        self.chart = chart
        self.pipeline_id = pipeline_id if pipeline_id is not None else generate_pipeline_id()
        self.input_kwargs = input_kwargs if input_kwargs is not None else {}
        self.meta = meta if meta is not None else {}
        self.artifact_store: ArtifactStoreLike = get_instance(
            cls=self.chart.artifact_store or NoOpArtifactStore, ctx=self,
        )

        self._event_managers = [get_instance(cls) for cls in self.chart.event_managers]

    async def save_node_result(self, node_id: NodeId, data: t.Any) -> None:
        await self.artifact_store.save(node_id=node_id, data=data)

    @property
    def model_name(self) -> ModelName:
        return self.chart.model_name

    def _get_event_managers(self) -> t.List[EventManagerLike]:
        return self._event_managers

    def __repr__(self) -> str:
        return f'<{self.__class__.__name__} model_name="{self.chart.model_name}" pipeline_id="{self.pipeline_id}">'


def create_context_from_chart(
    chart: PipelineChartLike,
    input_kwargs: t.Dict[str, t.Any],
    pipeline_id: PipelineId = None,
    meta: t.Optional[t.Dict[str, t.Any]] = None,
) -> DAGPipelineContext:
    """
    Создать контекст выполнения пайплайна ML-модели
    """

    return DAGPipelineContext(
        pipeline_id=pipeline_id, chart=chart, input_kwargs=input_kwargs, meta=meta,
    )
