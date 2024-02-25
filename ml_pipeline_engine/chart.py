import typing as t
from dataclasses import dataclass, field

from ml_pipeline_engine.context import dag as dag_ctx
from ml_pipeline_engine.node import generate_pipeline_id
from ml_pipeline_engine.types import (
    ArtifactStoreLike,
    DAGLike,
    EventManagerLike,
    ModelName,
    NodeLike,
    PipelineId,
    PipelineResult,
)

NodeResultT = t.TypeVar('NodeResultT')

Entrypoint = t.Optional[t.Union[NodeLike[NodeResultT], DAGLike[NodeResultT]]]


@dataclass(frozen=True, repr=False)
class PipelineChartBase:
    """
    Базовый класс определения пайплайна ML-модели
    """

    model_name: ModelName
    entrypoint: Entrypoint
    artifact_store: t.Optional[t.Type[ArtifactStoreLike]] = None
    event_managers: t.List[t.Type[EventManagerLike]] = field(default_factory=list)


@dataclass(frozen=True, repr=False)
class PipelineChart(PipelineChartBase):
    """
    Основная реализация определения пайплайна ML-модели
    """

    async def run(
        self,
        pipeline_id: t.Optional[PipelineId] = None,
        input_kwargs: t.Optional[t.Dict[str, t.Any]] = None,
        meta: t.Optional[t.Dict[str, t.Any]] = None,
    ) -> PipelineResult[NodeResultT]:
        input_kwargs = input_kwargs if input_kwargs is not None else {}
        pipeline_id = pipeline_id if pipeline_id is not None else generate_pipeline_id()

        ctx = dag_ctx.create_context_from_chart(
            chart=self,
            pipeline_id=pipeline_id,
            input_kwargs=input_kwargs,
            meta=meta if meta is not None else {},
        )

        await ctx.emit_on_pipeline_start()

        try:
            result = PipelineResult(
                value=await self.entrypoint.run(ctx),
                pipeline_id=pipeline_id,
                error=None,
            )

            await ctx.emit_on_pipeline_complete(result=result)
            return result

        except Exception as ex:
            result = PipelineResult(pipeline_id=pipeline_id, value=None, error=ex)
            await ctx.emit_on_pipeline_complete(result=result)

            return result


@dataclass(frozen=True, repr=False)
class NullPipelineChart(PipelineChartBase):
    """
    Пустая реализация определения пайплайна ML-модели

    Для обратной совместимости контекста со старым кодом моделей
    """

    entrypoint: Entrypoint = None

    def run(
        self,
        pipeline_id: t.Optional[PipelineId] = None,
        input_kwargs: t.Optional[t.Dict[str, t.Any]] = None,
        meta: t.Dict[str, t.Any] = None,
    ) -> NodeResultT:
        raise NotImplementedError('Unable to run this kind of pipeline')
