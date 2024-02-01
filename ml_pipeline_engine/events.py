import typing as t

from ml_pipeline_engine.types import (
    EventManagerLike,
    NodeId,
    PipelineContextLike,
    PipelineResult,
)


class EventManagerBase:
    async def on_pipeline_start(self, ctx: PipelineContextLike) -> None:
        ...

    async def on_pipeline_complete(self, ctx: PipelineContextLike, result: PipelineResult) -> None:
        ...

    async def on_node_start(self, ctx: PipelineContextLike, node_id: NodeId) -> None:
        ...

    async def on_node_complete(self, ctx: PipelineContextLike, node_id: NodeId, error: t.Optional[Exception]) -> None:
        ...


class EventSourceMixin:
    _get_event_managers: t.Callable[..., t.List[t.Type[EventManagerLike]]]

    async def _emit(self, event_name: str, **kwargs):
        for mgr in self._get_event_managers():
            callback = getattr(mgr, event_name, None)

            if callback:
                await callback(ctx=self, **kwargs)

    async def emit_on_node_start(self, node_id: NodeId):
        await self._emit('on_node_start', node_id=node_id)

    async def emit_on_node_complete(self, node_id: NodeId, error: t.Optional[Exception]):
        await self._emit('on_node_complete', node_id=node_id, error=error)

    async def emit_on_pipeline_start(self):
        await self._emit('on_pipeline_start')

    async def emit_on_pipeline_complete(self, result: PipelineResult):
        await self._emit('on_pipeline_complete', result=result)
