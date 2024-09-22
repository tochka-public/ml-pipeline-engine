import typing as t

from ml_pipeline_engine.types import EventManagerLike
from ml_pipeline_engine.types import NodeId
from ml_pipeline_engine.types import PipelineResult


class EventSourceMixin:
    _get_event_managers: t.Callable[..., t.List[t.Type[EventManagerLike]]]

    async def _emit(self, event_name: str, **kwargs: t.Any) -> None:
        for mgr in self._get_event_managers():
            callback = getattr(mgr, event_name, None)

            if callback:
                await callback(ctx=self, **kwargs)

    async def emit_on_node_start(self, node_id: NodeId) -> None:
        await self._emit('on_node_start', node_id=node_id)

    async def emit_on_node_complete(self, node_id: NodeId, error: t.Optional[Exception]) -> None:
        await self._emit('on_node_complete', node_id=node_id, error=error)

    async def emit_on_pipeline_start(self) -> None:
        await self._emit('on_pipeline_start')

    async def emit_on_pipeline_complete(self, result: PipelineResult) -> None:
        await self._emit('on_pipeline_complete', result=result)
