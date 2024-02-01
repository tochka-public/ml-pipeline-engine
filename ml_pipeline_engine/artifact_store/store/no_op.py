import typing as t

from ml_pipeline_engine.artifact_store.store.base import ArtifactStore
from ml_pipeline_engine.types import NodeId


class NoOpArtifactStore(ArtifactStore):
    async def save(self, node_id: NodeId, data: t.Any) -> None:
        ...

    async def load(self, node_id: NodeId) -> t.Any:
        raise NotImplementedError('Метод "load" не реализован для данного типа хранилища')
