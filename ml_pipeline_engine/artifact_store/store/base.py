import typing as t
from abc import ABCMeta, abstractmethod

from ml_pipeline_engine.artifact_store.enums import DataFormat
from ml_pipeline_engine.types import NodeId, PipelineContextLike


class ArtifactStore(metaclass=ABCMeta):
    def __init__(self, ctx: PipelineContextLike, *args, **kwargs):
        self.ctx = ctx

    @abstractmethod
    async def save(self, node_id: NodeId, data: t.Any) -> None:
        ...

    @abstractmethod
    async def load(self, node_id: NodeId) -> t.Any:
        ...


class SerializedArtifactStore(ArtifactStore, metaclass=ABCMeta):
    def __init__(self, ctx: PipelineContextLike, *args, **kwargs):
        super().__init__(ctx=ctx, *args, **kwargs)

    @abstractmethod
    async def save(self, node_id: NodeId, data: t.Any, fmt: DataFormat = None) -> None:
        ...
