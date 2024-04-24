import typing as t
from abc import ABCMeta
from abc import abstractmethod

from ml_pipeline_engine.artifact_store.enums import DataFormat
from ml_pipeline_engine.types import NodeId
from ml_pipeline_engine.types import PipelineContextLike


class ArtifactStore(metaclass=ABCMeta):
    def __init__(self, ctx: PipelineContextLike, *_: t.Any, **__: t.Any) -> None:
        self.ctx = ctx

    @abstractmethod
    async def save(self, node_id: NodeId, data: t.Any) -> None:
        ...

    @abstractmethod
    async def load(self, node_id: NodeId) -> t.Any:
        ...


class SerializedArtifactStore(ArtifactStore, metaclass=ABCMeta):
    def __init__(self, ctx: PipelineContextLike, *args: t.Any, **kwargs: t.Any) -> None:
        super().__init__(ctx=ctx, *args, **kwargs)  # noqa: B026

    @abstractmethod
    async def save(self, node_id: NodeId, data: t.Any, fmt: DataFormat = None) -> None:
        ...
