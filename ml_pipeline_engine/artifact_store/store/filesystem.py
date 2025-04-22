import functools
import typing as t
import warnings
from enum import Enum
from pathlib import Path

import anyio

from ml_pipeline_engine.artifact_store.enums import DataFormat
from ml_pipeline_engine.artifact_store.errors import ArtifactAlreadyExists
from ml_pipeline_engine.artifact_store.errors import ArtifactDoesNotExist
from ml_pipeline_engine.artifact_store.serializers import serializer_factory
from ml_pipeline_engine.artifact_store.store.base import SerializedArtifactStore
from ml_pipeline_engine.types import NodeId
from ml_pipeline_engine.types import NodeResultT
from ml_pipeline_engine.types import PipelineContextLike


class ArtifactFileAlreadyExists(ArtifactAlreadyExists):
    pass


class ArtifactFileDoesNotExist(ArtifactDoesNotExist):
    pass


def dont_use_for_prod(func: t.Callable) -> t.Callable[..., t.Any]:
    @functools.wraps(func)
    async def wrap(*args: t.Any, **kwargs: t.Any) -> t.Any:
        warnings.warn(f'Функция {func.__name__} предназначена для локального использования', stacklevel=1)
        return await func(*args, **kwargs)

    return wrap


class FileSystemArtifactStore(SerializedArtifactStore):
    def __init__(self, ctx: PipelineContextLike, artifact_dir: t.Union[Path, str]) -> None:
        super().__init__(ctx)

        self.artifact_dir = Path(artifact_dir)

    def _ensure_dir(self) -> Path:
        model_name = self.ctx.model_name.value if isinstance(self.ctx.model_name, Enum) else self.ctx.model_name
        path = Path(self.artifact_dir / model_name / str(self.ctx.pipeline_id))

        if not path.exists():
            path.mkdir(parents=True)

        return path

    def _get_glob(self, node_id: NodeId) -> list[Path]:
        return list(Path(self._ensure_dir()).glob(f'{node_id}.*'))

    @dont_use_for_prod
    async def save(self, node_id: NodeId, data: NodeResultT, fmt: DataFormat = DataFormat.PICKLE) -> None:
        if len(self._get_glob(node_id)):
            raise ArtifactFileAlreadyExists(f'Artifact file for {node_id} already exists')

        async with await anyio.open_file(Path(self._ensure_dir() / f'{node_id}.{fmt.value}'), 'wb') as file:
            await serializer_factory.from_data_format(fmt).dump(data, file)

    @dont_use_for_prod
    async def load(self, node_id: NodeId) -> NodeResultT:
        glob = self._get_glob(node_id)

        if not len(glob):
            raise ArtifactFileDoesNotExist(f'Artifact file for {node_id} does not exist')

        async with await anyio.open_file(Path(glob[0]), 'rb') as file:
            return await serializer_factory.from_extension(glob[0].suffix[1:]).load(file)
