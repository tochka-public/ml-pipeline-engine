import typing as t

import pytest

from ml_pipeline_engine.artifact_store.enums import DataFormat
from ml_pipeline_engine.artifact_store.store.filesystem import ArtifactFileAlreadyExists
from ml_pipeline_engine.artifact_store.store.filesystem import ArtifactFileDoesNotExist
from ml_pipeline_engine.artifact_store.store.filesystem import FileSystemArtifactStore
from ml_pipeline_engine.types import PipelineContextLike


@pytest.fixture
def store(tmp_path):
    class PipelineContext:
        model_name = 'some-model'
        pipeline_id = 'some-pipeline'

    return FileSystemArtifactStore(ctx=t.cast(PipelineContextLike, PipelineContext()), artifact_dir=tmp_path)


@pytest.mark.parametrize(
    'fmt',
    DataFormat,
)
async def test_fs_artifact_store_success(store, fmt):
    await store.save('some-node-id-1', {'some-key1': 'some-value1'})
    await store.save('some-node-id-2', {'some-key2': 'some-value2'})
    await store.save('some-node-id-3', {'some-key3': 'some-value3'})

    assert await store.load('some-node-id-1') == {'some-key1': 'some-value1'}
    assert await store.load('some-node-id-2') == {'some-key2': 'some-value2'}
    assert await store.load('some-node-id-3') == {'some-key3': 'some-value3'}


async def test_fs_artifact_store_error_already_exists(store):
    await store.save('some-node-id', {'some-key': 'some-value'})

    with pytest.raises(ArtifactFileAlreadyExists):
        await store.save('some-node-id', {'some-key': 'some-value'})


async def test_fs_artifact_store_error_does_not_exist(store):
    with pytest.raises(ArtifactFileDoesNotExist):
        await store.load('some-bad-node-id')
