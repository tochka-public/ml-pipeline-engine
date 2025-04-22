import pytest

from ml_pipeline_engine.artifact_store.serializers import JSONSerializer
from ml_pipeline_engine.artifact_store.serializers import PickleSerializer
from ml_pipeline_engine.artifact_store.serializers import Serializer


@pytest.mark.parametrize('serializer', [PickleSerializer(), JSONSerializer()])
async def test_serializers(serializer: Serializer) -> None:
    with serializer.get_default_io() as buf:
        await serializer.dump({'some-key': 'some-value'}, buf)
        assert await serializer.load(buf) == {'some-key': 'some-value'}
