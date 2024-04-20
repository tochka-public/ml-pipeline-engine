import pytest

from ml_pipeline_engine.artifact_store.serializers import JSONSerializer
from ml_pipeline_engine.artifact_store.serializers import PickleSerializer


@pytest.mark.parametrize('serializer', [PickleSerializer(), JSONSerializer()])
def test_serializers(serializer) -> None:
    with serializer.get_default_io() as buf:
        serializer.dump({'some-key': 'some-value'}, buf)
        assert serializer.load(buf) == {'some-key': 'some-value'}
