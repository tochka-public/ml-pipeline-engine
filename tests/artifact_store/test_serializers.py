import pytest

from ml_pipeline_engine.artifact_store.serializers import (
    JSONSerializer,
    PickleSerializer,
)


@pytest.mark.parametrize('serializer', [PickleSerializer(), JSONSerializer()])
def test_serializers(serializer):
    with serializer.get_default_io() as buf:
        serializer.dump({'some-key': 'some-value'}, buf)
        assert serializer.load(buf) == {'some-key': 'some-value'}
