from ml_pipeline_engine.cache import Cache


def test_cache():
    cache = Cache()
    cache.save(node_id='some-node-id-1', data={'some-key1': 'some-value1'})
    assert cache.load(node_id='some-node-id-1') == {'some-key1': 'some-value1'}
