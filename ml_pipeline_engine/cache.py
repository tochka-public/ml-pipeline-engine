import copy
import pickle
import typing as t

from ml_pipeline_engine.parallelism import process_pool_registry
from ml_pipeline_engine.types import DAGCacheManagerLike, NodeId


class Cache(DAGCacheManagerLike):
    def __init__(self):
        self._cache = {}

    def save(self, node_id: NodeId, data: t.Any):
        self._cache[node_id] = copy.deepcopy(data)

    def load(self, node_id: NodeId) -> t.Any:
        if node_id in self._cache:
            return self._cache[node_id]

    def exists(self, node_id: NodeId) -> bool:
        return bool(node_id in self._cache)


class MultiprocessCache(DAGCacheManagerLike):
    def __init__(self):
        self._cache = process_pool_registry.get_manager().dict()

    def save(self, node_id: NodeId, data: t.Any):
        self._cache[node_id] = pickle.dumps(data)

    def load(self, node_id: NodeId) -> t.Any:
        if node_id in self._cache:
            return pickle.loads(self._cache[node_id])

    def exists(self, node_id: NodeId) -> bool:
        return bool(node_id in self._cache)
