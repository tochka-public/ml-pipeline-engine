import copy
import typing as t

from ml_pipeline_engine.types import DAGCacheManagerLike
from ml_pipeline_engine.types import NodeId


class Cache(DAGCacheManagerLike):
    def __init__(self) -> None:
        self._cache = {}

    def save(self, node_id: NodeId, data: t.Any) -> None:
        self._cache[node_id] = copy.deepcopy(data)

    def load(self, node_id: NodeId) -> t.Any:
        if node_id in self._cache:
            return self._cache[node_id]
        return None

    def exists(self, node_id: NodeId) -> bool:
        return bool(node_id in self._cache)

    def remove(self, node_id: NodeId) -> None:
        self._cache.pop(node_id, None)
