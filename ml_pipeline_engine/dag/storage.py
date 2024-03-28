import typing as t
from collections import UserDict
from dataclasses import dataclass, field

from ml_pipeline_engine.types import HiddenDictLike, DAGNodeStorageLike, NodeId


class HiddenDict(UserDict, HiddenDictLike):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._hidden_keys: set = set()

    def get(self, key: t.Any, with_hidden: bool = True) -> t.Any:
        if with_hidden is False and key in self._hidden_keys:
            return None

        return super().get(key)

    def exists(self, key, with_hidden: bool = True) -> bool:
        if with_hidden is False and key in self._hidden_keys:
            return False

        return key in self

    def set(self, key: t.Any, value: t.Any) -> None:

        if key in self._hidden_keys:
            self._hidden_keys.remove(key)

        self[key] = value

    def hide(self, key: t.Any) -> None:
        self._hidden_keys.add(key)

    def delete(self, key: t.Any) -> None:
        self.pop(key)


@dataclass
class DAGNodeStorage(DAGNodeStorageLike):
    node_results: HiddenDict = field(default_factory=HiddenDict)
    processed_nodes: HiddenDict = field(default_factory=HiddenDict)
    switch_results: HiddenDict = field(default_factory=HiddenDict)
    recurrent_subgraph: HiddenDict = field(default_factory=HiddenDict)

    def set_node_result(self, node_id: NodeId, data: t.Any) -> None:
        self.node_results.set(node_id, data)

    def get_node_result(self, node_id: NodeId, with_hidden: bool = False) -> t.Any:
        return self.node_results.get(node_id, with_hidden)

    def hide_node_result(self, node_id: NodeId) -> None:
        self.node_results.hide(node_id)

    def exists_node_result(self, node_id: NodeId, with_hidden: bool = False) -> bool:
        return self.node_results.exists(node_id, with_hidden)

    def set_switch_result(self, node_id: NodeId, data: t.Any) -> t.Any:
        self.switch_results.set(node_id, data)

    def get_switch_result(self, node_id: NodeId, with_hidden: bool = False) -> t.Any:
        return self.switch_results.get(node_id, with_hidden)

    def set_node_as_processed(self, node_id: NodeId) -> None:
        self.processed_nodes.set(node_id, 1)

    def hide_processed_node(self, node_id: NodeId) -> None:
        self.processed_nodes.hide(node_id)

    def exists_processed_node(self, node_id: NodeId, with_hidden: bool = False) -> bool:
        return self.processed_nodes.exists(node_id, with_hidden)

    def set_active_rec_subgraph(self, source: NodeId, dest: NodeId) -> None:
        self.processed_nodes.set((source, dest), 1)

    def delete_active_rec_subgraph(self, source: NodeId, dest: NodeId) -> None:
        self.processed_nodes.delete((source, dest))

    def exists_active_rec_subgraph(self, source: NodeId, dest: NodeId) -> bool:
        return self.processed_nodes.exists((source, dest))

    def hide_last_execution(self, *node_ids: NodeId) -> None:
        for node_id in node_ids:
            self.hide_processed_node(node_id)
            self.hide_node_result(node_id)
