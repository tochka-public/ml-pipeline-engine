import typing as t
from collections import UserDict
from dataclasses import dataclass
from dataclasses import field

from ml_pipeline_engine.types import NodeId


class HiddenDict(UserDict):
    """
    Dict object that can hide some keys until they are set again
    """

    def __init__(self, *args: t.Any, **kwargs: t.Any) -> None:
        super().__init__(*args, **kwargs)
        self._hidden_keys: set = set()

    def get(self, key: t.Any, with_hidden: bool = True) -> t.Any:
        if with_hidden is False and key in self._hidden_keys:
            return None

        return super().get(key)

    def exists(self, key: t.Any, with_hidden: bool = True) -> bool:
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
class DAGNodeStorage:
    """
    A container for all information about node results
    """

    node_results: HiddenDict = field(default_factory=HiddenDict)
    processed_nodes: HiddenDict = field(default_factory=HiddenDict)
    switch_results: HiddenDict = field(default_factory=HiddenDict)
    recurrent_subgraph: HiddenDict = field(default_factory=HiddenDict)
    waiting_list: HiddenDict = field(default_factory=HiddenDict)

    def set_node_result(self, node_id: NodeId, data: t.Any) -> None:
        self.node_results.set(node_id, data)

    def get_node_result(self, node_id: NodeId, with_hidden: bool = False) -> t.Any:
        return self.node_results.get(node_id, with_hidden)

    def hide_node_result(self, node_id: NodeId) -> None:
        self.node_results.hide(node_id)

    def exists_node_result(self, node_id: NodeId, with_hidden: bool = False) -> bool:
        return self.node_results.exists(node_id, with_hidden)

    def copy_node_result(self, from_node_id: NodeId, to_node_id: NodeId) -> None:
        self.set_node_result(to_node_id, self.get_node_result(from_node_id, with_hidden=True))

    def exists_node_error(self, node_id: NodeId, with_hidden: bool = False) -> bool:
        return self.exists_result_type(node_id, (BaseException,), with_hidden=with_hidden)

    def exists_result_type(
        self,
        node_id: NodeId,
        target_type: t.Tuple[t.Any, ...] = (object,),
        exclude_type: t.Tuple[t.Any, ...] = (),
        exclude_none: bool = True,
        with_hidden: bool = False,
    ) -> bool:
        result = self.node_results.get(node_id, with_hidden)

        if exclude_none and result is None:
            return False

        return isinstance(result, target_type) and not isinstance(result, exclude_type)

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
