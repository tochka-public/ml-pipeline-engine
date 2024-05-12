from uuid import UUID

from ml_pipeline_engine.node import generate_pipeline_id
from ml_pipeline_engine.node import get_node_id
from ml_pipeline_engine.node import get_run_method
from ml_pipeline_engine.node import run_node
from ml_pipeline_engine.node.base_nodes import ProcessorBase
from ml_pipeline_engine.types import NodeBase


def test_generate_pipeline_id() -> None:
    assert isinstance(generate_pipeline_id(), UUID)


def test_get_id() -> None:
    node_id_test_prefix = __name__.replace('.', '_')

    class SomeNode(ProcessorBase):
        node_type = 'some-node-type'
        name = 'some-node'

    assert get_node_id(SomeNode) == 'some-node-type__some-node'
    assert get_node_id(type(SomeNode())) == 'some-node-type__some-node'

    class SomeNode(NodeBase):
        pass

    assert get_node_id(SomeNode) == f'node__{node_id_test_prefix}_SomeNode'


async def test_run_method() -> None:
    class SomeNode(ProcessorBase):
        @staticmethod
        def process(x: int) -> int:
            return x

    assert get_run_method(SomeNode) == 'process'
    assert await run_node(SomeNode, x=10) == 10
