from uuid import UUID

import pytest

from ml_pipeline_engine.base_nodes.processors import ProcessorBase
from ml_pipeline_engine.node import generate_pipeline_id
from ml_pipeline_engine.node import get_node_id
from ml_pipeline_engine.node import get_run_method
from ml_pipeline_engine.node import run_node
from ml_pipeline_engine.types import NodeBase


def test_generate_pipeline_id() -> None:
    assert isinstance(generate_pipeline_id(), UUID)


def test_get_id() -> None:
    class SomeNode(ProcessorBase):
        node_type = 'some-node-type'
        name = 'some-node'

    assert get_node_id(SomeNode) == 'some-node-type__some-node'
    assert get_node_id(type(SomeNode())) == 'some-node-type__some-node'


async def test_run_method() -> None:
    class SomeNode(ProcessorBase):
        @staticmethod
        def process(x: int) -> int:
            return x

    assert get_run_method(SomeNode) == 'process'
    assert await run_node(SomeNode, x=10, node_id='an_example') == 10


def test_get_run_method_2_methods_error() -> None:
    class SomeNode(NodeBase):
        @staticmethod
        def extract(x: int) -> int:
            return x

        @staticmethod
        def collect(x: int) -> int:
            return x

    with pytest.raises(AssertionError):
        get_run_method(SomeNode)
