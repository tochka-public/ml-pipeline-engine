import typing as t
from uuid import UUID

import pytest

from ml_pipeline_engine.node import ProcessorBase
from ml_pipeline_engine.node import generate_pipeline_id
from ml_pipeline_engine.node import get_node_id
from ml_pipeline_engine.node import run_node
from ml_pipeline_engine.node.errors import RunMethodExpectedError
from ml_pipeline_engine.types import NodeBase
from ml_pipeline_engine.types import PipelineChartLike


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

    assert await run_node(SomeNode, x=10, node_id='an_example') == 10


async def test_build_graph__error_no_process_method(
    build_chart: t.Callable[..., PipelineChartLike],
) -> None:
    class SomeNode(NodeBase):
        @staticmethod
        def process(x: int) -> int:
            return x

    class AnotherNode(NodeBase):
        @staticmethod
        def not_process(x: int) -> int:
            return x

    with pytest.raises(RunMethodExpectedError):
        build_chart(input_node=SomeNode, output_node=AnotherNode)
