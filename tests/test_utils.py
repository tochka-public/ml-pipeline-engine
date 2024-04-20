from uuid import UUID

import pytest

from ml_pipeline_engine.base_nodes.datasources import DataSource
from ml_pipeline_engine.base_nodes.feature import FeatureBase
from ml_pipeline_engine.base_nodes.ml_model import MLModelBase
from ml_pipeline_engine.base_nodes.processors import ProcessorBase
from ml_pipeline_engine.base_nodes.vectorizer import FeatureVectorizerBase
from ml_pipeline_engine.node import generate_pipeline_id
from ml_pipeline_engine.node import get_node_id
from ml_pipeline_engine.node import get_run_method
from ml_pipeline_engine.node import run_node
from ml_pipeline_engine.types import NodeBase


def test_generate_pipeline_id():
    assert isinstance(generate_pipeline_id(), UUID)


def test_get_id():
    node_id_test_prefix = __name__.replace('.', '_')

    class SomeNode(ProcessorBase):
        node_type = 'some-node-type'
        name = 'some-node'

    assert get_node_id(SomeNode) == 'some-node-type__some-node'
    assert get_node_id(type(SomeNode())) == 'some-node-type__some-node'

    class SomeNode(NodeBase):
        pass

    assert get_node_id(SomeNode) == f'node__{node_id_test_prefix}_SomeNode'


async def test_run_method():
    class SomeNode(ProcessorBase):
        @staticmethod
        def process(x: int) -> int:
            return x

    assert get_run_method(SomeNode) == 'process'
    assert await run_node(SomeNode, x=10) == 10

    class SomeNode(FeatureBase):
        @staticmethod
        def extract(x: int) -> int:
            return x

    assert get_run_method(SomeNode) == 'extract'
    assert await run_node(SomeNode, x=10) == 10

    class SomeNode(DataSource):
        @staticmethod
        def collect(x: int) -> int:
            return x

    assert get_run_method(SomeNode) == 'collect'
    assert await run_node(SomeNode, x=10) == 10

    class SomeNode(FeatureVectorizerBase):
        @staticmethod
        def vectorize(x: int) -> int:
            return x

    assert get_run_method(SomeNode) == 'vectorize'
    assert await run_node(SomeNode, x=10) == 10

    class SomeNode(MLModelBase):
        @staticmethod
        def predict(x: int) -> int:
            return x

    assert get_run_method(SomeNode) == 'predict'
    assert await run_node(SomeNode, x=10) == 10


def test_get_run_method_2_methods_error():
    class SomeNode(NodeBase):
        @staticmethod
        def extract(x: int) -> int:
            return x

        @staticmethod
        def collect(x: int) -> int:
            return x

    with pytest.raises(AssertionError):
        get_run_method(SomeNode)
