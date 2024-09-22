import typing as t

import pytest_mock

from ml_pipeline_engine.context.dag import DAGPipelineContext
from ml_pipeline_engine.dag_builders.annotation.marks import Input
from ml_pipeline_engine.dag_builders.annotation.marks import InputGeneric
from ml_pipeline_engine.dag_builders.annotation.marks import InputOneOf
from ml_pipeline_engine.node import ProcessorBase
from ml_pipeline_engine.node import build_node
from ml_pipeline_engine.types import DAGLike
from ml_pipeline_engine.types import NodeBase


class SomeInput(ProcessorBase):
    name = 'input'

    def process(self, base_num: int) -> int:
        return base_num


class FlDataSourceGeneric(ProcessorBase):
    name = 'source'

    def process(self, **__: t.Any) -> t.Type[Exception]:
        raise Exception


FlDataSource = build_node(
    FlDataSourceGeneric,
    inp=Input(SomeInput),
)


class SomeFeatureGeneric(ProcessorBase):
    name = 'feature'

    def process(self, fl_credit_history: InputGeneric(NodeBase), **__: t.Any) -> int:
        return len(fl_credit_history)


class SomeFeatureFallback(ProcessorBase):
    name = 'feature_fallback'

    def process(self) -> int:
        return 777_777


SomeFeature = build_node(
    SomeFeatureGeneric,
    fl_credit_history=Input(FlDataSource),
    inp=Input(SomeInput),
)


class SomeMLModel(ProcessorBase):
    name = 'some_model'

    def process(self, fl_credit_history_feature: InputOneOf([SomeFeature, SomeFeatureFallback])) -> int:
        return fl_credit_history_feature


async def test_fail_node(
    pipeline_context: t.Callable[..., DAGPipelineContext],
    build_dag: t.Callable[..., DAGLike],
    mocker: pytest_mock.MockerFixture,
) -> None:
    extract_patch = mocker.patch.object(SomeFeatureGeneric, 'process')
    dag = build_dag(input_node=SomeInput, output_node=SomeMLModel)

    assert await dag.run(pipeline_context(base_num=10)) == 777_777
    assert extract_patch.call_count == 0
