import pytest

from ml_pipeline_engine.chart import NullPipelineChart
from ml_pipeline_engine.context.dag import (
    DAGPipelineContext,
    DAGPipelineMultiprocessContext,
)
from ml_pipeline_engine.dag.enums import RunType
from ml_pipeline_engine.dag_builders.annotation import (
    build_dag as build_dag_base,
)
from ml_pipeline_engine.parallelism import (
    process_pool_registry,
    threads_pool_registry,
)


def pytest_sessionstart(session):  # noqa
    threads_pool_registry.auto_init()
    process_pool_registry.auto_init()


@pytest.fixture
def model_name_op() -> str:
    return 'no_op'


@pytest.fixture
def build_multiprocess_dag():

    def wrap(*args, **kwargs):
        kwargs['run_type'] = RunType.multi_process
        return build_dag_base(*args, **kwargs)

    return wrap


@pytest.fixture
def build_dag():

    def wrap(*args, **kwargs):
        kwargs['run_type'] = RunType.single_process
        return build_dag_base(*args, **kwargs)

    return wrap


@pytest.fixture
def pipeline_context(model_name_op):
    def wrapper(**input_kwargs) -> DAGPipelineContext:
        return DAGPipelineContext(
            chart=NullPipelineChart(model_name_op),
            input_kwargs=input_kwargs,
        )

    return wrapper


@pytest.fixture
def pipeline_multiprocess_context(model_name_op):
    def wrapper(**input_kwargs) -> DAGPipelineMultiprocessContext:
        return DAGPipelineMultiprocessContext(
            chart=NullPipelineChart(model_name_op),
            input_kwargs=input_kwargs,
        )

    return wrapper
