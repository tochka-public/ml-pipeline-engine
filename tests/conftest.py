import logging
import logging.config
import logging.handlers
import typing as t

import pytest

from ml_pipeline_engine import logs
from ml_pipeline_engine.chart import NullPipelineChart
from ml_pipeline_engine.context.dag import DAGPipelineContext
from ml_pipeline_engine.dag_builders.annotation import build_dag as build_dag_base
from ml_pipeline_engine.parallelism import process_pool_registry
from ml_pipeline_engine.parallelism import threads_pool_registry
from ml_pipeline_engine.types import DAGLike


def pytest_sessionstart(session):  # noqa
    threads_pool_registry.auto_init()
    process_pool_registry.auto_init()


@pytest.fixture
def get_loggers() -> t.Tuple[logging.Logger, ...]:
    return (
        logs.logger_manager,
        logs.logger_decorators,
        logs.logger_parallelism,
    )


@pytest.fixture
def caplog_debug(caplog, get_loggers) -> pytest.LogCaptureFixture:

    for logger in get_loggers:
        logger.addHandler(caplog.handler)

    log_level = logging.getLevelName(logging.root.level)
    caplog.set_level(logging.DEBUG)

    yield caplog

    caplog.set_level(log_level)
    for logger in get_loggers:
        logger.removeHandler(caplog.handler)


@pytest.fixture
def model_name_op() -> str:
    return 'no_op'


@pytest.fixture
def build_dag() -> t.Callable[..., DAGLike]:

    def wrap(*args, **kwargs) -> DAGLike:
        return build_dag_base(*args, **kwargs)

    return wrap


@pytest.fixture
def pipeline_context(model_name_op) -> t.Callable[..., DAGPipelineContext]:
    def wrapper(**input_kwargs) -> DAGPipelineContext:
        return DAGPipelineContext(
            chart=NullPipelineChart(model_name_op),
            input_kwargs=input_kwargs,
        )

    return wrapper
