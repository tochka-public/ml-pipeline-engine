import logging
import logging.config
import logging.handlers

import pytest

from ml_pipeline_engine import logs
from ml_pipeline_engine.chart import NullPipelineChart
from ml_pipeline_engine.context.dag import DAGPipelineContext
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

    for logger in (
        logs.logger_manager,
        logs.logger_decorators,
        logs.logger_parallelism,
    ):
        logger.setLevel(logging.DEBUG)

        handler = logging.handlers.SysLogHandler()
        handler.setLevel(logging.DEBUG)

        logger.addHandler(handler)


@pytest.fixture
def caplog_debug(caplog):
    caplog.set_level(logging.DEBUG)
    yield caplog


@pytest.fixture
def model_name_op() -> str:
    return 'no_op'


@pytest.fixture
def build_dag():

    def wrap(*args, **kwargs):
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
