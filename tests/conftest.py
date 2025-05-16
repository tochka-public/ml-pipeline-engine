import logging
import logging.config
import logging.handlers
import typing as t

import pytest

from ml_pipeline_engine import logs
from ml_pipeline_engine.chart import PipelineChart
from ml_pipeline_engine.dag_builders.annotation import build_dag as build_dag_base
from ml_pipeline_engine.parallelism import process_pool_registry
from ml_pipeline_engine.parallelism import threads_pool_registry
from ml_pipeline_engine.types import ArtifactStoreLike
from ml_pipeline_engine.types import PipelineChartLike


def pytest_sessionstart(session):  # noqa
    threads_pool_registry.auto_init()
    process_pool_registry.auto_init()


@pytest.fixture
def get_loggers() -> t.Tuple[logging.Logger, ...]:
    return (
        logs.logger_manager,
        logs.logger_decorators,
        logs.logger_parallelism,
        logs.logger_manager_lock,
    )


@pytest.fixture
def caplog_debug(caplog: pytest.LogCaptureFixture, get_loggers: t.Tuple[logging.Logger]) -> pytest.LogCaptureFixture:
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
def build_chart() -> t.Callable[..., PipelineChartLike]:
    def wrap(
        artifact_store: t.Optional[t.Type[ArtifactStoreLike]] = None,
        *args: t.Any,
        **kwargs: t.Any,
    ) -> PipelineChartLike:
        return PipelineChart(
            model_name='no op',
            entrypoint=build_dag_base(*args, **kwargs),
            artifact_store=artifact_store,
        )

    return wrap
