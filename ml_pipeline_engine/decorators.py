import functools
import typing as t

from ml_pipeline_engine.exceptions import DataSourceCollectError
from ml_pipeline_engine.logs import logger_decorators as logger


def guard_datasource_error() -> t.Callable:
    def _guard_datasource_error(func: t.Callable) -> t.Callable:
        @functools.wraps(func)
        def wrapper(*args: t.Any, **kwargs: t.Any) -> t.Any:
            node = args[0]
            try:
                return func(*args, **kwargs)
            except Exception as ex:
                logger.info('Источник отработал с ошибкой и вернул DataSourceCollectError: %s', str(ex))
                return DataSourceCollectError(name=getattr(node, 'name', None))

        return wrapper

    return _guard_datasource_error
