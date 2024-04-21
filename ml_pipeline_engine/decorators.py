import functools
import typing as t

from ml_pipeline_engine.base_nodes.datasources import DataSource
from ml_pipeline_engine.exceptions import DataSourceCollectError
from ml_pipeline_engine.logs import logger_decorators as logger


def guard_datasource_error(name: t.Optional[str] = None, title: t.Optional[str] = None) -> t.Callable:
    def _guard_datasource_error(func: t.Callable) -> t.Callable:
        @functools.wraps(func)
        def wrapper(*args: t.Any, **kwargs: t.Any) -> t.Any:
            is_empty_names = name is None and title is None
            if args and isinstance(args[0], DataSource):
                assert is_empty_names, 'Не нужно явно указывать имя источника данных, если используется DataSource'
                data_source = args[0]
            else:
                assert not is_empty_names, 'Укажите имя источника данных явно'
                _name, _title = name, title

                class _LegacyDataSource(DataSource):
                    """
                    Для совместимости с кодом, где не используется класс DataSource
                    """

                    name = _name
                    title = _title

                    def collect(self) -> t.Any:
                        raise NotImplementedError

                data_source = _LegacyDataSource()
            try:
                return func(*args, **kwargs)
            except Exception as ex:
                logger.info('Источник отработал с ошибкой и вернул DataSourceCollectError: %s', str(ex))
                return DataSourceCollectError(
                    source_title=data_source.title,
                    source_name=getattr(data_source, 'name', None),
                )

        return wrapper

    return _guard_datasource_error
