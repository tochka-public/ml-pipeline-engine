import typing as t
from enum import Enum


class NodeErrorType(str, Enum):
    succession_node_error = 'succession_node_error'


class DataSourceCollectError(Exception):
    def __init__(self, source_title: str, source_name: t.Optional[str] = None, *args: t.Any) -> None:
        super().__init__(args)
        self.source_title = source_title
        self.source_name = source_name

    def __str__(self) -> str:
        return f'Не удалось получить данные из источника "{self.source_title}"'
