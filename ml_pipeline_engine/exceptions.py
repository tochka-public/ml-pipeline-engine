import typing as t
from enum import Enum


class NodeErrorType(str, Enum):
    succession_node_error = 'succession_node_error'


class DataSourceCollectError(Exception):
    def __init__(self, name: t.Optional[str] = None, *args: t.Any) -> None:
        super().__init__(args)
        self.name = name

    def __str__(self) -> str:
        return f'Не удалось получить данные из источника "{self.name}"'
