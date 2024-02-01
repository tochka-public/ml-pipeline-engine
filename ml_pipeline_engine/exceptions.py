from enum import Enum
from typing import Optional


class NodeErrorType(str, Enum):
    succession_node_error = 'succession_node_error'


class DataSourceCollectError(Exception):
    def __init__(self, source_title: str, source_name: Optional[str] = None, *args):
        super().__init__(args)
        self.source_title = source_title
        self.source_name = source_name

    def __str__(self):
        return f'Не удалось получить данные из источника "{self.source_title}"'
