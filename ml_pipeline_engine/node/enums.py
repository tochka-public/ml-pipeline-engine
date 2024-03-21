import enum


class NodeTag(str, enum.Enum):
    """
    Поддерживаемые теги узлов
    """

    process = 'process'
    thread = 'thread'
    non_async = 'non_async'
