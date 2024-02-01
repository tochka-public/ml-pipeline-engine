import enum


class SerializationNodeKind(str, enum.Enum):
    """
    Поддерживаемые типы сериализации узлов между
    """

    cloudpickle = 'cloudpickle'


class NodeTag(str, enum.Enum):
    """
    Поддерживаемые теги узлов
    """

    process = 'process'
    thread = 'thread'
