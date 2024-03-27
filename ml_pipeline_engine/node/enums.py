import enum


class NodeType(str, enum.Enum):
    datasource = 'datasource'
    feature = 'feature'
    ml_model = 'ml_model'
    processor = 'processor'
    vectorizer = 'vectorizer'
    generic = 'generic'
    switch = 'switch'
    input_one_of = 'input_one_of'

    @classmethod
    def is_generic(cls, value: str) -> bool:
        return cls.generic.value in value.lower()

    @classmethod
    def by_prefix(cls, value: str) -> 'NodeType':

        for item in cls:
            if value.startswith(item):
                return cls(item)

        raise RuntimeError("Couldn't find the type")


class NodeTag(str, enum.Enum):
    """
    Поддерживаемые теги узлов
    """

    process = 'process'
    thread = 'thread'
    non_async = 'non_async'
