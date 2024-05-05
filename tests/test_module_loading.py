import pathlib
import typing as t

from ml_pipeline_engine.module_loading import get_instance
from ml_pipeline_engine.module_loading import import_string

SomeClassT = t.TypeVar('SomeClassT')


def test_import_string() -> None:
    assert import_string('pathlib.Path') == pathlib.Path


def test_get_instance_init() -> None:
    class SomeClass:
        def __init__(self, some_value: int) -> None:
            self.some_value = some_value

    instance = get_instance(SomeClass, some_value=10)

    assert isinstance(instance, SomeClass)
    assert instance.some_value == 10


def test_get_instance_default_factory() -> None:

    class SomeClass:
        def __init__(self, some_value: int, some_idem_value: int) -> None:
            self.sum = some_value + some_idem_value

        @classmethod
        def default_factory(cls, some_value: int) -> SomeClassT:
            return cls(some_idem_value=100, some_value=some_value)

    instance = get_instance(SomeClass, some_value=10)

    assert isinstance(instance, SomeClass)
    assert instance.sum == 110
