import sys
import typing as t
from importlib import import_module

__all__ = [
    'get_instance',
    'import_string',
]


def _cached_import(module_path: str, class_name: str) -> t.Type:
    if not (
        (module := sys.modules.get(module_path))
        and (spec := getattr(module, '__spec__', None))
        and getattr(spec, '_initializing', False) is False
    ):
        module = import_module(module_path)
    return getattr(module, class_name)


def import_string(dotted_path: str) -> t.Type:
    try:
        module_path, class_name = dotted_path.rsplit('.', 1)
    except ValueError as err:
        raise ImportError(f"{dotted_path} doesn't look like a module path") from err

    try:
        return _cached_import(module_path, class_name)
    except AttributeError as err:
        raise ImportError(f'Module "{module_path}" does not define a "{class_name}" attribute/class') from err


def get_instance(cls: t.Type, *args: t.Any, **kwargs: t.Any) -> t.Any:
    default_factory = getattr(cls, 'default_factory', None)
    if default_factory is None:
        return cls(*args, **kwargs)
    return default_factory(*args, **kwargs)
