import functools
import inspect
import typing as t
from unittest.mock import ANY, Mock, call

call_object = functools.partial(call, ANY)


class FactoryMocker:
    """
    Фабрика мокеров сделана для того, чтобы отслеживать вызовы функций/методов,
    при этом нет возможности потерять аннотации типов
    """

    def __init__(self):
        self.mock = Mock()

    def __call__(self, func: t.Callable) -> t.Callable:

        method = getattr(self.mock, func.__name__)

        @functools.wraps(func)
        def wrap(*args, **kwargs):
            method(*args, **kwargs)
            return func(*args, **kwargs)

        @functools.wraps(func)
        async def async_wrap(*args, **kwargs):
            method(*args, **kwargs)
            return await func(*args, **kwargs)

        if inspect.iscoroutinefunction(func):
            return async_wrap

        return wrap
