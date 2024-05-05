import abc
import typing as t
from concurrent.futures import ProcessPoolExecutor
from concurrent.futures import ThreadPoolExecutor

from ml_pipeline_engine.logs import logger_parallelism as logger

SingletonMetaT = t.TypeVar('SingletonMetaT', bound='SingletonMeta')


class SingletonMeta(type):
    """
    The Singleton class can be implemented in different ways in Python. Some
    possible methods include: base class, decorator, metaclass. We will use the
    metaclass because it is best suited for this purpose.
    """

    _instances: t.ClassVar[t.Dict] = {}

    def __call__(cls, *args: t.Any, **kwargs: t.Any) -> SingletonMetaT:
        """
        Possible changes to the value of the `__init__` argument do not affect
        the returned instance.
        """
        if cls not in cls._instances:
            instance = super().__call__(*args, **kwargs)
            cls._instances[cls] = instance
        return cls._instances[cls]


PoolExecutorT = t.Union[ProcessPoolExecutor, ThreadPoolExecutor]


class PoolExecutorRegistry(metaclass=SingletonMeta):

    def __init__(self) -> None:
        self._pool_executor: t.Optional[PoolExecutorT] = None

    @abc.abstractmethod
    def is_ready(self) -> None:
        ...

    def register_pool_executor(self, pool_executor: PoolExecutorT) -> None:
        if self._pool_executor:
            logger.info(
                'Регистрация пула %s возможна только один раз. Повторная инициализация будет пропущена',
                type(pool_executor),
            )
            return

        logger.info('Пул %s зарегистрирован', type(pool_executor))
        self._pool_executor = pool_executor

    def get_pool_executor(self) -> PoolExecutorT:
        self.is_ready()
        return self._pool_executor

    def shutdown(self) -> None:
        if self._pool_executor is not None:
            self._pool_executor.shutdown()

    def __del__(self) -> None:
        self.shutdown()
