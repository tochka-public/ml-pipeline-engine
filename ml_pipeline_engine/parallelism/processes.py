from concurrent.futures import ProcessPoolExecutor
from multiprocessing import Manager
from multiprocessing import get_context
from typing import Optional

from ml_pipeline_engine.logs import logger_parallelism as logger
from ml_pipeline_engine.parallelism.basic import PoolExecutorRegistry as BasePoolExecutorRegistry

__all__ = ('process_pool_registry', )


class PoolExecutorRegistry(BasePoolExecutorRegistry):

    def __init__(self) -> None:
        super().__init__()
        self._process_manager: Optional[Manager] = None

    def is_ready(self) -> None:

        if not self._pool_executor or self._pool_executor._shutdown_thread:
            raise RuntimeError('Исполнение невозможно без указания пула процессов')

        if not self._process_manager:
            raise RuntimeError('Исполнение невозможно без указания менеджера данных для процессов')

    def register_manager(self, manager: Manager) -> None:
        if self._process_manager:
            logger.info(
                'Регистрация менеджера пула процессов возможна только один раз. '
                'Повторная инициализация будет пропущена',
            )
            return

        logger.info('Зарегистрирован менеджер данных для процессов')
        self._process_manager = manager

    def get_manager(self) -> Manager:
        self.is_ready()
        return self._process_manager

    def shutdown(self) -> None:
        super().shutdown()

        if self._process_manager is not None:
            self._process_manager.shutdown()

    def auto_init(self) -> None:
        self.register_manager(Manager())
        self.register_pool_executor(ProcessPoolExecutor(mp_context=get_context('fork')))


process_pool_registry = PoolExecutorRegistry()
