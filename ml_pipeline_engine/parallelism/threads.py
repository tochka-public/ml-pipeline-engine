from concurrent.futures import ThreadPoolExecutor

from ml_pipeline_engine.parallelism.basic import PoolExecutorRegistry as BasePoolExecutorRegistry

__all__ = ('threads_pool_registry',)


class PoolExecutorRegistry(BasePoolExecutorRegistry):

    def is_ready(self) -> None:
        if not self._pool_executor or self._pool_executor._shutdown:
            raise RuntimeError('Исполнение невозможно без указания пула потоков')

    def auto_init(self) -> None:
        self.register_pool_executor(ThreadPoolExecutor())


threads_pool_registry = PoolExecutorRegistry()
