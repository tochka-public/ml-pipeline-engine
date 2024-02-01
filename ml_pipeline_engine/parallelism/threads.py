from concurrent.futures import ThreadPoolExecutor

from ml_pipeline_engine.parallelism.basic import (
    PoolExecutorRegistry as BasePoolExecutorRegistry,
)

__all__ = ('threads_pool_registry',)


class PoolExecutorRegistry(BasePoolExecutorRegistry):

    def auto_init(self):
        self.register_pool_executor(ThreadPoolExecutor())


threads_pool_registry = PoolExecutorRegistry()
