"""
Пример указания ретрая
"""

import asyncio

from ml_pipeline_engine.chart import PipelineChart
from ml_pipeline_engine.dag_builders.annotation import build_dag
from ml_pipeline_engine.dag_builders.annotation.marks import Input
from ml_pipeline_engine.node.base_nodes import ProcessorBase
from ml_pipeline_engine.parallelism import threads_pool_registry


class InvertNumber(ProcessorBase):
    def process(self, num: float) -> float:
        return -num


class SomeMlModelException(Exception):
    pass


COUNTER = 0


class SomeMlModel(ProcessorBase):
    node_type = 'ml_model'
    delay = 1.1
    attempts = 5
    exceptions = (SomeMlModelException,)  # we will catch this particular exception and try again

    def process(self, _: Input(InvertNumber)) -> None:
        global COUNTER  # noqa: PLW0603
        if COUNTER < 4:  # noqa: PLR2004
            COUNTER += 1
            raise SomeMlModelException(f'Need one more attempt, {COUNTER=}')


async def main() -> None:
    threads_pool_registry.auto_init()

    pipeline = PipelineChart(
        'pipeline_with_example',
        build_dag(input_node=InvertNumber, output_node=SomeMlModel),
    )
    await pipeline.run(input_kwargs={'num': 10})
    assert COUNTER == 4  # noqa: PLR2004


if __name__ == '__main__':
    asyncio.run(main())
