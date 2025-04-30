"""
Пример запуска чарта с хранилищем артефактов
"""

import asyncio

from ml_pipeline_engine.artifact_store.store.filesystem import FileSystemArtifactStore
from ml_pipeline_engine.chart import PipelineChart
from ml_pipeline_engine.dag_builders.annotation import build_dag
from ml_pipeline_engine.dag_builders.annotation.marks import Input
from ml_pipeline_engine.node.base_nodes import ProcessorBase
from ml_pipeline_engine.parallelism import threads_pool_registry


class InvertNumber(ProcessorBase):
    def process(self, num: float) -> float:
        return -num


class AddConst(ProcessorBase):
    def process(self, num: Input(InvertNumber), const: float = 0.2) -> float:
        return num + const


class DoubleNumber(ProcessorBase):
    def process(self, num: Input(InvertNumber)) -> float:
        return num * 2


async def main() -> None:
    threads_pool_registry.auto_init()
    dag = build_dag(input_node=InvertNumber, output_node=DoubleNumber)

    chart = PipelineChart(
        model_name='name',
        entrypoint=dag,
        artifact_store=FileSystemArtifactStore,  # S3 artifact store can be used here as well
    )

    result = await chart.run(
        input_kwargs={
            'artifact_dir': './data/artifacts',
            'num': 10,
        },
    )
    print(result)  # noqa: T201


if __name__ == '__main__':
    asyncio.run(main())
