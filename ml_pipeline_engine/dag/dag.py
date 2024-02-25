from dataclasses import dataclass
from typing import Dict, Type

from ml_pipeline_engine.dag.graph import DiGraph
from ml_pipeline_engine.dag.manager import DAGRunConcurrentManager
from ml_pipeline_engine.dag.retrying import DagRetryPolicy
from ml_pipeline_engine.parallelism import (
    process_pool_registry,
    threads_pool_registry,
)
from ml_pipeline_engine.types import (
    DAGLike,
    DAGRunManagerLike,
    NodeId,
    NodeResultT,
    NodeSerializerLike,
    PipelineContextLike,
    RetryPolicyLike,
)


@dataclass()
class DAG(DAGLike):
    graph: DiGraph
    input_node: NodeId
    output_node: NodeId
    is_process_pool_needed: bool
    is_thread_pool_needed: bool
    node_map: Dict[NodeId, NodeSerializerLike]
    retry_policy: Type[RetryPolicyLike] = DagRetryPolicy
    run_manager: Type[DAGRunManagerLike] = DAGRunConcurrentManager

    def __post_init__(self) -> None:
        self._start_runtime_validation()

    def _start_runtime_validation(self) -> None:
        self._validate_pool_executors()

    def _validate_pool_executors(self) -> None:
        if self.is_thread_pool_needed:
            threads_pool_registry.is_ready()

        if self.is_process_pool_needed:
            process_pool_registry.is_ready()

    async def run(self, ctx: PipelineContextLike) -> NodeResultT:
        return await self.run_manager(dag=self).run(ctx)
