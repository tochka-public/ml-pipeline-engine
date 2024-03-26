import pathlib
from dataclasses import dataclass
from typing import Dict, Type, Optional, Union

from ml_pipeline_engine.dag.graph import DiGraph
from ml_pipeline_engine.dag.manager import DAGRunConcurrentManager, DAGConcurrentManagerLock
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
    NodeLike,
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
    node_map: Dict[NodeId, NodeLike]
    retry_policy: Type[RetryPolicyLike] = DagRetryPolicy
    run_manager: Type[DAGRunManagerLike] = DAGRunConcurrentManager

    def _start_runtime_validation(self) -> None:
        self._validate_pool_executors()

    def _validate_pool_executors(self) -> None:
        if self.is_thread_pool_needed:
            threads_pool_registry.is_ready()

        if self.is_process_pool_needed:
            process_pool_registry.is_ready()

    async def run(self, ctx: PipelineContextLike) -> NodeResultT:
        self._start_runtime_validation()

        run_manager = self.run_manager(
            lock_manager=DAGConcurrentManagerLock(
                self.node_map.keys(),
            ),
            dag=self,
            ctx=ctx,
        )

        return await run_manager.run()

    def visualize(  # noqa
        self,
        name: str,
        verbose_name: Optional[str] = None,
        target_dir: Optional[Union[pathlib.Path, str]] = None,
        **kwargs,
    ) -> None:
        """
        Create a static for graph visualization

        Args:
            name: Tech name for the dag
            verbose_name: Dag title
            target_dir: Target dir for static
            **kwargs: Graph config kwargs
        """

        from ml_pipeline_engine.visualization.dag import GraphConfigImpl, build_static

        config = GraphConfigImpl(self).generate(
            name=name or 'Dag',
            verbose_name=verbose_name,
            **kwargs,
        )

        build_static(
            config,
            target_dir=pathlib.Path(target_dir) or pathlib.Path(__file__).resolve(),
        )
