import pathlib
import typing as t
from dataclasses import dataclass

from ml_pipeline_engine.dag.graph import DiGraph
from ml_pipeline_engine.dag.manager import DAGRunConcurrentManager
from ml_pipeline_engine.node.retrying import NodeRetryPolicy
from ml_pipeline_engine.parallelism import process_pool_registry
from ml_pipeline_engine.parallelism import threads_pool_registry
from ml_pipeline_engine.types import DAGLike
from ml_pipeline_engine.types import DAGRunManagerLike
from ml_pipeline_engine.types import NodeBase
from ml_pipeline_engine.types import NodeId
from ml_pipeline_engine.types import NodeResultT
from ml_pipeline_engine.types import PipelineContextLike
from ml_pipeline_engine.types import RetryPolicyLike


@dataclass()
class DAG(DAGLike):
    graph: DiGraph
    input_node: NodeId
    output_node: NodeId
    is_process_pool_needed: bool
    is_thread_pool_needed: bool
    node_map: t.Dict[NodeId, NodeBase]
    retry_policy: t.Type[RetryPolicyLike] = NodeRetryPolicy
    run_manager: t.Type[DAGRunManagerLike] = DAGRunConcurrentManager

    def _start_runtime_validation(self) -> None:
        self._validate_pool_executors()

    def _validate_pool_executors(self) -> None:
        if self.is_thread_pool_needed:
            threads_pool_registry.is_ready()

        if self.is_process_pool_needed:
            process_pool_registry.is_ready()

    async def run(self, ctx: PipelineContextLike) -> NodeResultT:
        self._start_runtime_validation()

        run_manager = self.run_manager(dag=self, ctx=ctx)
        return await run_manager.run()

    def visualize(  # type: ignore
        self,
        name: str,
        verbose_name: t.Optional[str] = None,
        target_dir: t.Optional[t.Union[pathlib.Path, str]] = None,
        **kwargs: t.Any,
    ) -> None:
        """
        Create static files for graph visualization

        Args:
            name: Tech name for the dag
            verbose_name: Dag title
            target_dir: Target dir for static
            **kwargs: Graph config kwargs
        """

        from ml_pipeline_viewer.visualization.dag import GraphConfigImpl
        from ml_pipeline_viewer.visualization.dag import build_static

        config = GraphConfigImpl(self).generate(
            name=name or 'Dag',
            verbose_name=verbose_name,
            **kwargs,
        )

        build_static(
            config,
            target_dir=pathlib.Path(target_dir) or pathlib.Path(__file__).resolve(),
        )
