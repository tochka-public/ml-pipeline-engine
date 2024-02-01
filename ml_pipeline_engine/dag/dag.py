import logging
from dataclasses import dataclass, field
from typing import Dict, Type

from ml_pipeline_engine.dag.enums import RunType
from ml_pipeline_engine.dag.graph import DiGraph
from ml_pipeline_engine.dag.manager import (
    DAGRunManagerMultiprocess,
    DAGRunManagerSingleProcess,
)
from ml_pipeline_engine.dag.retrying import DagRetryPolicy
from ml_pipeline_engine.types import (
    DAGLike,
    DAGRunManagerLike,
    NodeId,
    NodeResultT,
    NodeSerializerLike,
    PipelineContextLike,
    RetryPolicyLike,
)

logger = logging.getLogger(__name__)


@dataclass()
class DAG(DAGLike):
    graph: DiGraph
    input_node: NodeId
    output_node: NodeId
    node_map: Dict[NodeId, NodeSerializerLike]
    run_type: RunType
    retry_policy: Type[RetryPolicyLike] = DagRetryPolicy
    run_manager: Type[DAGRunManagerLike] = field(init=False)

    def __post_init__(self):

        if self.run_type == RunType.single_process:
            self.run_manager = DAGRunManagerSingleProcess

        elif self.run_type == RunType.multi_process:
            self.run_manager = DAGRunManagerMultiprocess

        else:
            raise RuntimeError(f'Указанный тип запуска не поддерживается. {self.run_type=}')

    async def run(self, ctx: PipelineContextLike) -> NodeResultT:
        logger.debug('Начало запуска DAG. run_type=%s', self.run_type)
        return await self.run_manager(dag=self).run(ctx)
