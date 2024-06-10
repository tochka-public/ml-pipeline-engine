import typing as t

import pytest

from ml_pipeline_engine.base_nodes.processors import ProcessorBase
from ml_pipeline_engine.base_nodes.processors import RecurrentProcessor
from ml_pipeline_engine.context.dag import DAGPipelineContext
from ml_pipeline_engine.dag_builders.annotation.marks import Input
from ml_pipeline_engine.dag_builders.annotation.marks import RecurrentSubGraph
from ml_pipeline_engine.types import AdditionalDataT
from ml_pipeline_engine.types import DAGLike
from ml_pipeline_engine.types import Recurrent


class InputNumber(ProcessorBase):
    def process(
        self,
        num: float,
        additional_data: t.Optional[AdditionalDataT] = None,  # noqa: ARG002
    ) -> float:
        return num


class PassNum(ProcessorBase):
    def process(self, _: Input(InputNumber)) -> float:
        raise Exception('AnErrorFromPassNum')


class DoubleNumber(RecurrentProcessor):
    async def process(self, num: Input(PassNum)) -> Recurrent:
        pass


double_number = RecurrentSubGraph(
    start_node=InputNumber,
    dest_node=DoubleNumber,
    max_iterations=3,
)


class ANode(ProcessorBase):
    async def process(
        self,
        potential_results: double_number,
    ) -> t.Any:
        return potential_results


async def test_dag(
    pipeline_context: t.Callable[..., DAGPipelineContext],
    build_dag: t.Callable[..., DAGLike],
) -> None:
    with pytest.raises(Exception, match='AnErrorFromPassNum'):
        assert await build_dag(input_node=InputNumber, output_node=ANode).run(pipeline_context(num=3))
