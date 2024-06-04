import typing as t

from ml_pipeline_engine.base_nodes.processors import ProcessorBase
from ml_pipeline_engine.base_nodes.processors import RecurrentProcessor
from ml_pipeline_engine.context.dag import DAGPipelineContext
from ml_pipeline_engine.dag_builders.annotation.marks import Input
from ml_pipeline_engine.dag_builders.annotation.marks import InputOneOf
from ml_pipeline_engine.dag_builders.annotation.marks import RecurrentSubGraph
from ml_pipeline_engine.types import AdditionalDataT
from ml_pipeline_engine.types import DAGLike
from ml_pipeline_engine.types import Recurrent


class InputNumber(ProcessorBase):
    def process(
        self,
        num: float,
        additional_data: t.Optional[AdditionalDataT] = None,
    ) -> float:

        if additional_data is None:
            return num

        if additional_data == 5:
            return 5

        if additional_data == 7:
            return 11

        raise Exception


class PassNum(ProcessorBase):
    def process(self, num: Input(InputNumber)) -> float:
        return num


class DoubleNumberWithError(RecurrentProcessor):
    async def process(self, num: Input(PassNum)) -> Recurrent:

        if num == 3:
            return self.next_iteration(5)

        if num == 5:
            raise Exception('Error')

        raise Exception


class DoubleNumber(RecurrentProcessor):
    async def process(self, num: Input(PassNum)) -> t.Union[Recurrent, float]:

        if num == 3:
            return self.next_iteration(5)

        if num == 5:
            return self.next_iteration(7)

        if num == 11:
            return 11

        return num


error_recurrent_double_number = RecurrentSubGraph(
    start_node=InputNumber,
    dest_node=DoubleNumberWithError,
    max_iterations=3,
)

normal_recurrent_double_number = RecurrentSubGraph(
    start_node=InputNumber,
    dest_node=DoubleNumber,
    max_iterations=3,
)


class ErrorRecProxy(ProcessorBase):
    async def process(
        self,
        rec_results: error_recurrent_double_number,
    ) -> t.Any:
        return rec_results


class RecProxy(ProcessorBase):
    async def process(
        self,
        rec_results: normal_recurrent_double_number,
    ) -> t.Any:
        return rec_results


class OneOfNode(ProcessorBase):
    async def process(
        self,
        potential_results: InputOneOf([ErrorRecProxy, RecProxy]),
    ) -> t.Any:
        return potential_results


async def test_dag(
    pipeline_context: t.Callable[..., DAGPipelineContext],
    build_dag: t.Callable[..., DAGLike],
) -> None:
    dag = build_dag(input_node=InputNumber, output_node=OneOfNode)
    assert await dag.run(pipeline_context(num=3)) == 11
