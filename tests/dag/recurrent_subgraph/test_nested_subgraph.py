import typing as t

from tests.helpers import FactoryMocker
from tests.helpers import call_object

from ml_pipeline_engine.base_nodes.processors import ProcessorBase
from ml_pipeline_engine.base_nodes.processors import RecurrentProcessor
from ml_pipeline_engine.context.dag import DAGPipelineContext
from ml_pipeline_engine.dag_builders.annotation.marks import Input
from ml_pipeline_engine.dag_builders.annotation.marks import RecurrentSubGraph
from ml_pipeline_engine.types import AdditionalDataT
from ml_pipeline_engine.types import DAGLike
from ml_pipeline_engine.types import Recurrent

pseudo_process_mocker = FactoryMocker()
double_process_mocker = FactoryMocker()


class InvertNumber(RecurrentProcessor):

    def process(
        self,
        additional_data: t.Optional[AdditionalDataT] = None,  # noqa: ARG002 ignore unused
        **_: t.Any,
    ) -> float:
        return -5


class DifferentSubgraph(RecurrentProcessor):
    def process(
        self,
        additional_data: t.Optional[AdditionalDataT] = None,  # noqa: ARG002 ignore unused
    ) -> float:
        return 10


class PseudoInfiniteProcess(RecurrentProcessor):
    use_default = True

    def get_default(self, **__: t.Any) -> float:
        return 0

    @pseudo_process_mocker
    def process(self, num: Input(DifferentSubgraph)) -> Recurrent:
        return self.next_iteration(num)


another_recurrent_subgraph = RecurrentSubGraph(
    dest_node=PseudoInfiniteProcess,
    start_node=DifferentSubgraph,
    max_iterations=2,
)


class AddZero(RecurrentProcessor):
    def process(self, num: Input(InvertNumber), num2: another_recurrent_subgraph) -> float:
        return num + num2 + 0


class DoubleNumber(RecurrentProcessor):
    use_default = True

    def get_default(self, **__: t.Any) -> float:
        return 10

    @double_process_mocker
    async def process(self, num: Input(AddZero)) -> t.Union[Recurrent, float]:

        if num == -5:
            return self.next_iteration(num)

        return num * 2


recurrent_double_number = RecurrentSubGraph(
    dest_node=DoubleNumber,
    start_node=InvertNumber,
    max_iterations=2,
)


class AddConst(ProcessorBase):
    def process(self, num: float = -3.0, const: float = 0.2) -> float:
        return num + const


class AddNumbers(ProcessorBase):
    def process(self, num1: Input(AddConst), num2: recurrent_double_number) -> float:
        return num1 + num2


async def test_dag(
    pipeline_context: t.Callable[..., DAGPipelineContext],
    build_dag: t.Callable[..., DAGLike],
) -> None:
    assert await build_dag(input_node=InvertNumber, output_node=AddNumbers).run(pipeline_context(num=3.0)) == 7.2

    assert double_process_mocker.mock.process.mock_calls == [
        call_object(num=-5),
        call_object(num=-5),
        call_object(num=-5),
    ]

    assert pseudo_process_mocker.mock.process.mock_calls == [
        # Основной запуск recurrent_double_number и попытка прогнать подграф в обычном режиме без зацикливания
        call_object(num=10),
        # Зацикливание another_recurrent_subgraph 1, как для обычного подграфа
        call_object(num=10),
        # Зацикливание another_recurrent_subgraph 2, как для обычного подграфа
        call_object(num=10),

        # Первый запуск recurrent_double_number в качестве рекуррентного подграфа
        call_object(num=10),
        # Зацикливание another_recurrent_subgraph 1
        call_object(num=10),
        # Зацикливание another_recurrent_subgraph 2
        call_object(num=10),

        # Второй запуск recurrent_double_number в качестве рекуррентного подграфа
        call_object(num=10),
        # Зацикливание another_recurrent_subgraph 1
        call_object(num=10),
        # Зацикливание another_recurrent_subgraph 2
        call_object(num=10),
    ]
