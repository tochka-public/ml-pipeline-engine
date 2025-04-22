import typing as t

import pytest

from tests.helpers import FactoryMocker
from tests.helpers import call_object

from ml_pipeline_engine.dag_builders.annotation.marks import Input
from ml_pipeline_engine.dag_builders.annotation.marks import RecurrentSubGraph
from ml_pipeline_engine.dag_builders.annotation.marks import SwitchCase
from ml_pipeline_engine.node import RecurrentProcessor
from ml_pipeline_engine.types import AdditionalDataT
from ml_pipeline_engine.types import PipelineChartLike
from ml_pipeline_engine.types import Recurrent

case_switch_node_mocker = FactoryMocker()


class Ident(RecurrentProcessor):
    def process(
        self,
        num: float,
        additional_data: t.Optional[AdditionalDataT] = None,  # noqa: ARG002 ignore unused
    ) -> float:
        return num


class SwitchNode(RecurrentProcessor):
    def process(self, num: Input(Ident)) -> str:
        if num < 0.0:
            return 'invert'
        if num == 0.0:
            return 'const'
        if num == 1.0:
            return 'double'
        return 'add_sub_chain'


class ConstNoInput(RecurrentProcessor):
    def process(self) -> float:
        return 10.0


class Add100(RecurrentProcessor):
    def process(self, num: Input(Ident)) -> float:
        return num + 100


class AddAConst(RecurrentProcessor):
    def process(self, num: Input(Ident), const: float = 9.0) -> float:
        return num + const


class InvertNumber(RecurrentProcessor):
    def process(self, num: Input(Ident)) -> float:
        return -num


class SubIdent(RecurrentProcessor):
    def process(self, num: Input(AddAConst), num_ident: Input(Ident)) -> float:
        return num - num_ident


class DoubleNumber(RecurrentProcessor):
    def process(self, num: Input(Ident)) -> float:
        return num * 2


SomeSwitchCase = SwitchCase(
    name='some_switch_case',
    switch=SwitchNode,
    cases=[
        ('const', ConstNoInput),
        ('double', DoubleNumber),
        ('invert', InvertNumber),
        ('add_sub_chain', SubIdent),
    ],
)


class CaseNode(RecurrentProcessor):
    def process(self, num: SomeSwitchCase, num2: Input(Ident), num3: Input(Add100)) -> float:
        return num + num2 + num3


class CaseSwitchNode(RecurrentProcessor):
    use_default = True

    def get_default(self, **__: t.Any) -> int:
        return 0

    @case_switch_node_mocker
    def process(self, num: Input(CaseNode)) -> Recurrent:
        return self.next_iteration(num)


recurrent_switch = RecurrentSubGraph(
    dest_node=CaseSwitchNode,
    start_node=Ident,
    max_iterations=2,
)


class Output(RecurrentProcessor):
    def process(self, num: recurrent_switch, num2: Input(Ident)) -> float:
        return num + num2 - num2


@pytest.mark.parametrize(
    'input_num, call_num',
    (
        (-1, 99),
        (0, 110),
        (1, 104),
        (10, 129),
    ),
)
async def test_dag__case1(
    input_num: int,
    call_num: int,
    build_chart: t.Callable[..., PipelineChartLike],
) -> None:
    case_switch_node_mocker.mock.reset_mock()

    chart = build_chart(input_node=Ident, output_node=Output)
    result = await chart.run(input_kwargs=dict(num=input_num))
    assert result.value == 0

    assert case_switch_node_mocker.mock.process.mock_calls == [
        call_object(num=call_num),
        call_object(num=call_num),
        call_object(num=call_num),
    ]
