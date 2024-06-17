import typing as t
from dataclasses import dataclass

from ml_pipeline_engine.types import CaseLabel
from ml_pipeline_engine.types import NodeBase

NodeResultT = t.TypeVar('NodeResultT')


@dataclass(frozen=True)
class InputGenericMark:
    node: NodeBase[t.Any]


@dataclass(frozen=True)
class InputMark:
    node: NodeBase[t.Any]


@dataclass(frozen=True)
class InputOneOfMark:
    nodes: t.List[NodeBase[t.Any]]


def InputOneOf(nodes: t.List[NodeBase[NodeResultT]]) -> t.Type[NodeResultT]:  # noqa:  N802,RUF100
    """
    Принимает список нод, возвращает результат первой успешно выполненной ноды
    """
    return t.cast(t.Any, InputOneOfMark(nodes))


def InputGeneric(node: NodeBase[NodeResultT]) -> t.Type[NodeResultT]:  # noqa:  N802,RUF100
    return t.cast(t.Any, InputGenericMark(node))


def Input(node: NodeBase[NodeResultT]) -> t.Type[NodeResultT]:  # noqa:  N802,RUF100
    return t.cast(t.Any, InputMark(node))


@dataclass(frozen=True)
class GenericInputMark:
    node: NodeBase[t.Any]


def GenericInput(node: NodeBase[NodeResultT]) -> t.Type[NodeResultT]:  # noqa:  N802,RUF100
    return t.cast(t.Any, GenericInputMark(node))


@dataclass(frozen=True)
class SwitchCaseMark:
    switch: NodeBase[t.Any]
    cases: t.List[t.Tuple[str, NodeBase]]
    name: str


def SwitchCase(  # noqa:  N802,RUF100
    switch: NodeBase[t.Any],
    cases: t.List[t.Tuple[CaseLabel, NodeBase[NodeResultT]]],
    name: t.Optional[str] = None,
) -> t.Type[NodeResultT]:
    return t.cast(t.Any, SwitchCaseMark(switch, cases, name))


@dataclass(frozen=True)
class RecurrentSubGraphMark:
    start_node: NodeBase[NodeResultT]
    dest_node: NodeBase[NodeResultT]
    max_iterations: int


def RecurrentSubGraph(  # noqa:  N802,RUF100
    start_node: t.Type[NodeBase[NodeResultT]],
    dest_node: t.Type[NodeBase[NodeResultT]],
    max_iterations: int,
) -> t.Type[NodeResultT]:
    """
    Указание рекуррентного подграфа.

    Задача:
        Исполнить некий подграф `A — B — C` с передачей дополнительных данных в узел `A`
        для изменения поведения всего подграфа и исполнить целевой узел `C` с измененным поведением.

    Инициализация подграфа:
        1. Для инициализации подграфа нужно вызвать self.next_iteration().
        2. Подграф будет исполняться до крайнего случая:
            1. Целевой узел больше не инициализирует self.next_iteration() и достигает необходимых условий исполнения
            2. Количество итераций закончилось и на выход отдается self.get_default()
        3. Узел, с которого начинается подграф, должен иметь в параметрах additional_data для получения данных
           из узла, который инициализировал повторное исполнение подграфа
        4. Инициализация узла возможна только из целевого узла (dest_node). TODO: В будущем мб этот функционал
                                                                                  будет расширен

    Условия:
        1. Целевой подграф должен иметь RecurrentProtocol в базовом классе.
        2. Целевой узел должен определять метод self.get_default()
        3. Целевой узел должен явно задавать use_default=True

    Комбинация с RetryProtocol:
        1. При наличии ошибок системного характера каждый узел может исполниться cls.attempts раз
        2. Классы ошибок можно настроить через cls.exceptions
        3. В случае, если use_default=False и количество попыток исчерпано — будет выкинуто исключение
        4. В случае, если use_default=True — будет вызван метод self.get_default()

    Кейсы:
        1. use_default=True
            1. Если в подграфе в любом узле возникла ошибка, то из узла будет взято дефолтное значение
            2. Если целевой узел достигает условий даже при всех дефолтных значениях предшествующих узлов, то
               этот кейс считается успешным и рекуррентный подграф завершает работу
            3. Если целевой подграф исполняется с ошибкой, то исполняется self.get_default()

        2. use_default=False
            1. Если любой узел подграфа завершается ошибкой, то весь граф завершается ошибкой

    Args:
        start_node: Узел с которого будет начинаться рекуррентный подграф.
        dest_node: Узел которым будет заканчиваться рекуррентный подграф.
        max_iterations: Количество перезапусков подграфа при невыполнении условий.
                        Вызывается с помощью self.next_iteration().
    """
    return t.cast(t.Any, RecurrentSubGraphMark(start_node, dest_node, max_iterations))
