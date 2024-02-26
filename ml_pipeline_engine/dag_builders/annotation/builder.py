import copy
import inspect
import typing as t
from collections import deque

from ml_pipeline_engine.dag import DAG, EdgeField, NodeField
from ml_pipeline_engine.dag.graph import DiGraph
from ml_pipeline_engine.dag.utils import get_connected_subgraph
from ml_pipeline_engine.dag_builders.annotation import errors
from ml_pipeline_engine.dag_builders.annotation.marks import (
    InputGenericMark,
    InputMark,
    InputOneOfMark,
    RecurrentSubGraphMark,
    SwitchCaseMark,
)
from ml_pipeline_engine.node import (
    NodeSerializer,
    NodeTag,
    generate_node_id,
    get_callable_run_method,
    get_node_id,
)
from ml_pipeline_engine.types import (
    DAGLike,
    NodeBase,
    NodeId,
    NodeLike,
    NodeSerializerLike,
    RecurrentProtocol,
)

__all__ = [
    'build_dag',
    'build_dag_single',
]

KwargName = str

NodeInputSpec = t.Tuple[KwargName, t.Union[InputMark, SwitchCaseMark]]

NodeResultT = t.TypeVar('NodeResultT')


class AnnotationDAGBuilder:
    def __init__(self):
        self._dag = DiGraph()
        self._node_map: t.Dict[NodeId, NodeSerializerLike] = dict()
        self._recurrent_sub_graphs: t.List[t.Tuple[NodeId, NodeId]] = []
        self._synthetic_nodes: t.List[NodeId] = []

    @staticmethod
    def _check_annotations(obj: t.Any) -> None:
        """
        Проверка наличия аннотаций типов у переданного объекта.
        В случае, если есть хотя бы один не типизированный параметр, будет ошибка.
        """

        obj = get_callable_run_method(obj)

        annotations = getattr(obj, '__annotations__', None)
        parameters = [
            (name, bool(parameter.empty))
            for name, parameter in inspect.signature(obj).parameters.items()
            if name not in ('self', 'args', 'kwargs')
        ]

        if not annotations and parameters:
            raise errors.UndefinedAnnotation(f'Невозможно найти аннотации типов. obj={obj}')

        for name, is_empty in parameters:
            if is_empty and name not in annotations:
                raise errors.UndefinedParamAnnotation(f'Не указан тип для параметра name={name}, obj={obj}')

    @staticmethod
    def _check_base_class(node: t.Any) -> None:
        """
        Проверка объекта на наличие корректного базового класса у всех узлов
        """

        if not inspect.isclass(node):
            raise errors.IncorrectTypeClass(f'{node} должен быть классом')

        if NodeBase not in inspect.getmro(node):
            raise errors.IncorrectBaseClass(
                f'У объекта не существует корректного базового класса, пригодного для графа. node={node}',
            )

    def validate_node(self, node: NodeLike) -> None:
        """
        Валидация ноды по разным правилам
        """

        self._check_base_class(node)
        self._check_annotations(node)

    @staticmethod
    def _get_input_marks_map(node: NodeLike) -> t.List[NodeInputSpec]:
        """
        Получение меток зависимостей для входных kwarg-ов узла
        """

        node = get_callable_run_method(node)

        inputs = []
        for name, annotation in node.__annotations__.items():  # noqa

            if isinstance(annotation, InputGenericMark):
                raise errors.NonRedefinedGenericTypeError(
                    f'Для использования узлов общего назначения необходимо их переопределение для целевого графа. '
                    f'param_name={name}, node={node}, ',
                )

            if not isinstance(annotation, (InputMark, SwitchCaseMark, InputOneOfMark, RecurrentSubGraphMark)):
                continue

            inputs.append((name, annotation))

        return inputs

    def _add_node_to_map(self, node: NodeLike) -> None:
        """
        Добавление узла в мэппинг "Имя узла -> Класс/функция узла"
        """

        self._node_map[get_node_id(node)] = NodeSerializer.serialize(node=node)

    def _add_node_pair_to_dag(self, source_node_id: NodeId, dest_node_id: NodeId, **edge_data) -> None:
        """
        Добавить в граф пару узлов, связанных ребром
        """

        self._dag.add_node(source_node_id)
        self._dag.add_node(dest_node_id)

        self._dag.add_edge(source_node_id, dest_node_id, **edge_data)

    def _add_switch_node(self, node_id: NodeId, switch_decide_node_id: NodeId) -> None:
        """
        Добавить в граф узел типа switch
        """

        self._dag.add_node(node_id, **{NodeField.is_switch: True})
        self._dag.add_edge(switch_decide_node_id, node_id, **{EdgeField.is_switch: True})

    def _traverse_breadth_first_to_dag(self, input_node: NodeLike, output_node: NodeLike):  # noqa
        """
        Выполнить обход зависимостей классов/функций узлов, построить граф
        """

        visited = {output_node}
        stack = deque([output_node])

        def _set_visited(node: NodeLike) -> None:
            if node in visited:
                return

            visited.add(node)
            stack.append(node)

        while stack:
            current_node = stack.pop()
            self.validate_node(current_node)

            self._add_node_to_map(current_node)

            input_marks_map = self._get_input_marks_map(current_node)

            if not input_marks_map and input_node != current_node:
                self._add_node_pair_to_dag(get_node_id(input_node), get_node_id(current_node))
                _set_visited(input_node)

            for kwarg_name, input_mark in input_marks_map:

                if isinstance(input_mark, RecurrentSubGraphMark):
                    self._add_node_to_map(input_mark.dest_node)
                    self._dag.add_node(
                        get_node_id(input_mark.dest_node),
                        **{
                            NodeField.start_node: get_node_id(input_mark.start_node),
                            NodeField.max_iterations: input_mark.max_iterations,
                        },
                    )
                    self._add_node_pair_to_dag(
                        get_node_id(input_mark.dest_node),
                        get_node_id(current_node),
                        **{EdgeField.kwarg_name: kwarg_name},
                    )
                    self._recurrent_sub_graphs.append(
                        (
                            get_node_id(input_mark.start_node),
                            get_node_id(input_mark.dest_node),
                        ),
                    )
                    _set_visited(input_mark.dest_node)

                if isinstance(input_mark, InputOneOfMark):
                    first_node_in_pool = input_mark.nodes[0]
                    synthetic_node_id = generate_node_id('input_one_of', get_node_id(first_node_in_pool))
                    self._dag.add_node(synthetic_node_id, **{NodeField.is_first_success: True})
                    self._dag.add_edge(get_node_id(input_node), synthetic_node_id)

                    node_list = input_mark.nodes
                    for idx, node in enumerate(node_list):
                        self._add_node_to_map(node)
                        self._dag.add_node(get_node_id(node), **{NodeField.is_first_success_pool: True})

                        if idx + 1 < len(node_list):
                            self._dag.add_edge(
                                get_node_id(node),
                                get_node_id(node_list[idx + 1]),
                                **{EdgeField.is_first_success: get_node_id(first_node_in_pool)},
                            )
                        else:
                            self._dag.add_edge(
                                get_node_id(node),
                                synthetic_node_id,
                                **{EdgeField.is_first_success: get_node_id(first_node_in_pool)},
                            )

                        _set_visited(node)

                    self._synthetic_nodes.append(synthetic_node_id)
                    self._dag.add_edge(
                        synthetic_node_id, get_node_id(current_node), **{EdgeField.kwarg_name: kwarg_name}
                    )

                if isinstance(input_mark, InputMark):
                    self._add_node_to_map(input_mark.node)
                    self._add_node_pair_to_dag(
                        get_node_id(input_mark.node), get_node_id(current_node), **{EdgeField.kwarg_name: kwarg_name}
                    )
                    _set_visited(input_mark.node)

                if isinstance(input_mark, SwitchCaseMark):

                    switch_node_id = generate_node_id('switch', input_mark.name)

                    self._add_node_to_map(input_mark.switch)
                    self._add_switch_node(switch_node_id, get_node_id(input_mark.switch))
                    _set_visited(input_mark.switch)

                    for case_branch, case_node in input_mark.cases:
                        self._add_node_to_map(case_node)
                        self._dag.add_edge(
                            get_node_id(case_node), switch_node_id, **{EdgeField.case_branch: case_branch}
                        )
                        _set_visited(case_node)

                    self._dag.add_edge(switch_node_id, get_node_id(current_node), **{EdgeField.kwarg_name: kwarg_name})
                    self._synthetic_nodes.append(switch_node_id)

    def _validate_recurrent_node_base_classes(self) -> None:
        """
        Валидация узлов из рекуррентных подграфов
        """

        for source, dest in self._recurrent_sub_graphs:
            for node_id in get_connected_subgraph(self._dag, source, dest):

                if node_id in self._synthetic_nodes:
                    continue

                node = self._node_map[node_id].get_node()

                if RecurrentProtocol not in inspect.getmro(node):
                    raise errors.IncorrectRecurrentMixinClass(
                        f'{node} не может быть узлом в рекуррентном подграфе, '
                        f'так как не унаследован от {RecurrentProtocol}',
                    )

    def _validate_recurrent_nodes_params(self) -> None:
        """
        Проверка наличия системного параметра в начале подграфа
        """

        for source, dest in self._recurrent_sub_graphs:
            if source in self._synthetic_nodes or dest in self._synthetic_nodes:
                continue

            method = get_callable_run_method(self._node_map[source].get_node())

            if 'additional_data' not in method.__annotations__:  # noqa
                raise errors.IncorrectParamsRecurrentNode(
                    f'В {method} отсутствует системный параметр "additional_data" для получения данных от узла, '
                    'который может перезапустить подграф',
                )

            dest_node = self._node_map[dest].get_node()
            if not dest_node.use_default:
                raise errors.IncorrectParamsRecurrentNode(
                    f'Для участия в рекуррентном подграфе {dest_node} должен устанавливать параметр use_default=True. '
                    'Дополнительно должен быть переопределен метод get_default()'
                )

    def _validate_graph(self) -> None:
        """
        Метод для валидации построенного графа
        """

        self._validate_recurrent_node_base_classes()
        self._validate_recurrent_nodes_params()

    def _is_executor_needed(self) -> t.Tuple[bool, bool]:
        """
        Проверяем надобность пулов для узлов
        """

        is_thread_pool_needed = False
        is_process_pool_needed = False

        for node in self._node_map.values():
            node = node.get_node()

            if inspect.iscoroutinefunction(get_callable_run_method(node)):
                continue

            if NodeTag.process in node.tags:
                is_process_pool_needed = True
            else:
                is_thread_pool_needed = True

        return is_process_pool_needed, is_thread_pool_needed

    def build(self, input_node: NodeLike, output_node: NodeLike = None) -> DAGLike:
        """
        Построить граф путем сборки зависимостей по аннотациям типа (меткам входов)
        """

        self._add_node_to_map(input_node)

        if output_node is None:
            output_node = input_node
            self._dag.add_node(get_node_id(input_node))
        else:
            self._traverse_breadth_first_to_dag(input_node, output_node)

        self._validate_graph()

        is_process_pool_needed, is_thread_pool_needed = self._is_executor_needed()

        return DAG(
            graph=self._dag.copy(),
            input_node=get_node_id(input_node),
            output_node=get_node_id(output_node),
            node_map=copy.deepcopy(self._node_map),
            is_process_pool_needed=is_process_pool_needed,
            is_thread_pool_needed=is_thread_pool_needed,
        )


def build_dag(
    input_node: NodeLike[t.Any],
    output_node: NodeLike[NodeResultT],
) -> DAGLike[NodeResultT]:
    """
    Построить граф путем сборки зависимостей по аннотациям типа (меткам входов)

    Args:
        input_node: Входной узел
        output_node: Выходной узел

    Returns:
        Граф
    """

    return (
        AnnotationDAGBuilder()
        .build(input_node=input_node, output_node=output_node)
    )


def build_dag_single(node: NodeLike[NodeResultT]) -> DAGLike[NodeResultT]:
    """
    Построить граф из одного узла

    Args:
        node: Узел

    Returns:
        Граф
    """
    return AnnotationDAGBuilder().build(input_node=node, output_node=None)
