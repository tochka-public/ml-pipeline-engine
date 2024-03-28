import asyncio
import functools
import typing as t
from collections import deque
from contextlib import suppress
from dataclasses import dataclass, field

import networkx as nx

from ml_pipeline_engine.dag.enums import EdgeField, NodeField
from ml_pipeline_engine.dag.graph import DiGraph
from ml_pipeline_engine.dag.retrying import DagRetryPolicy
from ml_pipeline_engine.dag.utils import get_connected_subgraph
from ml_pipeline_engine.exceptions import NodeErrorType
from ml_pipeline_engine.dag.storage import DAGNodeStorage
from ml_pipeline_engine.node import run_node, run_node_default
from ml_pipeline_engine.types import (
    CaseResult,
    DAGLike,
    DAGRunManagerLike,
    NodeId,
    NodeResultT,
    PipelineContextLike,
    Recurrent,
    DAGRunLockManagerLike,
    DAGNodeStorageLike,
)

from ml_pipeline_engine.logs import logger_manager as logger
from cachetools import cachedmethod
from cachetools.keys import hashkey


class DAGConcurrentManagerLock(DAGRunLockManagerLike):

    def __init__(self, node_ids: t.Iterable[NodeId]):
        self.lock_store = {node_id: asyncio.Event() for node_id in node_ids}

    def get_lock(self, node_id: NodeId) -> asyncio.Event:
        self.lock_store.setdefault(node_id, asyncio.Event())
        return self.lock_store[node_id]


def cache_key(prefix, _, *args, **kwargs) -> t.Type[tuple]:
    """Custom func key generation excluding 'self'."""
    return hashkey(*args, prefix, **kwargs)


@dataclass
class DAGRunConcurrentManager(DAGRunManagerLike):
    """
    Дефолтный менеджер запуска графов.
    Производит конкурентное исполнение узлов.
    """

    lock_manager: DAGRunLockManagerLike
    ctx: PipelineContextLike
    dag: DAGLike
    node_storage: DAGNodeStorageLike = field(default_factory=DAGNodeStorage)

    _memorization_store: t.Dict[t.Any, t.Any] = field(default_factory=dict)

    async def run(self) -> NodeResultT:
        return await self._run_dag(self._get_reduced_dag(self.dag.input_node, self.dag.output_node))

    def _get_node_kwargs(self, node_id: NodeId) -> t.Dict[str, t.Any]:
        """
        Получить входные kwarg-и для узла графа перед его запуском
        """

        kwargs = {}

        if node_id != self.dag.input_node:
            for pred_node_id in self.dag.graph.predecessors(node_id):
                kwarg_name = self.dag.graph.edges[(pred_node_id, node_id)].get(EdgeField.kwarg_name)

                if kwarg_name is None:
                    continue

                if self.dag.graph.nodes[pred_node_id].get(NodeField.is_switch):
                    kwargs[kwarg_name] = self.node_storage.get_node_result(
                        self.node_storage.get_switch_result(pred_node_id).node_id,
                    )
                    continue

                kwargs[kwarg_name] = self.node_storage.get_node_result(
                    pred_node_id,
                )

        else:
            kwargs = self.ctx.input_kwargs

        additional_data = self.dag.graph.nodes[node_id].get(NodeField.additional_data)

        if additional_data:
            kwargs[NodeField.additional_data] = additional_data

        return kwargs

    @cachedmethod(lambda self: self._memorization_store, key=functools.partial(cache_key, 'is_switch'))
    def _is_switch(self, dag: DiGraph, node_id: NodeId) -> bool:
        """
        Является ли узел switch-узлом
        """

        try:
            return dag.nodes[node_id].get(NodeField.is_switch) is True
        except KeyError:
            return False

    @cachedmethod(lambda self: self._memorization_store, key=functools.partial(cache_key, 'node_in_oneof'))
    def _is_node_in_oneof(self, dag: DiGraph, node_id: NodeId) -> bool:
        """
        Является ли узел узлом InputOneOf пула
        """
        return bool(dag.nodes[node_id].get(NodeField.is_first_success_pool))

    @cachedmethod(lambda self: self._memorization_store, key=functools.partial(cache_key, 'head_of_oneof'))
    def _is_head_of_oneof(self, dag: DiGraph, node_id: NodeId) -> bool:
        """
        Является ли узел родителем InputOneOf пула
        """
        return bool(dag.nodes[node_id].get(NodeField.is_first_success))

    def _get_reduced_dag(self, source: NodeId, dest: NodeId) -> DiGraph:
        """
        Получить связный подграф между двумя заданными узлами графа с удаленными ребрами условных операторов
        """

        def _filter(u, v) -> bool:
            """
            Удаляет ребра с меткой EdgeField.case_branch из subgraph_view

            Args:
                u - Node
                v - Node Edge
            """
            return not self.dag.graph.edges[u, v].get(EdgeField.case_branch)

        def _filter_node(u) -> bool:
            """
            Удаляет ноды с меткой NodeField.is_first_success_pool из subgraph_view

            Args:
                u -  Node
            """
            return not self.dag.graph.nodes[u].get(NodeField.is_first_success_pool)

        return get_connected_subgraph(
            nx.subgraph_view(self.dag.graph, filter_edge=_filter, filter_node=_filter_node), source, dest
        )

    def _get_reduced_dag_input_one_of(self, source: NodeId, dest: NodeId) -> DiGraph:
        """
        Получить связный подграф между двумя заданными узлами графа InputOneOf
        """

        return get_connected_subgraph(nx.subgraph_view(self.dag.graph), source, dest)

    def _add_case_result(self, switch_node_id: NodeId) -> None:
        """
        Записать выбор, сделанный оператором выбора Switch-Case, в контекст
        """
        selected_branch_label = None
        branch_nodes = {}
        for pred_id in self.dag.graph.predecessors(switch_node_id):
            edge = self.dag.graph.edges[(pred_id, switch_node_id)]

            if edge.get(EdgeField.is_switch):
                selected_branch_label = self.node_storage.get_node_result(pred_id)
                continue

            branch_nodes[edge.get(EdgeField.case_branch)] = pred_id

        self.node_storage.set_switch_result(
            switch_node_id,
            CaseResult(label=selected_branch_label, node_id=branch_nodes[selected_branch_label]),
        )

    async def _run_node(
        self,
        node_id: NodeId,
        is_node_from_success_pool: bool,
        force_default: bool = False,
    ) -> t.Union[NodeResultT, t.Any]:
        """
        Выполнить узел графа
        """

        # Due to concurrent nature, we have to check if the node is running concurrently in another subgraph.
        # In general, we have two checks: before execution and here. This check prevents a double run.
        # when the deep-nested node in the subgraph has been executed.
        if self.node_storage.exists_processed_node(node_id):
            logger.debug('Node %s has been executed. Stop new execution', node_id)

            event_lock = self.lock_manager.get_lock(node_id)
            await event_lock.wait()

            return self.node_storage.get_node_result(node_id)

        self.node_storage.set_node_as_processed(node_id)
        await self.ctx.emit_on_node_start(node_id=node_id)

        try:
            logger.info('Начало исполнения ноды, node_id=%s', node_id)
            input_kwargs = self._get_node_kwargs(node_id)

            # Если в узел попадают параметры, которые являются ошибками, то исполнять этот узел нельзя.
            # Как следствие, нам нужно завершить исполнение текущей ноды
            for key_value in input_kwargs.values():
                if isinstance(key_value, Exception):
                    raise key_value

            result = await self.execute(
                node_id=node_id,
                force_default=force_default,
                **input_kwargs,
            )

            if isinstance(result, Exception):
                await self.ctx.emit_on_node_complete(node_id=node_id, error=result)
            else:
                await self.ctx.emit_on_node_complete(node_id=node_id, error=None)

            logger.info('Завершение исполнения ноды, node_id=%s', node_id)
            return result

        except Exception as ex:
            await self.ctx.emit_on_node_complete(node_id=node_id, error=ex)
            logger.error('Ошибка исполнения ноды node_id=%s, err=%s', node_id, ex)

            if is_node_from_success_pool:
                return NodeErrorType.succession_node_error

            raise ex

    async def execute(
        self,
        node_id: NodeId,
        force_default: bool,
        **kwargs,
    ) -> t.Union[NodeResultT, t.Any]:
        """
        Запуск ноды с дополнительной обработкой

        Args:
            node_id: Идентификатор ноды
            force_default: Отдать дефолтное значение
            **kwargs: Ключевые аргументы целевой функции
        """

        node = self.dag.node_map[node_id]

        retry_policy = DagRetryPolicy(node=node)

        n_attempts = 1
        while True:
            try:
                logger.debug('Начало исполнения узла node_id=%s', node_id)
                if force_default:
                    return run_node_default(node, **kwargs)

                return await run_node(**kwargs, node=node)

            except retry_policy.exceptions as error:
                logger.debug(
                    'Node %s will be restarted in %s seconds...',
                    node_id,
                    retry_policy.delay,
                    exc_info=error,
                )

                if n_attempts == retry_policy.attempts:

                    if retry_policy.use_default:
                        return run_node_default(node, **kwargs)

                    raise error

                await self.ctx.emit_on_node_complete(node_id=node_id, error=error)

                n_attempts += 1
                await asyncio.sleep(retry_policy.delay)

            except Exception:
                if retry_policy.use_default:
                    return run_node_default(node, **kwargs)
                raise

    def _get_call_order(self, dag: DiGraph) -> t.List[NodeId]:
        """
        Рассчитываем план выполнения узлов графа.
        Учитываем, что если подграф находится в активной рекурсии, то исполнять нужно все узлы
        """

        return [
            node_id for node_id in nx.topological_sort(dag)
            if (
                not self.node_storage.exists_processed_node(node_id)
                if not dag.is_recurrent
                else True
            )
        ]

    @cachedmethod(lambda self: self._memorization_store, key=functools.partial(cache_key, 'node_dependencies'))
    def _get_node_dependencies(self, dag: DiGraph, node_id: NodeId) -> t.Set[NodeId]:
        """
        Метод возвращает узлы без которых оператор switch-case, oneof не запустится в указанном dag.
        """

        node_predecessors = set(self.dag.graph.predecessors(node_id))
        current_dag = set(nx.topological_sort(dag))

        return current_dag.intersection(node_predecessors)

    def _get_predecessors(self, dag: DiGraph, node_id: NodeId) -> t.List[NodeId]:
        """
        Получение всех предшественников для узла
        """
        predecessors = list(
            self._get_node_dependencies(dag, node_id)
            if self._is_switch(dag, node_id) or self._is_head_of_oneof(dag, node_id) or dag.is_recurrent
            else self.dag.graph.predecessors(node_id)
        )

        for idx, node_id in enumerate(predecessors):

            if self._is_switch(dag, node_id):
                with suppress(KeyError):
                    predecessors[idx] = self.node_storage.get_switch_result(node_id).node_id

        return predecessors

    async def _is_ready_to_run(self, dag: DiGraph, node_id: NodeId) -> bool:
        """
        Метод проверяет возможность запуска узла
        """

        # Switch context to collect any predecessors' results to approve the function for running.
        await asyncio.sleep(0)

        if self.node_storage.exists_processed_node(node_id):
            return False

        for pred_node_id in self._get_predecessors(dag, node_id):

            if not self.node_storage.exists_node_result(pred_node_id):
                return False

        return True

    async def _run_dag(self, dag: DiGraph) -> t.Union[NodeResultT, t.Any]:
        """
        Запустить граф / подграф
        """

        logger.debug('Начало запуска DAG, dag=%s', str(dag))

        list_node_ids = self._get_call_order(dag)

        if dag.is_recurrent:
            self.node_storage.hide_last_execution(*list_node_ids)

        if len(list_node_ids) == 0:
            return

        dag_output_node = list_node_ids[-1]
        list_node_ids = deque(list_node_ids)

        awaitable_nodes = []

        while list_node_ids:
            node_id = list_node_ids.popleft()

            # Sometimes we can encounter situations when we have a result of the node-id.
            # In that case, we simply skip the node because:
            #   1. If it's a recurrent subgraph, the result would be deleted for each node in the subgraph
            #      and this if-statement won't be executed.
            #   2. If it's a non-recurrent execution, we just assume that the node's done before in any other graphs.
            if self.node_storage.exists_processed_node(node_id):
                continue

            elif await self._is_ready_to_run(dag, node_id):
                logger.debug('Узел готов к обработке node_id=%s', node_id)

                if self._is_switch(dag, node_id):
                    logger.debug('Узел является switch-кейсом node_id=%s', node_id)
                    self._add_case_result(node_id)

                    coro_to_run = asyncio.create_task(
                        self._run_dag(
                            dag=self._get_reduced_dag(
                                self.dag.input_node,
                                (self.node_storage.get_switch_result(node_id)).node_id,
                            ),
                        ),
                        name=node_id,
                    )

                elif dag.nodes[node_id].get(NodeField.is_first_success):
                    logger.debug('Узел является InputOneOf node_id=%s', node_id)

                    coro_to_run = asyncio.create_task(
                        self._run_dag(
                            dag=self._get_reduced_dag_input_one_of(
                                self.dag.input_node,
                                node_id,
                            ),
                        ),
                        name=node_id,
                    )

                else:
                    logger.debug('Узел может быть обработан, отправляем его в обработку node_id=%s', node_id)
                    coro_to_run = asyncio.create_task(
                        self._run_node(
                            node_id,
                            # Добавляем флаг, что выполняется нода из InputOneOf пула
                            is_node_from_success_pool=self._is_node_in_oneof(dag, node_id),
                        ),
                        name=node_id,
                    )

                awaitable_nodes.append(coro_to_run)

                # Если мы только запустили последний узел, то результат необходимо получить из соседнего процесса,
                # по этой причине перезапускаем узел для повторной обработки
                if node_id == dag_output_node:
                    list_node_ids.appendleft(node_id)

            else:
                pending_nodes = awaitable_nodes

                while pending_nodes:
                    node_results, pending_nodes = await asyncio.wait(
                        pending_nodes, return_when=asyncio.FIRST_COMPLETED,
                    )

                    for node_result in node_results:
                        node_result_id = node_result.get_name()
                        node_result = await node_result

                        if isinstance(node_result, Recurrent):
                            max_iterations = dag.nodes[node_result_id].get(NodeField.max_iterations)
                            start_from_node_id = dag.nodes[node_result_id].get(NodeField.start_node)

                            if not self.node_storage.exists_active_rec_subgraph(start_from_node_id, node_result_id):
                                self.node_storage.set_active_rec_subgraph(start_from_node_id, node_result_id)

                                logger.debug(
                                    'Start the process of the recurrent subgraph for nodes start_node=%s, dest_node=%s',
                                    start_from_node_id,
                                    node_result_id,
                                )

                                recurrent_subgraph = get_connected_subgraph(
                                    self.dag.graph, start_from_node_id, node_result_id, is_recurrent=True,
                                )

                                for current_iter in range(max_iterations):
                                    logger.debug(
                                        'Executing the %s attempt of the recurrent subgraph start_node=%s, dest_node=%s',
                                        current_iter,
                                        start_from_node_id,
                                        node_result_id,
                                    )
                                    self.dag.graph.nodes[start_from_node_id][NodeField.additional_data] = node_result.data

                                    node_result = await self._run_dag(dag=recurrent_subgraph)

                                    if current_iter + 1 == max_iterations and isinstance(node_result, Recurrent):

                                        logger.debug(
                                            'Attempts to run a recurrent subgraph have been exceeded. '
                                            'Will be used the default value start_node=%s, dest_node=%s',
                                            start_from_node_id,
                                            node_result_id,
                                        )

                                        self.node_storage.hide_last_execution(node_result_id)

                                        node_result = await self._run_node(
                                            node_result_id,
                                            is_node_from_success_pool=self._is_node_in_oneof(dag, node_result_id),
                                            force_default=True,
                                        )

                                    elif not isinstance(node_result, Recurrent):
                                        break

                                self.node_storage.delete_active_rec_subgraph(start_from_node_id, node_result_id)

                            else:
                                # В случае, если исполняемый узел рекуррентного подграфа не завершается ожидаемым образом,
                                # то его нужно вернуть обратно к управляющей конструкции
                                logger.debug(
                                    'Finish the process of the recurrent subgraph for nodes '
                                    'start_node=%s, dest_node=%s',
                                    start_from_node_id,
                                    node_result_id,
                                )
                                return node_result

                        # Если нода из списка InputOneOf выполнилась успешно, то проходить остальные не нужно.
                        # При этом, результат сохраняется под выходным узлом
                        if (
                            not isinstance(node_result, NodeErrorType)
                            and self._is_node_in_oneof(dag, node_result_id)
                        ):
                            self.node_storage.set_node_result(dag_output_node, node_result)
                            # Для искусственной остановки пайплайна помечаем текущий узел как выходной
                            node_result_id = dag_output_node

                        self.node_storage.set_node_result(node_result_id, node_result)
                        self.lock_manager.get_lock(node_result_id).set()

                        # FIXME: Set new abstractions for event managers
                        await self.ctx.save_node_result(node_result_id, node_result)

                        if node_result_id == dag_output_node:
                            return self.node_storage.get_node_result(node_result_id, with_hidden=True)

                awaitable_nodes.clear()
                list_node_ids.appendleft(node_id)

    def __repr__(self):
        return f'<{self.__class__.__name__} nnodes="{len(self.dag.graph.nodes)}" nedges="{len(self.dag.graph.edges)}">'
