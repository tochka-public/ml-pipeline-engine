import asyncio
import functools
import typing as t
from collections import defaultdict
from contextlib import suppress
from dataclasses import dataclass
from dataclasses import field

import networkx as nx
from cachetools import cachedmethod
from cachetools.keys import hashkey

from ml_pipeline_engine.dag.enums import EdgeField
from ml_pipeline_engine.dag.enums import NodeField
from ml_pipeline_engine.dag.errors import OneOfDoesNotHaveResultError
from ml_pipeline_engine.dag.errors import RecurrentSubgraphDoesNotHaveResultError
from ml_pipeline_engine.dag.graph import DiGraph
from ml_pipeline_engine.dag.graph import get_connected_subgraph
from ml_pipeline_engine.dag.storage import DAGNodeStorage
from ml_pipeline_engine.logs import logger_manager as logger
from ml_pipeline_engine.logs import logger_manager_lock as lock_logger
from ml_pipeline_engine.node import NodeTag
from ml_pipeline_engine.node import run_node
from ml_pipeline_engine.node import run_node_default
from ml_pipeline_engine.node.retrying import NodeRetryPolicy
from ml_pipeline_engine.types import CaseResult
from ml_pipeline_engine.types import DAGLike
from ml_pipeline_engine.types import DAGRunManagerLike
from ml_pipeline_engine.types import NodeId
from ml_pipeline_engine.types import NodeResultT
from ml_pipeline_engine.types import PipelineContextLike
from ml_pipeline_engine.types import Recurrent

_EventDictT = t.Dict[t.Any, asyncio.Event]
_ConditionT = t.Dict[t.Any, asyncio.Condition]


@dataclass
class DAGConcurrentManagerLock:
    node_ids: t.Iterable[NodeId]
    event_lock_store: _EventDictT = field(
        default_factory=functools.partial(defaultdict, asyncio.Event),
    )
    condition_lock_store: _ConditionT = field(
        default_factory=functools.partial(defaultdict, asyncio.Condition),
    )

    @property
    def conditions(self) -> _ConditionT:
        return self.condition_lock_store

    @property
    def events(self) -> _EventDictT:
        return self.event_lock_store

    async def wait_for_event(self, event_name: str) -> None:
        await self.events[event_name].wait()

    def unlock_event(self, event_name: str) -> None:
        self.events[event_name].set()

    async def wait_for_condition(self, condition_name: t.Any, condition: t.Callable) -> None:
        cond = self.conditions[condition_name]

        async with cond:
            lock_logger.debug('Lock %s', condition_name)
            await cond.wait_for(condition)

        lock_logger.debug('Unlock %s', condition_name)

    async def unlock_condition(self, condition_name: t.Any) -> None:
        """
        Send a notification to waiters
        """

        condition = self.conditions[condition_name]

        async with condition:
            lock_logger.debug('Send a notification to unlock %s', condition_name)
            condition.notify_all()


def cache_key(prefix: str, _: t.Any, *args: t.Any, **kwargs: t.Any) -> t.Type[tuple]:
    """Custom func key generation excluding 'self'."""
    return hashkey(*args, prefix, **kwargs)


@dataclass
class DAGRunConcurrentManager(DAGRunManagerLike):
    """
    Дефолтный менеджер запуска графов.
    Производит конкурентное исполнение узлов.
    """

    ctx: PipelineContextLike
    dag: DAGLike

    _node_storage: DAGNodeStorage = field(default_factory=DAGNodeStorage)
    _lock_manager: DAGConcurrentManagerLock = field(init=False)
    _memorization_store: t.Dict[t.Any, t.Any] = field(default_factory=dict)
    _coro_tasks: t.Set[asyncio.Task] = field(default_factory=set)
    _alias_run_method: str = 'run'

    def __post_init__(self) -> None:
        self._lock_manager = DAGConcurrentManagerLock(self.dag.node_map.keys())

    @staticmethod
    def _stop_coro_tasks(*coro_tasks: asyncio.Task) -> None:
        """
        Stop running the coro tasks
        """

        for coro_task in coro_tasks:
            if coro_task.done() or coro_task.cancelled():
                continue

            coro_task.cancel()
            logger.debug('Task %s has been cancelled', coro_task.get_name())

    @staticmethod
    def _get_first_error_in_tasks(coro_tasks: t.Iterable[asyncio.Task]) -> t.Optional[t.Type[Exception]]:
        """
        Check if there is an error in the coro tasks and return the first one
        """

        for coro_task in coro_tasks:
            if coro_task.done() and isinstance(coro_task.exception(), BaseException):
                return coro_task.exception()

        return None

    def _create_task(self, coro: t.Coroutine, name: str) -> asyncio.Task:
        """
        Create asyncio.Task and collect it to the main DAG's storage
        """

        task = asyncio.create_task(coro, name=name)
        self._coro_tasks.add(task)

        return task

    async def run(self) -> NodeResultT:
        """
        Run the main DAG
        """

        try:
            self._create_task(
                self._run_dag(
                    self._get_reduced_dag(self.dag.input_node, self.dag.output_node),
                ),
                self._alias_run_method,
            )

            await self._lock_manager.wait_for_condition(
                self._alias_run_method,
                lambda: (
                    bool(self._get_first_error_in_tasks(self._coro_tasks))
                    or self._node_storage.exists_node_result(self.dag.output_node)
                ),
            )

            return self._get_dag_result()
        except Exception as ex:
            logger.error('DAG run raised error', exc_info=ex)
            raise
        finally:
            self._stop_coro_tasks(*self._coro_tasks)

    def _get_dag_result(self) -> NodeResultT:
        """
        Get the DAG's result or raise an error if there are any errors
        """

        logger.debug('Getting the result of the dag')

        error = self._get_first_error_in_tasks(self._coro_tasks)
        if error:
            raise error

        return self._node_storage.get_node_result(self.dag.output_node, with_hidden=True)

    def _get_node_kwargs(self, node_id: NodeId) -> t.Dict[str, t.Any]:
        """
        Get the node's dependencies that are needed to run the node
        """

        kwargs = {}

        if node_id != self.dag.input_node:
            for pred_node_id in self.dag.graph.predecessors(node_id):
                kwarg_name = self.dag.graph.edges[(pred_node_id, node_id)].get(EdgeField.kwarg_name)

                if kwarg_name is None:
                    continue

                if self._is_switch(pred_node_id):
                    kwargs[kwarg_name] = self._node_storage.get_node_result(
                        self._node_storage.get_switch_result(pred_node_id).node_id,
                        with_hidden=True,
                    )

                else:
                    kwargs[kwarg_name] = self._node_storage.get_node_result(
                        pred_node_id,
                        with_hidden=True,
                    )

        else:
            kwargs = self.ctx.input_kwargs

        additional_data = self.dag.graph.nodes[node_id].get(NodeField.additional_data)

        if additional_data is not None:
            kwargs[NodeField.additional_data] = additional_data

        return kwargs

    def _is_switch(self, node_id: NodeId) -> bool:
        """
        Checks if the node is a switch
        """

        try:
            return self.dag.graph.nodes[node_id].get(NodeField.is_switch) is True
        except KeyError:
            return False

    def _is_head_of_oneof(self, node_id: NodeId) -> bool:
        """
        Checks if the node is the head of OneOf
        """
        return bool(self.dag.graph.nodes[node_id].get(NodeField.is_oneof_head))

    def _is_skipped_for_storage(self, node_id: NodeId) -> bool:
        """
        Check if a node is tagged to skip result saving to artifact storage.
        """
        node = self.dag.node_map[node_id]
        tags = node.tags or ()
        return NodeTag.skip_store in tags

    def _get_reduced_dag(
        self,
        source: NodeId,
        dest: NodeId,
        is_recurrent: bool = False,
        is_oneof: bool = False,
        is_nested_oneof: bool = False,
    ) -> DiGraph:
        """
        Get filtered and connected subgraph
        """

        def _filter(u: str, v: str) -> bool:
            """
            Delete edges with EdgeField.case_branch from subgraph_view

            Args:
                u - Node
                v - Node Edge
            """
            return not self.dag.graph.edges[u, v].get(EdgeField.case_branch)

        def _filter_node(u: str) -> bool:
            """
            Delete nodes with NodeField.is_oneof_child from subgraph_view

            Args:
                u -  Node
            """
            return not self.dag.graph.nodes[u].get(NodeField.is_oneof_child)

        if is_oneof:
            self.dag.graph.nodes[dest][NodeField.is_oneof_child] = False

        return get_connected_subgraph(
            dag=nx.subgraph_view(self.dag.graph, filter_edge=_filter, filter_node=_filter_node),
            source=source,
            dest=dest,
            is_recurrent=is_recurrent,
            is_oneof=is_oneof,
            is_nested_oneof=is_nested_oneof,
        )

    def _add_case_result(self, switch_node_id: NodeId) -> None:
        """
        Save the switch branch
        """

        selected_branch_label = None
        branch_nodes = {}
        for pred_id in self.dag.graph.predecessors(switch_node_id):
            edge = self.dag.graph.edges[(pred_id, switch_node_id)]

            if edge.get(EdgeField.is_switch):
                selected_branch_label = self._node_storage.get_node_result(pred_id)
                continue

            branch_nodes[edge.get(EdgeField.case_branch)] = pred_id

        self._node_storage.set_switch_result(
            switch_node_id,
            CaseResult(label=selected_branch_label, node_id=branch_nodes[selected_branch_label]),
        )

    async def _execute_node(
        self,
        dag: DiGraph,
        node_id: NodeId,
        force_default: bool = False,
    ) -> t.Union[NodeResultT, t.Any]:
        """
        Execute node and save metadata
        """

        if self._node_storage.exists_processed_node(node_id):
            logger.debug('Node %s has been executed. Stop new execution', node_id)

            await self._lock_manager.wait_for_condition(
                node_id,
                functools.partial(self._node_storage.exists_node_result, node_id),
            )
            return self._node_storage.get_node_result(node_id)

        self._node_storage.set_node_as_processed(node_id)
        await self.ctx.emit_on_node_start(node_id=node_id)

        try:
            logger.info('Preparing node for the execution, node_id=%s', node_id)

            result = await self.__execute_node(
                node_id=node_id,
                force_default=force_default,
                **self._get_node_kwargs(node_id),
            )

            await self.ctx.emit_on_node_complete(node_id=node_id, error=None)

            logger.info('Getting the result after the execution, node_id=%s', node_id)
            return result

        except Exception as ex:
            await self.ctx.emit_on_node_complete(node_id=node_id, error=ex)
            logger.error('Execution error node_id=%s', node_id, exc_info=ex)

            if dag.is_oneof:
                return ex

            raise ex

    async def __execute_node(
        self,
        node_id: NodeId,
        force_default: bool = False,
        **kwargs: t.Any,
    ) -> t.Union[NodeResultT, t.Any]:
        """
        Execute the node by node id with retry protocol

        Args:
            node_id: Node id
            force_default: If the node should return default result
            **kwargs: Key value args for the node
        """

        node = self.dag.node_map[node_id]

        retry_policy = NodeRetryPolicy(node=node)

        n_attempts = 1
        while True:
            try:
                if force_default:
                    return run_node_default(node, **kwargs)

                logger.debug('Start execution node_id=%s', node_id)
                result = await run_node(**kwargs, node=node, node_id=node_id)

                logger.debug('Finish the node execution, node_id=%s', node_id)
                return result

            except retry_policy.exceptions as error:  # noqa: PERF203
                logger.debug(
                    'Node %s will be restarted in %s seconds...',
                    node_id,
                    retry_policy.delay,
                    exc_info=error,
                )

                if n_attempts == retry_policy.attempts:
                    if node.use_default:
                        return run_node_default(node, **kwargs)

                    raise error

                await self.ctx.emit_on_node_complete(node_id=node_id, error=error)

                n_attempts += 1
                await asyncio.sleep(retry_policy.delay)

            except Exception:
                if node.use_default:
                    return run_node_default(node, **kwargs)

                raise

    def _get_node_order(self, dag: DiGraph) -> t.List[NodeId]:
        """
        Calculate the order for nodes according to the dag's type
        """

        return [
            node_id
            for node_id in nx.topological_sort(dag)
            if (not self._node_storage.exists_processed_node(node_id) if not dag.is_recurrent else True)
        ]

    @cachedmethod(lambda self: self._memorization_store, key=functools.partial(cache_key, 'node_dependencies'))
    def _get_node_dependencies(self, dag: DiGraph, node_id: NodeId) -> t.Set[NodeId]:
        """
        Get the node's dependencies
        """

        node_predecessors = set(self.dag.graph.predecessors(node_id))
        current_dag = set(nx.topological_sort(dag))

        return current_dag.intersection(node_predecessors)

    def _get_predecessors(self, dag: DiGraph, node_id: NodeId) -> t.List[NodeId]:
        """
        Get the node's predecessors
        """

        predecessors = list(
            self._get_node_dependencies(dag, node_id)
            if self._is_switch(node_id) or self._is_head_of_oneof(node_id) or dag.is_recurrent
            else self.dag.graph.predecessors(node_id),
        )

        for idx, predecessors_node_id in enumerate(predecessors):
            if self._is_switch(predecessors_node_id):
                with suppress(KeyError, AttributeError):
                    predecessors[idx] = self._node_storage.get_switch_result(predecessors_node_id).node_id

        return predecessors

    def _is_ready_to_execute(self, dag: DiGraph, node_id: NodeId) -> bool:
        """
        Check if the node is read to be executed
        """

        logger.debug('Checking if the node can be executed node_id=%s', node_id)

        for pred_node_id in self._get_predecessors(dag, node_id):
            if (
                not self._node_storage.exists_node_result(pred_node_id)
                # The node cannot be executed if there is a "Recurrent" result in the node's dependencies.
                # Hence, the node should wait for proper a result or an error.
                or isinstance(self._node_storage.get_node_result(pred_node_id), Recurrent)
            ):
                logger.debug(
                    'The node %s cannot be executed due to absense the dependent result of the node %s',
                    node_id,
                    pred_node_id,
                )

                return False

        logger.debug('The node can be executed node_id=%s', node_id)
        return True

    async def _run_dag(self, dag: DiGraph) -> t.Any:
        """
        Run the dag.
        """

        logger.debug('Start DAG execution, dag=%s', str(dag))

        list_node_ids = self._get_node_order(dag)

        if dag.is_recurrent:
            logger.debug('Hide previous node results for recurrent subgraph %s', list_node_ids)
            self._node_storage.hide_last_execution(*list_node_ids)

        if len(list_node_ids) == 0:
            return None

        local_tasks = []

        for node_id in list_node_ids:
            await self._lock_manager.wait_for_condition(
                node_id,
                functools.partial(self._is_ready_to_execute, dag, node_id),
            )

            if dag.is_oneof and self.__has_subgraph_error(dag):
                logger.debug('An error has been found in the %s', dag)
                self._stop_coro_tasks(*local_tasks)

                # We must unlock descendants because the next OneOf subgraph should start the process.
                # Otherwise, the entire subgraph will be locked.
                await self.__unlock_descendants(node_id=node_id, dag=dag)
                return None

            if self._is_switch(node_id):
                coro_to_run = self._run_switch(dag, node_id)

            elif self._is_head_of_oneof(node_id):
                coro_to_run = self._run_oneof(dag, node_id)

            else:
                coro_to_run = self._run_node(
                    node_id=node_id,
                    dag=dag,
                )

            local_tasks.append(self._create_task(coro_to_run, name=node_id))

        logger.debug('Await for result for %s the dag %s', dag.dest, dag)

        await self._lock_manager.wait_for_condition(
            dag.dest,
            functools.partial(self._node_storage.exists_node_result, dag.dest),
        )

        return self._node_storage.get_node_result(dag.dest, with_hidden=True)

    def __has_subgraph_error(self, dag: DiGraph) -> bool:
        """
        Check if the subgraph has an error
        """
        return any([self._node_storage.exists_node_error(node_id) for node_id in dag.nodes])

    async def _run_oneof(self, dag: DiGraph, node_id: NodeId) -> t.Any:
        """
        Run OneOf subgraph. Returns the first non-error result or specific error
        """

        logger.debug('Prepare OneOf DAG node_id=%s', node_id)

        for idx, subgraph_node_id in enumerate(self.dag.graph.nodes[node_id][NodeField.oneof_nodes]):
            oneof_dag = self._get_reduced_dag(
                source=self.dag.input_node,
                dest=subgraph_node_id,
                is_oneof=True,
                is_nested_oneof=True,
            )

            logger.debug('Prepare [%s]%s to start. OneOf result node %s', idx, oneof_dag, node_id)

            self._create_task(coro=self._run_dag(dag=oneof_dag), name=str(oneof_dag))

            await self._lock_manager.wait_for_condition(
                subgraph_node_id,
                lambda: (
                    self.__has_subgraph_error(oneof_dag)  # noqa: B023
                    or self._node_storage.exists_result_type(
                        subgraph_node_id,  # noqa: B023
                        exclude_type=(Recurrent,),
                    )
                ),
            )

            if not self.__has_subgraph_error(oneof_dag):
                # The node_id is a synthetic node and cannot be executed anywhere. Hence, we should copy the
                # result of the last successful subgraph and unlock everything related to the synthetic node.
                self._node_storage.copy_node_result(subgraph_node_id, node_id)

                await self.__unlock_itself(node_id)
                await self.__unlock_descendants(node_id=node_id, dag=dag)
                await self.__unlock_run_method()

                logger.debug('The %s has been succeeded', oneof_dag)
                return

        if dag.is_nested_oneof:
            self._node_storage.set_node_result(node_id, OneOfDoesNotHaveResultError(node_id))
            await self.__unlock_itself(node_id)
            await self.__unlock_descendants(node_id=node_id, dag=dag)
        else:
            await self.__raise_exc(
                OneOfDoesNotHaveResultError(node_id),
            )

    async def _run_switch(self, dag: DiGraph, node_id: NodeId) -> t.Any:
        """
        Run switch subgraph
        """

        logger.debug('Prepare Switch DAG node_id=%s', node_id)

        self._add_case_result(node_id)

        return await self._run_dag(
            dag=self._get_reduced_dag(
                self.dag.input_node,
                (self._node_storage.get_switch_result(node_id)).node_id,
                is_oneof=dag.is_oneof,
            ),
        )

    async def _run_node(
        self,
        dag: DiGraph,
        node_id: NodeId,
        force_default: bool = False,
    ) -> None:
        """
        Method runs the node and according to the execution result orchestrates the node's locks
        in order to unlock all dependencies and itself

        Args:
            dag: A DAG that started the method
            node_id: Node id to execute
            force_default: If the node should be executed with default result
        """

        to_unlock_descendants = True

        try:
            result = await self._execute_node(
                force_default=force_default,
                node_id=node_id,
                dag=dag,
            )

            if isinstance(result, Recurrent):
                self._create_task(
                    name=f'rec-{node_id}',
                    coro=self._run_recurrent_subgraph(
                        node_result=result,
                        node_id=node_id,
                        dag=dag,
                    ),
                )

                # We shouldn't unlock the node's descendants if we have to perform recurrent subgraph.
                # It has to be this way because the node, which has `Recurrent` result,
                # will be executed again and the function will unlock the descendants in the other branch.
                to_unlock_descendants = False

            logger.debug('Save the result "%s" for the node %s', result, node_id)
            self._node_storage.set_node_result(node_id, result)

            # TODO: Needs to reorganize saving policy for artifact storage
            if not self._is_skipped_for_storage(node_id):
                await self.ctx.save_node_result(node_id, result)
            else:
                logger.debug('Node %s is skipped without saving result to the storage', node_id)

        finally:
            if not to_unlock_descendants:
                logger.debug('Skip unlocking the descendants of the node, node_id=%s', node_id)
                self.__unlock_execution_lock(node_id)

                # Unlock itself to perform the next step in the node's DAG
                await self.__unlock_itself(node_id)

                return  # noqa: B012

            logger.debug('Start the procedure of unlocking dependencies node_id=%s', node_id)

            self.__unlock_execution_lock(node_id)

            await self.__unlock_descendants(node_id=node_id, dag=dag)
            await self.__unlock_run_method()

            if node_id == dag.dest:
                logger.debug('The node %s is an output node', node_id)
                await self.__unlock_itself(node_id)

    async def __unlock_itself(self, node_id: NodeId) -> None:
        """
        Unlock the node itself to perform the next step in DAGs
        """
        await self._lock_manager.unlock_condition(node_id)

    async def __unlock_run_method(self) -> None:
        """
        Unlock the main method in order to return the main DAG result
        """

        await self._lock_manager.unlock_condition(self._alias_run_method)

    def __unlock_execution_lock(self, node_id: NodeId) -> None:
        """
        Unlock the execution function for the node that can be executed concurrently
        """

        lock_logger.debug('Unlock execution for %s if there is any concurrent execution', node_id)
        self._lock_manager.unlock_event(node_id)

    async def _run_recurrent_subgraph(
        self,
        dag: DiGraph,
        node_id: NodeId,
        node_result: Recurrent,
    ) -> None:
        """
        Run a recurrent subgraph with

        Args:
            dag: A DAG that started the rec subgraph
            node_id: Node id as the end of the subgraph
            node_result: Previous subgraph's result
        """

        start_from_node_id = self.dag.graph.nodes[node_id].get(NodeField.start_node)

        if self._node_storage.exists_active_rec_subgraph(start_from_node_id, node_id):
            return

        self._node_storage.set_active_rec_subgraph(start_from_node_id, node_id)
        max_iterations = self.dag.graph.nodes[node_id].get(NodeField.max_iterations)

        recurrent_subgraph = get_connected_subgraph(
            self.dag.graph,
            start_from_node_id,
            node_id,
            is_recurrent=True,
            is_oneof=dag.is_oneof,
        )
        logger.debug('%s Start the process of the recurrent subgraph', recurrent_subgraph)

        for current_iter in range(max_iterations):
            name = f'Recurrent-subgraph[attempt={current_iter}] {start_from_node_id} --> {node_id}'
            logger.debug('Executing the %s', name)

            start_node = self.dag.graph.nodes[start_from_node_id]
            start_node[NodeField.additional_data] = node_result.data

            node_result = await self._run_dag(dag=recurrent_subgraph)

            is_rec_result = isinstance(node_result, Recurrent)
            has_errors = self.__has_subgraph_error(recurrent_subgraph)

            if has_errors:
                logger.debug('The subgraph should be stopped. There is an error in %s', name)
                return

            if not is_rec_result and not has_errors:
                logger.debug('The subgraph should be stopped. The result has been processed for the subgraph %s', name)
                break

        else:
            node = self.dag.node_map[node_id]

            if isinstance(node_result, Recurrent) and node.use_default:
                logger.debug(
                    'Attempts to run a recurrent subgraph have been exceeded. '
                    'Will be used the default value start_node=%s, dest_node=%s',
                    start_from_node_id,
                    node_id,
                )

                self._node_storage.hide_last_execution(node_id)

                await self._run_node(dag=dag, node_id=node_id, force_default=True)

            else:
                logger.debug(
                    '%s has been ended with error because recurrent subgraph does not have a result',
                    recurrent_subgraph,
                )

                error = RecurrentSubgraphDoesNotHaveResultError(
                    dict(
                        start_from_node_id=start_from_node_id,
                        node_result=node_result,
                        node_id=node_id,
                    ),
                )

                if dag.is_oneof:
                    # If the node_id doesn't have a result at the end, we should either end the main DAG with error or
                    # return the control to the "parent" function.
                    # In this particular situation we should return it via the node's descendants so that
                    # another node could continue its process.
                    self._node_storage.set_node_result(node_id, error)
                    await self.__unlock_itself(node_id)
                    await self.__unlock_descendants(node_id=node_id, dag=dag)

                else:
                    await self.__raise_exc(error)

        self._node_storage.delete_active_rec_subgraph(start_from_node_id, node_id)

    async def __raise_exc(self, exc: Exception) -> None:
        """
        Raise an exception and let the run method know about the exception so the entire graph could be ended
        """

        await self.__unlock_run_method()
        raise exc

    async def __unlock_descendants(self, node_id: NodeId, dag: DiGraph) -> None:
        """
        Send a notification to the node's descendants to unlock them
        """

        descendants = self.__get_descendants(node_id, dag=dag)

        for descendant_node_id in descendants:
            await self._lock_manager.unlock_condition(descendant_node_id)

    def __get_descendants(self, node_id: NodeId, dag: DiGraph) -> t.Set[NodeId]:
        """
        Get all first-line the node's descendants including artificial nodes
        """

        logger.debug('Getting descendants for the node %s', node_id)

        descendants = nx.descendants_at_distance(self.dag.graph, node_id, 1)

        for descendant_node_id in set(descendants):
            if dag.is_oneof or self._is_switch(descendant_node_id):
                descendants.update(self.__get_descendants(descendant_node_id, dag=dag))
        return descendants

    def __repr__(self) -> str:
        return f'<{self.__class__.__name__} nnodes="{len(self.dag.graph.nodes)}" nedges="{len(self.dag.graph.edges)}">'
