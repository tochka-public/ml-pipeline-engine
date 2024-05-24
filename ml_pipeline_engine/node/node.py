import asyncio
import functools
import inspect
import typing as t
import uuid

from ulid import ULID

from ml_pipeline_engine.logs import logger_node as logger
from ml_pipeline_engine.module_loading import get_instance
from ml_pipeline_engine.node.enums import NodeTag
from ml_pipeline_engine.node.errors import ClassExpectedError
from ml_pipeline_engine.node.errors import RunMethodExpectedError
from ml_pipeline_engine.parallelism import process_pool_registry
from ml_pipeline_engine.parallelism import threads_pool_registry
from ml_pipeline_engine.types import NodeBase
from ml_pipeline_engine.types import NodeId
from ml_pipeline_engine.types import NodeLike

NodeResultT = t.TypeVar('NodeResultT')


def generate_pipeline_id() -> uuid.UUID:
    return ULID().to_uuid()


def generate_node_id(prefix: str, name: t.Optional[str] = None) -> str:
    return f'{prefix}__{name if name is not None else uuid.uuid4().hex[-8:]}'


def get_node_id(node: NodeLike) -> NodeId:
    node_type = node.node_type if getattr(node, 'node_type', None) else 'node'

    if getattr(node, 'name', None):
        node_name = node.name
    else:
        node_name = f'{node.__module__}_{getattr(node, "__name__", node.__class__.__name__)}'.replace('.', '_')

    return '__'.join([node_type, node_name])


def get_run_method(node: NodeLike) -> t.Optional[str]:
    run_method = None

    for method in NodeBase.RUN_METHOD_ALIASES:
        if callable(getattr(node, method, None)):
            if run_method is not None:
                raise AssertionError(f'Node should have only one run method. {run_method} + {method} detected')
            run_method = method

    return run_method


def get_callable_run_method(node: NodeLike) -> t.Callable:
    run_method_name = get_run_method(node)

    if run_method_name is not None:
        node = get_instance(node)
        return getattr(node, run_method_name)

    return node


def run_node_default(node: NodeLike[NodeResultT], **kwargs: t.Any) -> t.Type[NodeResultT]:
    """
    Get default value from the node
    """
    return get_instance(node).get_default(**kwargs)


async def run_node(node: NodeLike[NodeResultT], *args: t.Any, node_id: NodeId, **kwargs: t.Any) -> t.Type[NodeResultT]:
    """
    Run a node in a specific way according to the node's tags
    """

    run_method = get_callable_run_method(node)
    loop = asyncio.get_running_loop()
    tags = node.tags or ()

    if inspect.iscoroutinefunction(run_method):
        logger.debug('The node will be executed as coroutine function in the loop, node_id=%s', node_id)
        result = await run_method(*args, **kwargs)

    elif NodeTag.non_async in tags:
        logger.debug('The node will be executed as sync function, node_id=%s', node_id)
        result = run_method(*args, **kwargs)

    else:
        executor = (
            process_pool_registry.get_pool_executor()
            if NodeTag.process in tags
            else threads_pool_registry.get_pool_executor()
        )

        logger.debug(
            'The node will be executed using the executor, executor=%s, node_id=%s',
            executor.__class__.__name__,
            node_id,
        )

        result = await loop.run_in_executor(
            executor,
            functools.partial(run_method, *args, **kwargs),
        )

    return result


def build_node(
    node: NodeLike,
    node_name: t.Optional[str] = None,
    class_name: t.Optional[str] = None,
    atts: t.Optional[t.Dict[str, t.Any]] = None,
    dependencies_default: t.Optional[t.Dict[str, t.Any]] = None,
    **target_dependencies: t.Any,
) -> t.Type[NodeLike]:
    """
    Build new node that inherits all properties from the basic node.

    Args:
        node: Basic node class
        class_name: Title for the new class node
        node_name: Title for the node
        atts: Any additional attrs for the new class
        dependencies_default: Default kwargs for the run method
        **target_dependencies: Main dependencies like other nodes
    """

    if not inspect.isclass(node):
        raise ClassExpectedError('Для создания узла ожидается объекта класса')

    run_method = get_run_method(node)
    if not run_method:
        raise RunMethodExpectedError(
            f'Ожидается наличие хотя бы одного run-метода. methods={NodeBase.RUN_METHOD_ALIASES}',
        )

    if inspect.iscoroutinefunction(getattr(node, run_method)):

        async def class_method(*args: t.Any, **kwargs: t.Any) -> t.Any:
            return await getattr(node, run_method)(*args, **kwargs, **(dependencies_default or {}))

    else:
        def class_method(*args: t.Any, **kwargs: t.Any) -> t.Any:
            return getattr(node, run_method)(*args, **kwargs, **(dependencies_default or {}))

    class_name = class_name or f'Generic{node.__name__}'
    created_node = type(
        class_name,
        (node,),
        {
            # Меняем на lambda-функцию, чтобы убить ссылку на метод родительского класса.
            run_method: class_method,
            '__module__': __name__,
            '__generic_class__': node,
            'name': node_name or node.name,
            **(atts or {}),
        },
    )

    method = getattr(created_node, run_method)
    method.__annotations__.update(target_dependencies)

    globals()[class_name] = created_node
    return created_node
