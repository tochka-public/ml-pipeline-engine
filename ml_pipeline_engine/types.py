import abc
import asyncio
import typing as t
from dataclasses import dataclass
from uuid import UUID

import networkx as nx

ProcessorResultT = t.TypeVar('ProcessorResultT')
DataSourceResultT = t.TypeVar('DataSourceResultT')
FeatureResultT = t.TypeVar('FeatureResultT')
FeatureVectorizerResultT = t.TypeVar('FeatureVectorizerResultT')
MLModelResultT = t.TypeVar('MLModelResultT')

NodeResultT = t.TypeVar('NodeResultT')
AdditionalDataT = t.TypeVar('AdditionalDataT', bound=t.Any)

PipelineId = t.Union[UUID, str]

NodeId = str
ModelName = str
CaseLabel = str
SerializationNodeKind = str
NodeTag = str


class RetryProtocol(t.Protocol):
    """
    Протокол, описывающий свойства повторного исполнения ноды.
    Предоставляет возможность использовать дефолтное значение в случае ошибки.
    """

    attempts: t.ClassVar[t.Optional[int]] = None
    delay: t.ClassVar[t.Optional[t.Union[int, float]]] = None
    exceptions: t.ClassVar[t.Optional[t.Tuple[t.Type[BaseException], ...]]] = None
    use_default: t.ClassVar[bool] = False

    def get_default(self, **kwargs) -> NodeResultT:
        ...


class TagProtocol(t.Protocol):
    """
    Протокол, который позволяет декларировать параметры запуска узла
    """

    tags: t.ClassVar[t.Tuple[NodeTag, ...]] = ()


@dataclass
class Recurrent:
    data: t.Optional[AdditionalDataT]


class RecurrentProtocol(t.Protocol):
    """
    Протокол, определяющий поведение узла при повторном исполнении подграфа
    """

    @abc.abstractmethod
    def next_iteration(self, data: t.Optional[AdditionalDataT]) -> Recurrent:
        """
        Метод для инициализации повторного исполнения подграфа
        """


class NodeProtocol(t.Protocol):
    """
    Узел графа модели
    """

    RUN_METHOD_ALIASES = (
        'process',
        'extract',
        'collect',
        'vectorize',
        'predict',
    )

    node_type: t.ClassVar[str] = None
    name: t.ClassVar[str] = None
    title: t.ClassVar[str] = None  # TODO: Remove it in the future
    verbose_name: t.ClassVar[str] = None


class NodeBase(NodeProtocol, RetryProtocol, TagProtocol):
    pass


class ProcessorLike(RetryProtocol, TagProtocol, t.Protocol[ProcessorResultT]):  # noqa
    """
    Узел общего назначения
    """

    process: t.Union[
        t.Callable[..., ProcessorResultT],
        t.Callable[..., t.Awaitable[ProcessorResultT]]
    ]


class DataSourceLike(RetryProtocol, TagProtocol, t.Protocol[DataSourceResultT]):  # noqa
    """
    Источник данных
    """

    collect: t.Union[
        t.Callable[..., DataSourceResultT],
        t.Callable[..., t.Awaitable[DataSourceResultT]]
    ]


class FeatureLike(RetryProtocol, TagProtocol, t.Protocol[FeatureResultT]):  # noqa
    """
    Фича
    """

    extract: t.Union[
        t.Callable[..., FeatureResultT],
        t.Callable[..., t.Awaitable[FeatureResultT]]
    ]


class FeatureVectorizerLike(RetryProtocol, TagProtocol, t.Protocol[FeatureVectorizerResultT]):
    """
    Векторизатор фичей
    """

    vectorize: t.Union[
        t.Callable[..., FeatureVectorizerResultT],
        t.Callable[..., t.Awaitable[FeatureVectorizerResultT]]
    ]


class MLModelLike(RetryProtocol, TagProtocol, t.Protocol[MLModelResultT]):
    """
    ML-модель
    """

    predict: t.Union[
        t.Callable[..., MLModelResultT],
        t.Callable[..., t.Awaitable[MLModelResultT]]
    ]


NodeLike = t.Union[
    t.Type[ProcessorLike[NodeResultT]],
    t.Type[DataSourceLike[NodeResultT]],
    t.Type[FeatureLike[NodeResultT]],
    t.Type[FeatureVectorizerLike[NodeResultT]],
    t.Type[MLModelLike[NodeResultT]],
]


@dataclass(frozen=True)
class PipelineResult(t.Generic[NodeResultT]):
    """
    Контейнер результата выполнения пайплайна.
    """

    pipeline_id: PipelineId
    value: NodeResultT
    error: t.Optional[Exception]

    def raise_on_error(self) -> None:
        if self.error is not None:
            raise self.error


@dataclass(frozen=True)
class CaseResult:
    """
    Результат оператора switch-case
    """

    label: CaseLabel
    node_id: NodeId


class PipelineChartLike(t.Protocol[NodeResultT]):
    """
    Определение пайплайна ML-модели

    Этот объект можно хранить в глобальной области. Он является иммутабельным.
    """

    model_name: ModelName
    entrypoint: t.Optional[t.Union[NodeLike[NodeResultT], 'DAGLike[NodeResultT]']]
    event_managers: t.List[t.Type['EventManagerLike']]
    artifact_store: t.Optional[t.Type['ArtifactStoreLike']]

    async def run(
        self,
        pipeline_id: t.Optional[PipelineId] = None,
        input_kwargs: t.Optional[t.Dict[str, t.Any]] = None,
        meta: t.Optional[t.Dict[str, t.Any]] = None,
    ) -> PipelineResult[NodeResultT]:
        ...


class PipelineContextLike(t.Protocol):
    """
    Контекст выполнения пайплайна

    Создается при запуске пайплайна из чарта пайплайна и хранит состояние выполнения пайплайна, является источником
    событий жизненного цикла пайплайна.
    """

    chart: PipelineChartLike
    pipeline_id: PipelineId
    input_kwargs: t.Dict[str, t.Any]
    meta: t.Dict[str, t.Any]
    artifact_store: 'ArtifactStoreLike'

    async def emit_on_node_start(self, node_id: NodeId):
        ...

    async def emit_on_node_complete(self, node_id: NodeId, error: t.Optional[Exception]):
        ...

    async def emit_on_pipeline_start(self):
        ...

    async def emit_on_pipeline_complete(self, result: PipelineResult):
        ...

    # TODO: Remove it in the future!
    async def save_node_result(self, node_id: NodeId, data: t.Any) -> None:
        ...

    @property
    def model_name(self) -> ModelName:  # noqa
        ...

    def _get_event_managers(self) -> t.List['EventManagerLike']:
        ...


class EventManagerLike(t.Protocol):
    """
    Менеджер событий жизненного цикла пайплайна
    """

    async def on_pipeline_start(self, ctx: PipelineContextLike) -> None:
        ...

    async def on_pipeline_complete(self, ctx: PipelineContextLike, result: PipelineResult) -> None:
        ...

    async def on_node_start(self, ctx: PipelineContextLike, node_id: NodeId) -> None:
        ...

    async def on_node_complete(self, ctx: PipelineContextLike, node_id: NodeId, error: t.Optional[Exception]) -> None:
        ...


class ArtifactStoreLike(t.Protocol):
    """
    Хранилище артефактов - результатов расчета узлов
    """

    def __init__(self, ctx: PipelineContextLike, *args, **kwargs):
        ...

    async def save(self, node_id: NodeId, data: t.Any) -> None:
        ...

    async def load(self, node_id: NodeId) -> NodeResultT:
        ...


class DAGCacheManagerLike(t.Protocol):
    """
    Менеджер локального кэша DAG
    """

    @abc.abstractmethod
    def save(self, node_id: NodeId, data: t.Any):
        ...

    @abc.abstractmethod
    def load(self, node_id: NodeId) -> t.Any:
        ...

    @abc.abstractmethod
    def exists(self, node_id: NodeId) -> bool:
        ...


class RetryPolicyLike(t.Protocol):
    node: NodeLike

    @property
    @abc.abstractmethod
    def delay(self) -> int:
        ...

    @property
    @abc.abstractmethod
    def attempts(self) -> int:
        ...

    @property
    @abc.abstractmethod
    def exceptions(self) -> t.Tuple[t.Type[Exception]]:
        ...


class DAGRunLockManagerLike(t.Protocol):
    """
    A manager object to store and manage locks related to NodeIds
    """

    lock_store: t.Dict[str, asyncio.Event] = {}

    @abc.abstractmethod
    def get_lock(self, node_id: NodeId) -> asyncio.Event:
        ...


class HiddenDictLike(t.Protocol):
    """
    Dict object that can hide some keys until they are set again
    """

    @abc.abstractmethod
    def get(self, key: t.Any, with_hidden: bool = True) -> t.Any:
        ...

    @abc.abstractmethod
    def exists(self, key, with_hidden: bool = True) -> bool:
        ...

    @abc.abstractmethod
    def set(self, key: t.Any, value: t.Any) -> None:
        ...

    @abc.abstractmethod
    def hide(self, key: t.Any) -> None:
        ...

    @abc.abstractmethod
    def delete(self, key: t.Any) -> None:
        ...


class DAGNodeStorageLike(t.Protocol):
    """
    A container for all information about node results
    """

    # FIXME: If we want to show these methods and structures, we have to declare switch in types.py,
    #        as well as recurrent subgraph

    processed_nodes: HiddenDictLike
    node_results: HiddenDictLike
    switch_results: HiddenDictLike
    recurrent_subgraph: HiddenDictLike

    @abc.abstractmethod
    def set_node_result(self, node_id: NodeId, data: t.Any) -> None:
        ...

    @abc.abstractmethod
    def get_node_result(self, node_id: NodeId, with_hidden: bool = False) -> t.Any:
        ...

    @abc.abstractmethod
    def hide_node_result(self, node_id: NodeId) -> None:
        ...

    @abc.abstractmethod
    def exists_node_result(self, node_id: NodeId, with_hidden: bool = False) -> bool:
        ...

    @abc.abstractmethod
    def set_switch_result(self, node_id: NodeId, data: t.Any) -> t.Any:
        ...

    @abc.abstractmethod
    def get_switch_result(self, node_id: NodeId, with_hidden: bool = False) -> t.Any:
        ...

    @abc.abstractmethod
    def set_node_as_processed(self, node_id: NodeId) -> None:
        ...

    @abc.abstractmethod
    def hide_processed_node(self, node_id: NodeId) -> None:
        ...

    @abc.abstractmethod
    def exists_processed_node(self, node_id: NodeId, with_hidden: bool = False) -> bool:
        ...

    @abc.abstractmethod
    def set_active_rec_subgraph(self, source: NodeId, dest: NodeId) -> None:
        ...

    @abc.abstractmethod
    def delete_active_rec_subgraph(self, source: NodeId, dest: NodeId) -> None:
        ...

    @abc.abstractmethod
    def exists_active_rec_subgraph(self, source: NodeId, dest: NodeId) -> bool:
        ...

    @abc.abstractmethod
    def hide_last_execution(self, *node_ids: NodeId) -> None:
        ...


class DAGRunManagerLike(t.Protocol):
    """
    Менеджер управлением запуска графа
    """

    lock_manager: DAGRunLockManagerLike
    node_storage: DAGNodeStorageLike
    ctx: PipelineContextLike
    dag: 'DAGLike'

    _memorization_store: t.Dict[t.Any, t.Any]

    @abc.abstractmethod
    async def run(self) -> NodeResultT:
        ...


class DAGLike(t.Protocol[NodeResultT]):
    """
    Граф
    """

    graph: [t.Type[nx.DiGraph], nx.DiGraph]
    input_node: NodeId
    output_node: NodeId
    node_map: t.Dict[NodeId, NodeLike]
    run_manager: DAGRunManagerLike
    retry_policy: RetryPolicyLike
    is_process_pool_needed: bool
    is_thread_pool_needed: bool

    @abc.abstractmethod
    async def run(self, ctx: PipelineContextLike) -> NodeResultT:
        ...

    def visualize(self, *args, **kwargs) -> None:
        ...
