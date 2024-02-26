import abc
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
    title: t.ClassVar[str] = None


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


class NodeSerializerLike(t.Protocol):
    node: t.Union[bytes]
    serialization_kind: SerializationNodeKind

    @classmethod
    def serialize(
        cls,
        node: NodeLike,
        serialization_kind: t.Optional[SerializationNodeKind] = None,
    ) -> 'NodeSerializerLike':
        ...

    def get_node(self) -> NodeLike:
        ...


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

    async def save_node_result(self, node_id: NodeId, data: t.Any) -> None:
        ...

    async def load_node_result(self, node_id: NodeId) -> t.Any:
        ...

    async def add_case_result(self, switch_node_id: NodeId, selection: CaseResult) -> None:
        ...

    async def get_case_result(self, switch_node_id: NodeId) -> CaseResult:
        ...

    async def add_node_in_run(self, node_id: NodeId) -> None:
        ...

    async def is_node_in_run(self, node_id: NodeId) -> bool:
        ...

    async def exists_node_result(self, node_id: NodeId) -> bool:
        ...

    async def is_active_recurrence_subgraph(self, source: NodeId, dest: NodeId) -> bool:
        ...

    async def set_active_recurrence_subgraph(self, source: NodeId, dest: NodeId) -> None:
        ...

    async def remove_recurrence_subgraph(self, source: NodeId, dest: NodeId) -> None:
        ...

    def delete_node_results(self, node_ids: t.Iterable[NodeId]) -> None:
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


class DAGRunManagerLike(t.Protocol):
    """
    Менеджер управлением запуска графа
    """

    dag: 'DAGLike'

    @abc.abstractmethod
    async def run(self, ctx: PipelineContextLike) -> NodeResultT:
        ...


class DAGLike(t.Protocol[NodeResultT]):
    """
    Граф
    """

    graph: [t.Type[nx.DiGraph], nx.DiGraph]
    input_node: NodeId
    output_node: NodeId
    node_map: t.Dict[NodeId, NodeSerializerLike]
    run_manager: DAGRunManagerLike
    retry_policy: RetryPolicyLike
    is_process_pool_needed: bool
    is_thread_pool_needed: bool

    @abc.abstractmethod
    async def run(self, ctx: PipelineContextLike) -> NodeResultT:
        ...
