import typing as t
from dataclasses import dataclass

from ml_pipeline_engine.types import NodeBase
from ml_pipeline_engine.types import RetryPolicyLike


@dataclass(frozen=True)
class NodeRetryPolicy(RetryPolicyLike):
    node: t.Type[NodeBase]

    @property
    def delay(self) -> float:
        return self.node.delay or 0.0

    @property
    def attempts(self) -> int:
        return self.node.attempts or 1

    @property
    def exceptions(self) -> tuple[t.Type[BaseException], ...]:
        return self.node.exceptions or (Exception,)
