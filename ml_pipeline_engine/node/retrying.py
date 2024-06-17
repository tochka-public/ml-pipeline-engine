from dataclasses import dataclass
from typing import Tuple
from typing import Type

from ml_pipeline_engine.types import NodeBase
from ml_pipeline_engine.types import RetryPolicyLike


@dataclass(frozen=True)
class NodeRetryPolicy(RetryPolicyLike):
    node: NodeBase

    @property
    def delay(self) -> int:
        return self.node.delay or 0

    @property
    def attempts(self) -> int:
        return self.node.attempts or 1

    @property
    def exceptions(self) -> Tuple[Type[Exception], ...]:
        return self.node.exceptions or (Exception,)
