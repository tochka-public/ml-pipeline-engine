from dataclasses import dataclass
from typing import Tuple, Type

from ml_pipeline_engine.types import NodeLike, RetryPolicyLike


@dataclass(frozen=True)
class DagRetryPolicy(RetryPolicyLike):
    node: NodeLike

    @property
    def delay(self) -> int:
        return self.node.delay or 0

    @property
    def attempts(self) -> int:
        return self.node.attempts or 1

    @property
    def exceptions(self) -> Tuple[Type[Exception], ...]:
        return self.node.exceptions or (Exception,)

    @property
    def use_default(self) -> bool:
        return self.node.use_default
