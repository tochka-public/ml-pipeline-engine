from dataclasses import asdict
from dataclasses import dataclass
from dataclasses import field
from typing import Any
from typing import Dict
from typing import List
from typing import Optional


@dataclass
class GraphAttributes:
    verbose_name: str
    name: str  # Tech name
    repo_link: Optional[str] = None
    edgesep: int = field(default=60)  # Number of pixels that separate edges horizontally in the layout.
    ranksep: int = field(default=700)  # Number of pixels between each rank in the layout.


@dataclass
class NodeAttributes:
    name: str  # Tech name
    verbose_name: str
    doc: str
    code_source: str


@dataclass
class Node:
    id: str
    is_virtual: bool
    is_generic: bool
    type: Optional[str] = None  # If a node is artificial
    data: Optional[NodeAttributes] = None  # If a node is artificial


@dataclass
class Edge:
    id: str = field(init=False)
    source: str
    target: str

    def __post_init__(self) -> None:
        self.id = f'{self.source}->{self.target}'


@dataclass
class NodeType:
    name: str  # Tech name
    hex_bgr_color: Optional[str] = None


@dataclass
class GraphConfig:
    nodes: List[Node]
    edges: List[Edge]
    node_types: Dict[str, NodeType]
    attributes: GraphAttributes

    def as_dict(self) -> Dict[str, Any]:
        return asdict(self)
