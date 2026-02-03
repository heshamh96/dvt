from dataclasses import dataclass
from typing import Literal, Optional

from dvt.artifacts.resources.types import NodeType
from dvt.artifacts.resources.v1.components import CompiledResource


@dataclass
class HookNode(CompiledResource):
    resource_type: Literal[NodeType.Operation]
    index: Optional[int] = None
