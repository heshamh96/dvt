from dataclasses import dataclass
from typing import Literal

from dvt.artifacts.resources.base import BaseResource
from dvt.artifacts.resources.types import NodeType


@dataclass
class Documentation(BaseResource):
    resource_type: Literal[NodeType.Documentation]
    block_contents: str
