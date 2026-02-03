from dataclasses import dataclass
from typing import Literal

from dvt.artifacts.resources.types import NodeType
from dvt.artifacts.resources.v1.components import CompiledResource


@dataclass
class Analysis(CompiledResource):
    resource_type: Literal[NodeType.Analysis]
