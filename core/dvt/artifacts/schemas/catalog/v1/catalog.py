from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Dict, List, Optional, Union

from dvt.artifacts.schemas.base import (
    ArtifactMixin,
    BaseArtifactMetadata,
    schema_version,
)
from dbt_common.contracts.metadata import CatalogTable
from dbt_common.dataclass_schema import dbtClassMixin

Primitive = Union[bool, str, float, None]
PrimitiveDict = Dict[str, Primitive]


@dataclass
class CatalogMetadata(BaseArtifactMetadata):
    dbt_schema_version: str = field(
        default_factory=lambda: str(CatalogArtifact.dbt_schema_version)
    )


@dataclass
class CatalogResults(dbtClassMixin):
    nodes: Dict[str, CatalogTable]
    sources: Dict[str, CatalogTable]
    errors: Optional[List[str]] = None
    _compile_results: Optional[Any] = None
    # DVT: source unique_id -> connection (profile target name from profiles.yml) for docs visibility
    source_connections: Dict[str, str] = field(default_factory=dict)
    # DVT: connection (target) name -> optional metadata for docs (e.g. adapter type) for multi-connection visibility
    connection_metadata: Dict[str, Dict[str, Any]] = field(default_factory=dict)

    def __post_serialize__(self, dct: Dict, context: Optional[Dict] = None):
        dct = super().__post_serialize__(dct, context)
        if "_compile_results" in dct:
            del dct["_compile_results"]
        return dct


@dataclass
@schema_version("catalog", 1)
class CatalogArtifact(CatalogResults, ArtifactMixin):
    metadata: CatalogMetadata

    @classmethod
    def from_results(
        cls,
        generated_at: datetime,
        nodes: Dict[str, CatalogTable],
        sources: Dict[str, CatalogTable],
        compile_results: Optional[Any],
        errors: Optional[List[str]],
        source_connections: Optional[Dict[str, str]] = None,
        connection_metadata: Optional[Dict[str, Dict[str, Any]]] = None,
    ) -> "CatalogArtifact":
        meta = CatalogMetadata(generated_at=generated_at)
        return cls(
            metadata=meta,
            nodes=nodes,
            sources=sources,
            errors=errors,
            _compile_results=compile_results,
            source_connections=source_connections or {},
            connection_metadata=connection_metadata or {},
        )
