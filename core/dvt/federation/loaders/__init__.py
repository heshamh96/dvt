"""
Loader registry for EL layer.

Maps adapter types to specialized loaders.
Falls back to generic loader for unsupported adapters.

Supports same 34 adapter types as extractors:
- Cloud-native (bulk load): snowflake, bigquery, redshift, databricks
- PostgreSQL-compatible: postgres, greenplum, materialize, risingwave, cratedb, alloydb, timescaledb
- All others: JDBC-only fallback via GenericLoader
"""

from typing import Callable, Dict, Optional, Type

from dvt.federation.loaders.base import BaseLoader, LoadConfig, LoadResult
from dvt.federation.loaders.generic import GenericLoader

# Import specialized loaders
from dvt.federation.loaders.postgres import PostgresLoader
from dvt.federation.loaders.snowflake import SnowflakeLoader
from dvt.federation.loaders.bigquery import BigQueryLoader
from dvt.federation.loaders.redshift import RedshiftLoader
from dvt.federation.loaders.databricks import DatabricksLoader

# Registry mapping adapter types to loader classes
LOADER_REGISTRY: Dict[str, Type[BaseLoader]] = {}


def _register_loader(loader_class: Type[BaseLoader]) -> None:
    """Register a loader class for its adapter types."""
    for adapter_type in loader_class.adapter_types:
        LOADER_REGISTRY[adapter_type] = loader_class


# Register loaders with bulk load support
# PostgreSQL-compatible (7 adapters) - COPY FROM, no staging needed
_register_loader(PostgresLoader)
# Includes: postgres, greenplum, materialize, risingwave, cratedb, alloydb, timescaledb

# Cloud-native (with bulk load from cloud storage)
_register_loader(SnowflakeLoader)  # COPY INTO from S3/GCS/Azure
_register_loader(BigQueryLoader)  # LOAD DATA from GCS
_register_loader(RedshiftLoader)  # COPY from S3
_register_loader(DatabricksLoader)  # COPY INTO from S3/GCS/Azure

# Note: GenericLoader is the fallback, not registered
# All other adapters (mysql, oracle, sqlserver, trino, etc.) use JDBC via GenericLoader


def get_loader(
    adapter_type: str,
    on_progress: Optional[Callable[[str], None]] = None,
) -> BaseLoader:
    """Get the appropriate loader for an adapter type.

    Args:
        adapter_type: The dbt adapter type (e.g., 'postgres', 'snowflake')
        on_progress: Optional callback for progress messages

    Returns:
        Loader instance for the adapter type
    """
    loader_class = LOADER_REGISTRY.get(adapter_type, GenericLoader)
    return loader_class(on_progress)


def get_supported_adapters() -> list[str]:
    """Get list of adapter types with optimized loaders."""
    return list(LOADER_REGISTRY.keys())


def get_bulk_load_adapters() -> list[str]:
    """Get list of adapters that support native bulk load."""
    bulk_load = []
    for adapter_type, loader_class in LOADER_REGISTRY.items():
        loader = loader_class.__new__(loader_class)
        if loader.get_bulk_load_bucket_types():
            bulk_load.append(adapter_type)
    return bulk_load


__all__ = [
    # Base classes
    "BaseLoader",
    "LoadConfig",
    "LoadResult",
    # Loaders
    "GenericLoader",
    "PostgresLoader",
    "SnowflakeLoader",
    "BigQueryLoader",
    "RedshiftLoader",
    "DatabricksLoader",
    # Functions
    "get_loader",
    "get_supported_adapters",
    "get_bulk_load_adapters",
]
