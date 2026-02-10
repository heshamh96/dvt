"""
Extractor registry for EL layer.

Maps adapter types to specialized extractors.
Falls back to generic extractor for unsupported adapters.

Supports all 28 dbt adapter types:
- Cloud-native (parallel export): snowflake, bigquery, redshift, databricks, athena
- PostgreSQL-compatible: postgres, greenplum, materialize, risingwave, cratedb, alloydb, timescaledb
- MySQL-compatible: mysql, mariadb, tidb, singlestore, starrocks, doris
- Enterprise: oracle, sqlserver, synapse, fabric, duckdb, trino, starburst
- Analytics: clickhouse, vertica, exasol, firebolt
- Legacy: db2, teradata, hive, impala
- Special: spark
"""

from typing import Any, Callable, Dict, Optional, Type

from dvt.federation.extractors.base import (
    BaseExtractor,
    ExtractionConfig,
    ExtractionResult,
)
from dvt.federation.extractors.generic import GenericExtractor

# Import all extractors
from dvt.federation.extractors.postgres import PostgresExtractor
from dvt.federation.extractors.mysql import MySQLExtractor
from dvt.federation.extractors.snowflake import SnowflakeExtractor
from dvt.federation.extractors.bigquery import BigQueryExtractor
from dvt.federation.extractors.redshift import RedshiftExtractor
from dvt.federation.extractors.databricks import DatabricksExtractor
from dvt.federation.extractors.oracle import OracleExtractor
from dvt.federation.extractors.sqlserver import SQLServerExtractor
from dvt.federation.extractors.duckdb import DuckDBExtractor
from dvt.federation.extractors.trino import TrinoExtractor
from dvt.federation.extractors.athena import AthenaExtractor
from dvt.federation.extractors.clickhouse import ClickHouseExtractor
from dvt.federation.extractors.vertica import VerticaExtractor
from dvt.federation.extractors.exasol import ExasolExtractor
from dvt.federation.extractors.db2 import DB2Extractor
from dvt.federation.extractors.teradata import TeradataExtractor
from dvt.federation.extractors.hive import HiveExtractor
from dvt.federation.extractors.firebolt import FireboltExtractor
from dvt.federation.extractors.spark import SparkExtractor

# Registry mapping adapter types to extractor classes
EXTRACTOR_REGISTRY: Dict[str, Type[BaseExtractor]] = {}


def _register_extractor(extractor_class: Type[BaseExtractor]) -> None:
    """Register an extractor class for its adapter types."""
    for adapter_type in extractor_class.adapter_types:
        EXTRACTOR_REGISTRY[adapter_type] = extractor_class


# Register all extractors
# Cloud-native (with parallel export to cloud storage)
_register_extractor(SnowflakeExtractor)
_register_extractor(BigQueryExtractor)
_register_extractor(RedshiftExtractor)
_register_extractor(DatabricksExtractor)
_register_extractor(AthenaExtractor)

# PostgreSQL-compatible (7 adapters)
_register_extractor(PostgresExtractor)
# Includes: postgres, greenplum, materialize, risingwave, cratedb, alloydb, timescaledb

# MySQL-compatible (6 adapters)
_register_extractor(MySQLExtractor)
# Includes: mysql, mariadb, tidb, singlestore, starrocks, doris

# Enterprise databases
_register_extractor(OracleExtractor)
_register_extractor(SQLServerExtractor)  # Includes: sqlserver, synapse, fabric
_register_extractor(DuckDBExtractor)
_register_extractor(TrinoExtractor)  # Includes: trino, starburst

# Analytics databases
_register_extractor(ClickHouseExtractor)
_register_extractor(VerticaExtractor)
_register_extractor(ExasolExtractor)
_register_extractor(FireboltExtractor)

# Legacy/enterprise
_register_extractor(DB2Extractor)
_register_extractor(TeradataExtractor)
_register_extractor(HiveExtractor)  # Includes: hive, impala

# Special
_register_extractor(SparkExtractor)

# Note: GenericExtractor is the fallback, not registered


def get_extractor(
    adapter_type: str,
    connection: Any,
    dialect: str,
    on_progress: Optional[Callable[[str], None]] = None,
    connection_config: Optional[Dict[str, Any]] = None,
) -> BaseExtractor:
    """Get the appropriate extractor for an adapter type.

    Args:
        adapter_type: The dbt adapter type (e.g., 'postgres', 'mysql')
        connection: Raw database connection from dbt adapter
        dialect: SQL dialect name
        on_progress: Optional callback for progress messages
        connection_config: Connection configuration dict for lazy connection creation

    Returns:
        Extractor instance for the adapter type
    """
    extractor_class = EXTRACTOR_REGISTRY.get(adapter_type, GenericExtractor)
    return extractor_class(connection, dialect, on_progress, connection_config)


def get_supported_adapters() -> list[str]:
    """Get list of adapter types with optimized extractors."""
    return list(EXTRACTOR_REGISTRY.keys())


def get_cloud_native_adapters() -> list[str]:
    """Get list of adapters that support cloud-native parallel export."""
    cloud_native = []
    for adapter_type, extractor_class in EXTRACTOR_REGISTRY.items():
        extractor = extractor_class.__new__(extractor_class)
        if extractor.get_native_export_bucket_types():
            cloud_native.append(adapter_type)
    return cloud_native


__all__ = [
    # Base classes
    "BaseExtractor",
    "ExtractionConfig",
    "ExtractionResult",
    # Extractors
    "GenericExtractor",
    "PostgresExtractor",
    "MySQLExtractor",
    "SnowflakeExtractor",
    "BigQueryExtractor",
    "RedshiftExtractor",
    "DatabricksExtractor",
    "OracleExtractor",
    "SQLServerExtractor",
    "DuckDBExtractor",
    "TrinoExtractor",
    "AthenaExtractor",
    "ClickHouseExtractor",
    "VerticaExtractor",
    "ExasolExtractor",
    "DB2Extractor",
    "TeradataExtractor",
    "HiveExtractor",
    "FireboltExtractor",
    "SparkExtractor",
    # Functions
    "get_extractor",
    "get_supported_adapters",
    "get_cloud_native_adapters",
]
