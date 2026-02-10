# coding=utf-8
"""Authentication handler factory for DVT federation.

Provides get_auth_handler() to retrieve the appropriate auth handler
for any supported adapter type.

Usage:
    from dvt.federation.auth import get_auth_handler

    handler = get_auth_handler("snowflake")
    jdbc_props = handler.get_jdbc_properties(creds)
"""

from typing import Dict, List, Type

from .base import BaseAuthHandler
from .snowflake import SnowflakeAuthHandler
from .databricks import DatabricksAuthHandler
from .bigquery import BigQueryAuthHandler
from .redshift import RedshiftAuthHandler
from .postgres import PostgresAuthHandler
from .mysql import MySQLAuthHandler
from .sqlserver import SQLServerAuthHandler
from .oracle import OracleAuthHandler
from .trino import TrinoAuthHandler
from .clickhouse import ClickHouseAuthHandler
from .athena import AthenaAuthHandler
from .exasol import ExasolAuthHandler
from .vertica import VerticaAuthHandler
from .duckdb import DuckDBAuthHandler
from .firebolt import FireboltAuthHandler
from .db2 import DB2AuthHandler
from .hive import HiveAuthHandler
from .impala import ImpalaAuthHandler
from .generic import GenericAuthHandler


# Registry: adapter type -> auth handler class
AUTH_HANDLERS: Dict[str, Type[BaseAuthHandler]] = {
    # Tier 1: Official dbt-labs adapters
    "snowflake": SnowflakeAuthHandler,
    "databricks": DatabricksAuthHandler,
    "bigquery": BigQueryAuthHandler,
    "redshift": RedshiftAuthHandler,
    "postgres": PostgresAuthHandler,
    "spark": GenericAuthHandler,  # Spark is compute engine, not target
    # PostgreSQL-compatible
    "greenplum": PostgresAuthHandler,
    "materialize": PostgresAuthHandler,
    "risingwave": PostgresAuthHandler,
    "cratedb": PostgresAuthHandler,
    "alloydb": PostgresAuthHandler,
    "timescaledb": PostgresAuthHandler,
    # MySQL-compatible
    "mysql": MySQLAuthHandler,
    "tidb": MySQLAuthHandler,
    "singlestore": MySQLAuthHandler,
    # Microsoft
    "sqlserver": SQLServerAuthHandler,
    "synapse": SQLServerAuthHandler,
    "fabric": SQLServerAuthHandler,
    # Hadoop ecosystem
    "hive": HiveAuthHandler,
    "impala": ImpalaAuthHandler,
    # Cloud data warehouses
    "athena": AthenaAuthHandler,
    # Others
    "oracle": OracleAuthHandler,
    "trino": TrinoAuthHandler,
    "starburst": TrinoAuthHandler,
    "clickhouse": ClickHouseAuthHandler,
    "exasol": ExasolAuthHandler,
    "vertica": VerticaAuthHandler,
    "duckdb": DuckDBAuthHandler,
    "firebolt": FireboltAuthHandler,
    "db2": DB2AuthHandler,
}


def get_auth_handler(adapter_type: str) -> BaseAuthHandler:
    """Get the auth handler for an adapter type.

    Args:
        adapter_type: Adapter type from profiles.yml (e.g., 'snowflake')

    Returns:
        Appropriate BaseAuthHandler instance
    """
    handler_class = AUTH_HANDLERS.get(adapter_type.lower(), GenericAuthHandler)
    return handler_class()


def is_adapter_supported(adapter_type: str) -> bool:
    """Check if an adapter has explicit auth handler support."""
    return adapter_type.lower() in AUTH_HANDLERS


def get_supported_adapters() -> List[str]:
    """Get list of all supported adapter types."""
    return list(AUTH_HANDLERS.keys())


# Export key classes
__all__ = [
    "BaseAuthHandler",
    "get_auth_handler",
    "is_adapter_supported",
    "get_supported_adapters",
    "AUTH_HANDLERS",
]
