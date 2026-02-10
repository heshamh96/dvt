"""
Generic loader - JDBC only fallback for all unsupported adapters.

Used as the fallback when no specialized loader is available for an adapter type.
All adapters can use JDBC through Spark, so this provides universal compatibility.
"""

from typing import Any

from dvt.federation.loaders.base import BaseLoader, LoadConfig, LoadResult


class GenericLoader(BaseLoader):
    """Generic loader using Spark JDBC only.

    Used as fallback for adapters without specialized bulk load support.
    Works with any database that has a JDBC driver.
    """

    adapter_types = []  # Fallback - handles any adapter not in registry

    def load(
        self,
        df: Any,  # pyspark.sql.DataFrame
        config: LoadConfig,
        adapter: Any = None,
    ) -> LoadResult:
        """Load using Spark JDBC with optional adapter DDL.

        Args:
            df: PySpark DataFrame to load
            config: Load configuration
            adapter: Optional dbt adapter for DDL operations

        Returns:
            LoadResult with success status and metadata
        """
        return self._load_jdbc(df, config, adapter)
