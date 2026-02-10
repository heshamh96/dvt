"""
Databricks loader with COPY INTO optimization.

Load priority:
1. COPY INTO from cloud bucket (S3/GCS/Azure) - if bucket configured
2. Spark JDBC with adapter DDL - default path

DDL operations use the dbt adapter for proper backtick quoting.
Data loading uses Spark JDBC parallel writes.
"""

import time
from typing import Any, List, Optional

from dvt.federation.loaders.base import BaseLoader, LoadConfig, LoadResult


class DatabricksLoader(BaseLoader):
    """Databricks loader using COPY INTO from cloud storage.

    When a cloud bucket is configured (S3, GCS, Azure), uses Databricks
    COPY INTO for efficient bulk loading. Otherwise falls back to
    Spark JDBC with adapter-managed DDL.

    The adapter handles proper backtick quoting for Databricks SQL.
    """

    adapter_types = ["databricks"]

    def supports_bulk_load(self, bucket_type: str) -> bool:
        """Check if bulk load is supported for bucket type."""
        return bucket_type in ("s3", "gcs", "azure")

    def get_bulk_load_bucket_types(self) -> List[str]:
        """Get supported bucket types for bulk load."""
        return ["s3", "gcs", "azure"]

    def load(
        self,
        df: Any,
        config: LoadConfig,
        adapter: Optional[Any] = None,
    ) -> LoadResult:
        """Load DataFrame to Databricks.

        Args:
            df: PySpark DataFrame to load
            config: Load configuration
            adapter: dbt adapter for DDL operations

        Returns:
            LoadResult with success status and metadata
        """
        bucket_type = config.bucket_config.get("type") if config.bucket_config else None

        # Try bulk load if cloud bucket is configured
        if bucket_type and self.supports_bulk_load(bucket_type):
            try:
                return self._load_bulk(df, config, adapter)
            except Exception as e:
                self._log(f"COPY INTO failed ({e}), falling back to JDBC...")

        # Default: Spark JDBC with adapter DDL
        return self._load_jdbc(df, config, adapter)

    def _load_bulk(
        self,
        df: Any,
        config: LoadConfig,
        adapter: Optional[Any] = None,
    ) -> LoadResult:
        """Load via COPY INTO from cloud storage.

        Stages data to cloud bucket as Parquet, then uses
        Databricks COPY INTO for efficient bulk loading.

        Args:
            df: PySpark DataFrame to load
            config: Load configuration with bucket_config
            adapter: dbt adapter for DDL and COPY INTO execution

        Returns:
            LoadResult with success status and metadata
        """
        start_time = time.time()

        try:
            if not config.bucket_config:
                raise ValueError("bucket_config required for bulk load")

            if not adapter:
                raise ValueError("adapter required for COPY INTO")

            from dvt.federation.adapter_manager import get_quoted_table_name
            from dvt.federation.cloud_storage import get_cloud_storage_path

            bucket_type = config.bucket_config.get("type")
            bucket_path = get_cloud_storage_path(config.bucket_config)

            # Generate unique staging path
            import uuid

            staging_path = f"{bucket_path}/staging/{uuid.uuid4().hex}"

            self._log(f"Staging data to {bucket_type}://{staging_path}...")

            # Write DataFrame to cloud storage as Parquet
            df.write.parquet(staging_path, mode="overwrite")

            row_count = df.count()
            quoted_table = get_quoted_table_name(adapter, config.table_name)

            with adapter.connection_named("dvt_loader"):
                # Execute DDL
                if config.full_refresh:
                    self._log(f"Dropping {config.table_name}...")
                    adapter.execute(f"DROP TABLE IF EXISTS {quoted_table}")
                elif config.truncate:
                    self._log(f"Truncating {config.table_name}...")
                    try:
                        adapter.execute(f"TRUNCATE TABLE {quoted_table}")
                    except Exception:
                        pass  # Table might not exist

                # Execute COPY INTO
                self._log(f"Loading {config.table_name} via COPY INTO...")
                copy_sql = f"""
                    COPY INTO {quoted_table}
                    FROM '{staging_path}'
                    FILEFORMAT = PARQUET
                    COPY_OPTIONS ('mergeSchema' = 'true')
                """
                adapter.execute(copy_sql)

            elapsed = time.time() - start_time
            self._log(f"Loaded {row_count:,} rows via COPY INTO in {elapsed:.1f}s")

            return LoadResult(
                success=True,
                table_name=config.table_name,
                row_count=row_count,
                load_method="bulk_load",
                elapsed_seconds=elapsed,
            )

        except Exception as e:
            elapsed = time.time() - start_time
            self._log(f"COPY INTO failed: {e}")
            return LoadResult(
                success=False,
                table_name=config.table_name,
                error=str(e),
                load_method="bulk_load",
                elapsed_seconds=elapsed,
            )
