"""
Snowflake loader using COPY INTO from staged Parquet files.

Stages Parquet to S3/GCS/Azure, then uses COPY INTO for bulk load.
Falls back to JDBC for local filesystem.

Uses CloudStorageHelper for unified cloud path and credential handling.
"""

import time
from typing import Any, Dict, List

from dvt.federation.loaders.base import BaseLoader, LoadConfig, LoadResult


class SnowflakeLoader(BaseLoader):
    """Snowflake loader using COPY INTO from cloud storage.

    Load priority:
    1. Stage Parquet to cloud -> COPY INTO (for S3/GCS/Azure buckets)
    2. Spark JDBC write (for local filesystem or when bulk load fails)

    COPY INTO provides:
    - Parallel loading from multiple Parquet files
    - Better performance for large datasets
    - Native Snowflake optimization
    """

    adapter_types = ["snowflake"]

    def supports_bulk_load(self, bucket_type: str) -> bool:
        """Snowflake supports bulk load from S3, GCS, and Azure."""
        return bucket_type in ("s3", "gcs", "azure")

    def get_bulk_load_bucket_types(self) -> List[str]:
        """Return supported bucket types for bulk load."""
        return ["s3", "gcs", "azure"]

    def load(
        self,
        df: Any,  # pyspark.sql.DataFrame
        config: LoadConfig,
        adapter: Any = None,
    ) -> LoadResult:
        """Load using COPY INTO for cloud buckets, JDBC for local.

        Args:
            df: PySpark DataFrame to load
            config: Load configuration
            adapter: Optional dbt adapter for DDL operations

        Returns:
            LoadResult with success status and metadata
        """
        bucket_config = config.bucket_config
        bucket_type = bucket_config.get("type") if bucket_config else None

        if bucket_type and self.supports_bulk_load(bucket_type):
            try:
                return self._load_bulk(df, config, bucket_config, adapter)
            except Exception as e:
                self._log(f"COPY INTO failed ({e}), falling back to JDBC...")

        return self._load_jdbc(df, config, adapter)

    def _load_bulk(
        self,
        df: Any,  # pyspark.sql.DataFrame
        config: LoadConfig,
        bucket_config: Dict[str, Any],
        adapter: Any = None,
    ) -> LoadResult:
        """Load using Snowflake COPY INTO from staged Parquet.

        1. Write DataFrame as Parquet to cloud staging
        2. Execute COPY INTO from staging location
        3. Purge staged files after successful load

        Args:
            df: PySpark DataFrame to load
            config: Load configuration
            bucket_config: Bucket configuration for staging

        Returns:
            LoadResult with success status and metadata
        """
        start_time = time.time()

        try:
            import snowflake.connector
        except ImportError:
            raise ImportError(
                "snowflake-connector-python required. "
                "Install with: pip install snowflake-connector-python"
            )

        from dvt.federation.auth.snowflake import SnowflakeAuthHandler
        from dvt.federation.cloud_storage import CloudStorageHelper

        connection = config.connection_config
        if not connection:
            raise ValueError("connection_config required")

        handler = SnowflakeAuthHandler()
        is_valid, error_msg = handler.validate(connection)
        if not is_valid:
            raise ValueError(error_msg)

        # Use CloudStorageHelper for paths and credentials
        helper = CloudStorageHelper(bucket_config)
        bucket_type = bucket_config.get("type")

        # Generate staging paths
        staging_suffix = helper.generate_staging_path("load")
        spark_path = helper.get_spark_path(staging_suffix)
        native_path = helper.get_native_path(staging_suffix, dialect="snowflake")
        creds_clause = helper.get_copy_credentials_clause("snowflake")

        # Write Parquet to cloud staging via Spark
        self._log(f"Staging Parquet to {bucket_type.upper()}...")
        df.write.mode("overwrite").option("compression", "snappy").parquet(spark_path)
        row_count = df.count()

        # Execute DDL via adapter for proper quoting, COPY via native connection
        if adapter:
            from dvt.federation.adapter_manager import get_quoted_table_name

            quoted_table = get_quoted_table_name(adapter, config.table_name)

            # Build COPY INTO SQL with properly quoted table name
            copy_sql = f"""
                COPY INTO {quoted_table}
                FROM '{native_path}'
                FILE_FORMAT = (TYPE = PARQUET)
                {creds_clause}
                MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
                PURGE = TRUE
            """

            with adapter.connection_named("dvt_loader"):
                if config.mode == "overwrite":
                    if config.full_refresh:
                        self._log(f"Dropping {config.table_name} (full refresh)...")
                        adapter.execute(f"DROP TABLE IF EXISTS {quoted_table}")
                        self._safe_commit(adapter)
                        # Create table from DataFrame schema
                        self._create_table_with_adapter(adapter, df, config)
                    elif config.truncate:
                        self._log(f"Truncating {config.table_name}...")
                        try:
                            adapter.execute(f"TRUNCATE TABLE IF EXISTS {quoted_table}")
                        except Exception:
                            pass  # Table may not exist yet
                        self._safe_commit(adapter)

            # COPY INTO uses native connection (needs creds clause)
            connect_kwargs = handler.get_native_connection_kwargs(connection)
            conn = snowflake.connector.connect(**connect_kwargs)
            try:
                cursor = conn.cursor()
                self._log(f"Loading {config.table_name} via COPY INTO...")
                cursor.execute(copy_sql)
                cursor.close()
            finally:
                conn.close()
        else:
            # Fallback: native connection for DDL (no adapter quoting)
            copy_sql = f"""
                COPY INTO {config.table_name}
                FROM '{native_path}'
                FILE_FORMAT = (TYPE = PARQUET)
                {creds_clause}
                MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
                PURGE = TRUE
            """
            connect_kwargs = handler.get_native_connection_kwargs(connection)
            conn = snowflake.connector.connect(**connect_kwargs)
            try:
                cursor = conn.cursor()

                if config.mode == "overwrite":
                    if config.full_refresh:
                        self._log(f"Dropping {config.table_name} (full refresh)...")
                        cursor.execute(f"DROP TABLE IF EXISTS {config.table_name}")
                    elif config.truncate:
                        self._log(f"Truncating {config.table_name}...")
                        try:
                            cursor.execute(
                                f"TRUNCATE TABLE IF EXISTS {config.table_name}"
                            )
                        except Exception:
                            pass

                self._log(f"Loading {config.table_name} via COPY INTO...")
                cursor.execute(copy_sql)
                cursor.close()
            finally:
                conn.close()

        elapsed = time.time() - start_time
        self._log(f"Loaded {row_count:,} rows via COPY INTO in {elapsed:.1f}s")

        return LoadResult(
            success=True,
            table_name=config.table_name,
            row_count=row_count,
            load_method="bulk_load",
            elapsed_seconds=elapsed,
        )
