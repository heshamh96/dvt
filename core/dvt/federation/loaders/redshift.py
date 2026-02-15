"""
Redshift loader using COPY from S3 staged Parquet files.

Stages Parquet to S3, then uses COPY for bulk load.
Falls back to JDBC for local filesystem or non-S3 buckets.

Uses CloudStorageHelper for unified cloud path and credential handling.
"""

import time
from typing import Any, Dict, List

from dvt.federation.loaders.base import BaseLoader, LoadConfig, LoadResult


class RedshiftLoader(BaseLoader):
    """Redshift loader using COPY from S3.

    Load priority:
    1. Stage Parquet to S3 -> COPY (for S3 buckets)
    2. Spark JDBC write (for local filesystem or when bulk load fails)

    COPY provides:
    - Parallel loading from multiple Parquet files
    - Better performance for large datasets
    - Native Redshift optimization
    """

    adapter_types = ["redshift"]

    def supports_bulk_load(self, bucket_type: str) -> bool:
        """Redshift supports bulk load from S3 only."""
        return bucket_type == "s3"

    def get_bulk_load_bucket_types(self) -> List[str]:
        """Return supported bucket types for bulk load."""
        return ["s3"]

    def load(
        self,
        df: Any,  # pyspark.sql.DataFrame
        config: LoadConfig,
        adapter: Any = None,
    ) -> LoadResult:
        """Load using COPY for S3 buckets, JDBC for others.

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
                self._log(f"COPY failed ({e}), falling back to JDBC...")

        return self._load_jdbc(df, config, adapter)

    def _load_bulk(
        self,
        df: Any,  # pyspark.sql.DataFrame
        config: LoadConfig,
        bucket_config: Dict[str, Any],
        adapter: Any = None,
    ) -> LoadResult:
        """Load using Redshift COPY from staged Parquet.

        1. Write DataFrame as Parquet to S3 staging
        2. Execute COPY from S3 location

        Args:
            df: PySpark DataFrame to load
            config: Load configuration
            bucket_config: Bucket configuration for staging

        Returns:
            LoadResult with success status and metadata
        """
        start_time = time.time()

        try:
            import redshift_connector
        except ImportError:
            raise ImportError(
                "redshift_connector required. "
                "Install with: pip install redshift-connector"
            )

        from dvt.federation.auth.redshift import RedshiftAuthHandler
        from dvt.federation.cloud_storage import CloudStorageHelper

        connection = config.connection_config
        if not connection:
            raise ValueError("connection_config required")

        handler = RedshiftAuthHandler()
        is_valid, error_msg = handler.validate(connection)
        if not is_valid:
            raise ValueError(error_msg)

        # Use CloudStorageHelper for paths and credentials
        helper = CloudStorageHelper(bucket_config)
        region = bucket_config.get("region", "us-east-1")

        # Generate staging paths
        staging_suffix = helper.generate_staging_path("load")
        spark_path = helper.get_spark_path(staging_suffix)
        native_path = helper.get_native_path(staging_suffix, dialect="redshift")
        creds_clause = helper.get_copy_credentials_clause("redshift")

        # Write Parquet to S3 via Spark
        self._log(f"Staging Parquet to S3...")
        df.write.mode("overwrite").option("compression", "snappy").parquet(spark_path)
        row_count = df.count()

        # Execute DDL + COPY via adapter for proper quoting
        if adapter:
            from dvt.federation.adapter_manager import get_quoted_table_name

            quoted_table = get_quoted_table_name(adapter, config.table_name)

            # Build COPY SQL with properly quoted table name
            copy_sql = f"""
                COPY {quoted_table}
                FROM '{native_path}'
                {creds_clause}
                FORMAT AS PARQUET
                REGION '{region}'
            """

            with adapter.connection_named("dvt_loader"):
                if config.mode == "overwrite":
                    if config.full_refresh:
                        self._log(
                            f"Dropping {config.table_name} CASCADE (full refresh)..."
                        )
                        adapter.execute(f"DROP TABLE IF EXISTS {quoted_table} CASCADE")
                        self._safe_commit(adapter)
                        # Create table from DataFrame schema
                        self._create_table_with_adapter(adapter, df, config)
                    elif config.truncate:
                        self._log(f"Truncating {config.table_name}...")
                        try:
                            adapter.execute(f"TRUNCATE TABLE {quoted_table}")
                        except Exception:
                            pass  # Table may not exist yet - COPY will create it
                        self._safe_commit(adapter)

            # COPY still uses native connection (adapter can't do COPY with creds)
            connect_kwargs = handler.get_native_connection_kwargs(connection)
            conn = redshift_connector.connect(**connect_kwargs)
            try:
                conn.autocommit = True
                cursor = conn.cursor()
                self._log(f"Loading {config.table_name} via COPY...")
                cursor.execute(copy_sql)
                cursor.close()
            finally:
                conn.close()
        else:
            # Fallback: native connection for DDL (no adapter quoting)
            copy_sql = f"""
                COPY {config.table_name}
                FROM '{native_path}'
                {creds_clause}
                FORMAT AS PARQUET
                REGION '{region}'
            """
            connect_kwargs = handler.get_native_connection_kwargs(connection)
            conn = redshift_connector.connect(**connect_kwargs)
            try:
                conn.autocommit = True
                cursor = conn.cursor()

                if config.mode == "overwrite":
                    if config.full_refresh:
                        self._log(
                            f"Dropping {config.table_name} CASCADE (full refresh)..."
                        )
                        cursor.execute(
                            f"DROP TABLE IF EXISTS {config.table_name} CASCADE"
                        )
                    elif config.truncate:
                        self._log(f"Truncating {config.table_name}...")
                        try:
                            cursor.execute(f"TRUNCATE TABLE {config.table_name}")
                        except Exception:
                            pass

                self._log(f"Loading {config.table_name} via COPY...")
                cursor.execute(copy_sql)
                cursor.close()
            finally:
                conn.close()

        elapsed = time.time() - start_time
        self._log(f"Loaded {row_count:,} rows via COPY in {elapsed:.1f}s")

        return LoadResult(
            success=True,
            table_name=config.table_name,
            row_count=row_count,
            load_method="bulk_load",
            elapsed_seconds=elapsed,
        )
