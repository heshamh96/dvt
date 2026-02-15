"""
BigQuery loader using LOAD DATA from GCS staged Parquet files.

Stages Parquet to GCS, then uses LOAD DATA for bulk load.
Falls back to JDBC for local filesystem or non-GCS buckets.

Uses CloudStorageHelper for unified cloud path and credential handling.
"""

import time
from typing import Any, Dict, List

from dvt.federation.loaders.base import BaseLoader, LoadConfig, LoadResult


class BigQueryLoader(BaseLoader):
    """BigQuery loader using LOAD DATA from GCS.

    Load priority:
    1. Stage Parquet to GCS -> LOAD DATA (for GCS buckets)
    2. Spark JDBC write (for local filesystem or when bulk load fails)

    LOAD DATA provides:
    - Parallel loading from multiple Parquet files
    - Better performance for large datasets
    - Native BigQuery optimization
    """

    adapter_types = ["bigquery"]

    def supports_bulk_load(self, bucket_type: str) -> bool:
        """BigQuery supports bulk load from GCS only."""
        return bucket_type == "gcs"

    def get_bulk_load_bucket_types(self) -> List[str]:
        """Return supported bucket types for bulk load."""
        return ["gcs"]

    def load(
        self,
        df: Any,  # pyspark.sql.DataFrame
        config: LoadConfig,
        adapter: Any = None,
    ) -> LoadResult:
        """Load using LOAD DATA for GCS buckets, JDBC for others.

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
                self._log(f"LOAD DATA failed ({e}), falling back to JDBC...")

        return self._load_jdbc(df, config, adapter)

    def _load_bulk(
        self,
        df: Any,  # pyspark.sql.DataFrame
        config: LoadConfig,
        bucket_config: Dict[str, Any],
        adapter: Any = None,
    ) -> LoadResult:
        """Load using BigQuery LOAD DATA from staged Parquet.

        1. Write DataFrame as Parquet to GCS staging
        2. Execute LOAD DATA from GCS location

        Args:
            df: PySpark DataFrame to load
            config: Load configuration
            bucket_config: Bucket configuration for staging

        Returns:
            LoadResult with success status and metadata
        """
        start_time = time.time()

        try:
            from google.cloud import bigquery
            from google.oauth2 import service_account
        except ImportError:
            raise ImportError(
                "google-cloud-bigquery required. "
                "Install with: pip install google-cloud-bigquery"
            )

        from dvt.federation.auth.bigquery import BigQueryAuthHandler
        from dvt.federation.cloud_storage import CloudStorageHelper

        connection = config.connection_config
        if not connection:
            raise ValueError("connection_config required")

        handler = BigQueryAuthHandler()
        is_valid, error_msg = handler.validate(connection)
        if not is_valid:
            raise ValueError(error_msg)

        # Use CloudStorageHelper for paths
        helper = CloudStorageHelper(bucket_config)
        staging_suffix = helper.generate_staging_path("load")
        spark_path = helper.get_spark_path(staging_suffix)

        # Write Parquet to GCS via Spark
        self._log(f"Staging Parquet to GCS...")
        df.write.mode("overwrite").option("compression", "snappy").parquet(spark_path)
        row_count = df.count()

        # Get BigQuery client
        native_kwargs = handler.get_native_connection_kwargs(connection)
        project = native_kwargs.get("project", connection.get("project", ""))

        # Build credentials
        if native_kwargs.get("keyfile"):
            credentials = service_account.Credentials.from_service_account_file(
                native_kwargs["keyfile"]
            )
            client = bigquery.Client(project=project, credentials=credentials)
        elif native_kwargs.get("keyfile_json"):
            credentials = service_account.Credentials.from_service_account_info(
                native_kwargs["keyfile_json"]
            )
            client = bigquery.Client(project=project, credentials=credentials)
        else:
            # Use application default credentials
            client = bigquery.Client(project=project)

        try:
            # Handle DDL via adapter for proper quoting when available
            if adapter and config.mode == "overwrite":
                from dvt.federation.adapter_manager import get_quoted_table_name

                quoted_table = get_quoted_table_name(adapter, config.table_name)

                with adapter.connection_named("dvt_loader"):
                    if config.full_refresh:
                        self._log(f"Dropping {config.table_name} (full refresh)...")
                        try:
                            adapter.execute(f"DROP TABLE IF EXISTS {quoted_table}")
                        except Exception:
                            pass
                        self._safe_commit(adapter)
                        # Create table from DataFrame schema
                        self._create_table_with_adapter(adapter, df, config)
                    elif config.truncate:
                        self._log(f"Truncating {config.table_name}...")
                        try:
                            adapter.execute(f"TRUNCATE TABLE {quoted_table}")
                        except Exception:
                            pass  # Table may not exist yet
                        self._safe_commit(adapter)
            elif config.mode == "overwrite":
                # Fallback: DDL via BigQuery client (no adapter quoting)
                if config.full_refresh:
                    self._log(f"Dropping {config.table_name} (full refresh)...")
                    try:
                        client.query(
                            f"DROP TABLE IF EXISTS {config.table_name}"
                        ).result()
                    except Exception:
                        pass
                elif config.truncate:
                    self._log(f"Truncating {config.table_name}...")
                    try:
                        client.query(f"TRUNCATE TABLE {config.table_name}").result()
                    except Exception:
                        pass

            # Use BigQuery load job for Parquet files
            self._log(f"Loading {config.table_name} via LOAD DATA...")

            # Configure load job
            # DDL already handled above — use WRITE_APPEND to avoid
            # redundant truncation by the load job itself.
            # autodetect=True lets BQ infer schema from Parquet when
            # table was DROPped without explicit CREATE (no-adapter fallback).
            job_config = bigquery.LoadJobConfig(
                source_format=bigquery.SourceFormat.PARQUET,
                write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
                autodetect=True,
            )

            # Load from GCS (use spark_path which is gs:// format)
            load_job = client.load_table_from_uri(
                f"{spark_path}*.parquet",
                config.table_name,
                job_config=job_config,
            )

            # Wait for job to complete
            load_job.result()

        finally:
            client.close()

        elapsed = time.time() - start_time
        self._log(f"Loaded {row_count:,} rows via LOAD DATA in {elapsed:.1f}s")

        return LoadResult(
            success=True,
            table_name=config.table_name,
            row_count=row_count,
            load_method="bulk_load",
            elapsed_seconds=elapsed,
        )
