"""
Databricks extractor for EL layer.

Supports native COPY INTO for cloud storage (S3, GCS, Azure, HDFS).
Falls back to Spark JDBC for local filesystem.

Uses CloudStorageHelper for unified cloud path and credential handling.
"""

import time
from pathlib import Path
from typing import Any, Dict, List, Optional

from dvt.federation.extractors.base import (
    BaseExtractor,
    ExtractionConfig,
    ExtractionResult,
)


class DatabricksExtractor(BaseExtractor):
    """Databricks-specific extractor with native cloud export support.

    Extraction priority:
    1. COPY INTO for cloud buckets (S3, GCS, Azure, HDFS) - fastest, parallel
    2. Spark JDBC for local filesystem - parallel reads
    """

    adapter_types = ["databricks"]

    def _get_connection(self, config: Optional[ExtractionConfig] = None) -> Any:
        """Get or create a Databricks connection.

        If self.connection is None but connection_config is available,
        creates a new connection using databricks-sql-connector.

        Args:
            config: Optional extraction config with connection_config

        Returns:
            Databricks connection
        """
        if self.connection is not None:
            return self.connection

        # Return cached lazy connection if available
        if self._lazy_connection is not None:
            return self._lazy_connection

        # Try to get connection_config from config or instance
        conn_config = None
        if config and config.connection_config:
            conn_config = config.connection_config
        elif self.connection_config:
            conn_config = self.connection_config

        if not conn_config:
            raise ValueError(
                "No connection provided and no connection_config available. "
                "Either provide a connection to the extractor or include "
                "connection_config in ExtractionConfig."
            )

        try:
            from databricks import sql as databricks_sql
        except ImportError:
            raise ImportError(
                "databricks-sql-connector is required for Databricks extraction. "
                "Install with: pip install databricks-sql-connector"
            )

        from dvt.federation.auth.databricks import DatabricksAuthHandler

        handler = DatabricksAuthHandler()
        connect_kwargs = handler.get_native_connection_kwargs(conn_config)

        self._lazy_connection = databricks_sql.connect(**connect_kwargs)
        return self._lazy_connection

    def supports_native_export(self, bucket_type: str) -> bool:
        """Databricks supports native export to S3, GCS, Azure, and HDFS."""
        return bucket_type in ("s3", "gcs", "azure", "hdfs")

    def get_native_export_bucket_types(self) -> List[str]:
        return ["s3", "gcs", "azure", "hdfs"]

    def extract(
        self,
        config: ExtractionConfig,
        output_path: Path,
    ) -> ExtractionResult:
        """Extract data from Databricks to Parquet."""
        bucket_config = config.bucket_config
        bucket_type = bucket_config.get("type") if bucket_config else None

        if bucket_type and bucket_config and self.supports_native_export(bucket_type):
            try:
                return self._extract_native_parallel(config, bucket_config, output_path)
            except Exception as e:
                self._log(
                    f"COPY INTO failed ({e}), falling back to cursor extraction..."
                )

        # For local filesystem, use native cursor-based extraction
        # (avoids dependency on SparkManager)
        try:
            return self._extract_native_cursor(config, output_path)
        except Exception as e:
            self._log(f"Cursor extraction failed ({e}), falling back to Spark JDBC...")
            return self._extract_jdbc(config, output_path)

    def _extract_native_cursor(
        self,
        config: ExtractionConfig,
        output_path: Path,
    ) -> ExtractionResult:
        """Extract using native cursor with streaming PyArrow.

        Uses fetchmany() to process data in batches and writes Parquet
        incrementally via ParquetWriter. Memory: O(batch_size) instead
        of O(dataset).
        """
        start_time = time.time()

        try:
            import pyarrow as pa
            import pyarrow.parquet as pq
        except ImportError:
            raise ImportError(
                "pyarrow is required for cursor extraction. "
                "Install with: pip install pyarrow"
            )

        conn = self._get_connection(config)
        query = self.build_export_query(config)

        cursor = conn.cursor()
        cursor.execute(query)

        column_names = [desc[0] for desc in cursor.description]

        # Ensure output directory exists
        output_path.parent.mkdir(parents=True, exist_ok=True)

        writer = None
        row_count = 0

        try:
            while True:
                rows = cursor.fetchmany(config.batch_size)
                if not rows:
                    break

                # Build RecordBatch from chunk
                arrays = []
                for col_idx in range(len(column_names)):
                    col_data = [row[col_idx] for row in rows]
                    arrays.append(pa.array(col_data))
                batch = pa.record_batch(arrays, names=column_names)

                if writer is None:
                    writer = pq.ParquetWriter(
                        str(output_path),
                        batch.schema,
                        compression="zstd",
                    )
                writer.write_batch(batch)
                row_count += len(rows)
        finally:
            if writer:
                writer.close()
            cursor.close()

        # Handle empty result set (no batches written)
        if writer is None:
            # Write an empty Parquet file with schema
            empty_table = pa.table({name: [] for name in column_names})
            pq.write_table(empty_table, output_path, compression="zstd")

        elapsed = time.time() - start_time

        self._log(
            f"Extracted {row_count:,} rows from {config.source_name} "
            f"via streaming cursor in {elapsed:.1f}s"
        )

        return ExtractionResult(
            success=True,
            source_name=config.source_name,
            row_count=row_count,
            output_path=output_path,
            extraction_method="native_cursor_streaming",
            elapsed_seconds=elapsed,
        )

    def _extract_native_parallel(
        self,
        config: ExtractionConfig,
        bucket_config: Dict[str, Any],
        output_path: Path,
    ) -> ExtractionResult:
        """Extract using Databricks COPY INTO."""
        start_time = time.time()

        try:
            from dvt.federation.cloud_storage import CloudStorageHelper

            bucket_type = bucket_config.get("type")
            query = self.build_export_query(config)

            # Handle HDFS separately (not covered by CloudStorageHelper)
            if bucket_type == "hdfs":
                hdfs_path = bucket_config.get("path", "").rstrip("/")
                import uuid

                export_id = str(uuid.uuid4())[:8]
                cloud_path = f"{hdfs_path}/{config.source_name}_{export_id}/"
            else:
                helper = CloudStorageHelper(bucket_config)
                staging_suffix = helper.generate_staging_path(config.source_name)
                # Use native path for Databricks (s3://, gs://, abfss://)
                cloud_path = helper.get_native_path(
                    staging_suffix, dialect="databricks"
                )

            copy_sql = f"""
                COPY INTO '{cloud_path}'
                FROM ({query})
                FILEFORMAT = PARQUET
            """

            conn = self._get_connection(config)
            cursor = conn.cursor()
            cursor.execute(copy_sql)

            # Get row count
            count_cursor = conn.cursor()
            count_cursor.execute(f"SELECT COUNT(*) FROM ({query}) t")
            row_count = count_cursor.fetchone()[0]
            count_cursor.close()

            cursor.close()
            elapsed = time.time() - start_time

            self._log(
                f"Exported {row_count:,} rows from {config.source_name} "
                f"to {bucket_type.upper()} in {elapsed:.1f}s (parallel)"
            )

            return ExtractionResult(
                success=True,
                source_name=config.source_name,
                row_count=row_count,
                output_path=output_path,
                extraction_method="native_parallel",
                elapsed_seconds=elapsed,
            )

        except Exception as e:
            elapsed = time.time() - start_time
            return ExtractionResult(
                success=False,
                source_name=config.source_name,
                error=str(e),
                elapsed_seconds=elapsed,
            )

    def extract_hashes(self, config: ExtractionConfig) -> Dict[str, str]:
        """Extract row hashes using Spark SQL MD5 function."""
        if not config.pk_columns:
            raise ValueError("pk_columns required for hash extraction")

        pk_expr = (
            config.pk_columns[0]
            if len(config.pk_columns) == 1
            else f"CONCAT_WS('|', {', '.join(config.pk_columns)})"
        )

        cols = config.columns or [
            c["name"] for c in self.get_columns(config.schema, config.table)
        ]
        col_exprs = [f"COALESCE(CAST({c} AS STRING), '')" for c in cols]
        hash_expr = f"MD5(CONCAT_WS('|', {', '.join(col_exprs)}))"

        query = f"""
            SELECT CAST({pk_expr} AS STRING) as _pk, {hash_expr} as _hash
            FROM {config.schema}.{config.table}
        """
        if config.predicates:
            query += f" WHERE {' AND '.join(config.predicates)}"

        conn = self._get_connection(config)
        cursor = conn.cursor()
        cursor.execute(query)
        hashes = {}
        while True:
            batch = cursor.fetchmany(config.batch_size)
            if not batch:
                break
            hashes.update({row[0]: row[1] for row in batch})
        cursor.close()
        return hashes

    def get_row_count(
        self, schema: str, table: str, predicates: Optional[List[str]] = None
    ) -> int:
        query = f"SELECT COUNT(*) FROM {schema}.{table}"
        if predicates:
            query += f" WHERE {' AND '.join(predicates)}"
        conn = self._get_connection()
        cursor = conn.cursor()
        cursor.execute(query)
        count = cursor.fetchone()[0]
        cursor.close()
        return count

    def get_columns(self, schema: str, table: str) -> List[Dict[str, str]]:
        conn = self._get_connection()
        cursor = conn.cursor()
        cursor.execute(f"DESCRIBE TABLE {schema}.{table}")
        columns = []
        for row in cursor.fetchall():
            if row[0] and not row[0].startswith("#"):
                columns.append({"name": row[0], "type": row[1]})
        cursor.close()
        return columns

    def detect_primary_key(self, schema: str, table: str) -> List[str]:
        # Databricks/Delta doesn't have traditional PKs
        return []
