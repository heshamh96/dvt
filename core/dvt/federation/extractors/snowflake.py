"""
Snowflake extractor for EL layer.

Supports native COPY INTO for cloud buckets (S3, GCS, Azure).
Falls back to Spark JDBC for local filesystem or non-cloud targets.

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


class SnowflakeExtractor(BaseExtractor):
    """Snowflake-specific extractor with native cloud export support.

    Extraction priority:
    1. COPY INTO for cloud buckets (S3, GCS, Azure) - fastest, parallel
    2. Spark JDBC for local filesystem - parallel reads

    Uses COPY INTO for parallel export to S3/GCS/Azure buckets.
    Falls back to Spark JDBC for local filesystem.
    """

    adapter_types = ["snowflake"]

    def supports_native_export(self, bucket_type: str) -> bool:
        """Snowflake supports native export to S3, GCS, and Azure."""
        return bucket_type in ("s3", "gcs", "azure")

    def get_native_export_bucket_types(self) -> List[str]:
        return ["s3", "gcs", "azure"]

    def extract(
        self,
        config: ExtractionConfig,
        output_path: Path,
    ) -> ExtractionResult:
        """Extract data from Snowflake to Parquet.

        Uses native COPY INTO for cloud buckets, Spark JDBC for local.
        """
        bucket_config = config.bucket_config
        bucket_type = bucket_config.get("type") if bucket_config else None

        if bucket_type and bucket_config and self.supports_native_export(bucket_type):
            try:
                return self._extract_native_parallel(config, bucket_config, output_path)
            except Exception as e:
                self._log(f"COPY INTO failed ({e}), falling back to Spark JDBC...")

        # Fallback to Spark JDBC (parallel reads)
        return self._extract_jdbc(config, output_path)

    def _extract_native_parallel(
        self,
        config: ExtractionConfig,
        bucket_config: Dict[str, Any],
        output_path: Path,
    ) -> ExtractionResult:
        """Extract using Snowflake COPY INTO with parallel files."""
        start_time = time.time()

        try:
            from dvt.federation.cloud_storage import CloudStorageHelper

            helper = CloudStorageHelper(bucket_config)
            bucket_type = bucket_config.get("type")
            query = self.build_export_query(config)

            # Generate unique staging path
            staging_suffix = helper.generate_staging_path(config.source_name)
            native_path = helper.get_native_path(staging_suffix, dialect="snowflake")
            creds_clause = helper.get_copy_credentials_clause("snowflake")

            # Build COPY INTO command for export
            copy_sql = f"""
                COPY INTO '{native_path}'
                FROM ({query})
                FILE_FORMAT = (TYPE = PARQUET)
                {creds_clause}
                HEADER = TRUE
                OVERWRITE = TRUE
                MAX_FILE_SIZE = 268435456
            """

            cursor = self.connection.cursor()
            cursor.execute(copy_sql)

            # Get row count from COPY result
            result = cursor.fetchone()
            row_count = result[0] if result else 0

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

    def extract_hashes(
        self,
        config: ExtractionConfig,
    ) -> Dict[str, str]:
        """Extract row hashes using Snowflake MD5 function."""
        if not config.pk_columns:
            raise ValueError("pk_columns required for hash extraction")

        # Snowflake uses TO_VARCHAR for casting and MD5 for hashing
        pk_expr = (
            config.pk_columns[0]
            if len(config.pk_columns) == 1
            else f"CONCAT_WS('|', {', '.join(config.pk_columns)})"
        )

        if config.columns:
            cols = config.columns
        else:
            col_info = self.get_columns(config.schema, config.table)
            cols = [c["name"] for c in col_info]

        # Build hash expression
        col_exprs = [f"COALESCE(TO_VARCHAR({c}), '')" for c in cols]
        hash_expr = f"MD5(CONCAT_WS('|', {', '.join(col_exprs)}))"

        query = f"""
            SELECT
                TO_VARCHAR({pk_expr}) as _pk,
                {hash_expr} as _hash
            FROM {config.schema}.{config.table}
        """

        if config.predicates:
            where_clause = " AND ".join(config.predicates)
            query += f" WHERE {where_clause}"

        cursor = self.connection.cursor()
        cursor.execute(query)

        hashes = {}
        for row in cursor.fetchall():
            hashes[row[0]] = row[1]

        cursor.close()
        return hashes

    def get_row_count(
        self,
        schema: str,
        table: str,
        predicates: Optional[List[str]] = None,
    ) -> int:
        """Get row count using COUNT(*)."""
        query = f"SELECT COUNT(*) FROM {schema}.{table}"
        if predicates:
            query += f" WHERE {' AND '.join(predicates)}"

        cursor = self.connection.cursor()
        cursor.execute(query)
        count = cursor.fetchone()[0]
        cursor.close()
        return count

    def get_columns(
        self,
        schema: str,
        table: str,
    ) -> List[Dict[str, str]]:
        """Get column metadata from information_schema."""
        query = """
            SELECT COLUMN_NAME, DATA_TYPE
            FROM INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_SCHEMA = %s AND TABLE_NAME = %s
            ORDER BY ORDINAL_POSITION
        """
        cursor = self.connection.cursor()
        cursor.execute(query, (schema.upper(), table.upper()))

        columns = []
        for row in cursor.fetchall():
            columns.append({"name": row[0], "type": row[1]})

        cursor.close()
        return columns

    def detect_primary_key(
        self,
        schema: str,
        table: str,
    ) -> List[str]:
        """Detect primary key using SHOW PRIMARY KEYS."""
        query = f"SHOW PRIMARY KEYS IN TABLE {schema}.{table}"

        cursor = self.connection.cursor()
        try:
            cursor.execute(query)
            # Column name is at index 4 in SHOW PRIMARY KEYS result
            pk_cols = [row[4] for row in cursor.fetchall()]
        except Exception:
            pk_cols = []
        finally:
            cursor.close()

        return pk_cols
