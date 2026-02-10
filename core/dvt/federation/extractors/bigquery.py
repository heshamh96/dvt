"""
BigQuery extractor for EL layer.

Supports native EXPORT DATA for GCS buckets.
Falls back to Spark JDBC for other targets.

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


class BigQueryExtractor(BaseExtractor):
    """BigQuery-specific extractor with native cloud export support.

    Extraction priority:
    1. EXPORT DATA for GCS buckets - fastest, parallel
    2. Spark JDBC for local filesystem - parallel reads
    """

    adapter_types = ["bigquery"]

    def supports_native_export(self, bucket_type: str) -> bool:
        """BigQuery supports native export to GCS."""
        return bucket_type == "gcs"

    def get_native_export_bucket_types(self) -> List[str]:
        return ["gcs"]

    def extract(
        self,
        config: ExtractionConfig,
        output_path: Path,
    ) -> ExtractionResult:
        """Extract data from BigQuery to Parquet.

        Uses EXPORT DATA for GCS, Spark JDBC for local.
        """
        bucket_config = config.bucket_config
        bucket_type = bucket_config.get("type") if bucket_config else None

        if bucket_type and bucket_config and self.supports_native_export(bucket_type):
            try:
                return self._extract_native_parallel(config, bucket_config, output_path)
            except Exception as e:
                self._log(f"EXPORT DATA failed ({e}), falling back to Spark JDBC...")

        # Fallback to Spark JDBC (parallel reads)
        return self._extract_jdbc(config, output_path)

    def _extract_native_parallel(
        self,
        config: ExtractionConfig,
        bucket_config: Dict[str, Any],
        output_path: Path,
    ) -> ExtractionResult:
        """Extract using BigQuery EXPORT DATA."""
        start_time = time.time()

        try:
            from dvt.federation.cloud_storage import CloudStorageHelper

            helper = CloudStorageHelper(bucket_config)
            query = self.build_export_query(config)

            # Generate unique staging path
            staging_suffix = helper.generate_staging_path(config.source_name)
            # BigQuery EXPORT DATA uses gs:// paths with wildcard for output
            native_path = helper.get_native_path(staging_suffix, dialect="bigquery")
            gcs_path = f"{native_path}*.parquet"

            export_sql = f"""
                EXPORT DATA OPTIONS(
                    uri='{gcs_path}',
                    format='PARQUET',
                    overwrite=true,
                    compression='ZSTD'
                ) AS
                {query}
            """

            cursor = self.connection.cursor()
            cursor.execute(export_sql)
            cursor.close()

            # Get row count
            count_query = f"SELECT COUNT(*) FROM ({query})"
            cursor = self.connection.cursor()
            cursor.execute(count_query)
            row_count = cursor.fetchone()[0]
            cursor.close()

            elapsed = time.time() - start_time
            self._log(
                f"Exported {row_count:,} rows from {config.source_name} "
                f"to GCS in {elapsed:.1f}s (parallel)"
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
        """Extract row hashes using BigQuery MD5 function."""
        if not config.pk_columns:
            raise ValueError("pk_columns required for hash extraction")

        pk_expr = (
            config.pk_columns[0]
            if len(config.pk_columns) == 1
            else f"CONCAT({', '.join(config.pk_columns)})"
        )

        if config.columns:
            cols = config.columns
        else:
            col_info = self.get_columns(config.schema, config.table)
            cols = [c["name"] for c in col_info]

        # BigQuery uses TO_HEX(MD5(...))
        col_exprs = [f"IFNULL(CAST({c} AS STRING), '')" for c in cols]
        hash_expr = f"TO_HEX(MD5(CONCAT({', '.join(col_exprs)})))"

        query = f"""
            SELECT
                CAST({pk_expr} AS STRING) as _pk,
                {hash_expr} as _hash
            FROM `{config.schema}.{config.table}`
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
        query = f"SELECT COUNT(*) FROM `{schema}.{table}`"
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
        """Get column metadata from INFORMATION_SCHEMA."""
        query = f"""
            SELECT column_name, data_type
            FROM `{schema}.INFORMATION_SCHEMA.COLUMNS`
            WHERE table_name = '{table}'
            ORDER BY ordinal_position
        """
        cursor = self.connection.cursor()
        cursor.execute(query)

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
        """Detect primary key from BigQuery table constraints.

        Note: BigQuery primary keys are relatively new and optional.
        """
        query = f"""
            SELECT column_name
            FROM `{schema}.INFORMATION_SCHEMA.KEY_COLUMN_USAGE`
            WHERE table_name = '{table}'
            AND constraint_name LIKE '%_pk'
            ORDER BY ordinal_position
        """
        cursor = self.connection.cursor()
        try:
            cursor.execute(query)
            pk_cols = [row[0] for row in cursor.fetchall()]
        except Exception:
            pk_cols = []
        finally:
            cursor.close()

        return pk_cols
