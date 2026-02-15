"""
Redshift extractor for EL layer.

Supports native UNLOAD for S3 buckets.
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


class RedshiftExtractor(BaseExtractor):
    """Redshift-specific extractor with native S3 export support.

    Extraction priority:
    1. UNLOAD for S3 buckets - fastest, parallel
    2. Spark JDBC for local filesystem - parallel reads
    """

    adapter_types = ["redshift"]

    def _get_connection(self, config: ExtractionConfig = None) -> Any:
        """Get or create a Redshift database connection.

        If self.connection is None but connection_config is available,
        creates a new connection using psycopg2 (Redshift is PostgreSQL-compatible).

        Args:
            config: Optional extraction config with connection_config

        Returns:
            Redshift database connection
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
            import psycopg2
        except ImportError:
            raise ImportError(
                "psycopg2 is required for Redshift extraction. "
                "Install with: pip install psycopg2-binary"
            )

        from dvt.federation.auth.redshift import RedshiftAuthHandler

        handler = RedshiftAuthHandler()
        connect_kwargs = handler.get_native_connection_kwargs(conn_config)

        self._lazy_connection = psycopg2.connect(**connect_kwargs)
        return self._lazy_connection

    def supports_native_export(self, bucket_type: str) -> bool:
        """Redshift supports native export to S3."""
        return bucket_type == "s3"

    def get_native_export_bucket_types(self) -> List[str]:
        return ["s3"]

    def extract(
        self,
        config: ExtractionConfig,
        output_path: Path,
    ) -> ExtractionResult:
        """Extract data from Redshift to Parquet."""
        bucket_config = config.bucket_config
        bucket_type = bucket_config.get("type") if bucket_config else None

        if bucket_type and bucket_config and self.supports_native_export(bucket_type):
            try:
                return self._extract_native_parallel(config, bucket_config, output_path)
            except Exception as e:
                self._log(f"UNLOAD failed ({e}), falling back to Spark JDBC...")

        return self._extract_jdbc(config, output_path)

    def _extract_native_parallel(
        self,
        config: ExtractionConfig,
        bucket_config: Dict[str, Any],
        output_path: Path,
    ) -> ExtractionResult:
        """Extract using Redshift UNLOAD."""
        start_time = time.time()

        from dvt.federation.cloud_storage import CloudStorageHelper

        helper = CloudStorageHelper(bucket_config)
        query = self.build_export_query(config)

        # Generate unique staging path
        staging_suffix = helper.generate_staging_path(config.source_name)
        native_path = helper.get_native_path(staging_suffix, dialect="redshift")

        # Get credentials clause (IAM role or access keys)
        creds_clause = helper.get_copy_credentials_clause("redshift")
        if not creds_clause:
            creds_clause = "IAM_ROLE DEFAULT"

        unload_sql = f"""
            UNLOAD ('{query.replace("'", "''")}')
            TO '{native_path}'
            {creds_clause}
            FORMAT PARQUET
            ALLOWOVERWRITE
            PARALLEL ON
        """

        cursor = self._get_connection(config).cursor()
        cursor.execute(unload_sql)
        cursor.close()

        # Get row count
        count_cursor = self._get_connection(config).cursor()
        count_cursor.execute(f"SELECT COUNT(*) FROM ({query})")
        row_count = count_cursor.fetchone()[0]
        count_cursor.close()

        elapsed = time.time() - start_time
        self._log(
            f"Exported {row_count:,} rows from {config.source_name} "
            f"to S3 in {elapsed:.1f}s (parallel)"
        )

        return ExtractionResult(
            success=True,
            source_name=config.source_name,
            row_count=row_count,
            output_path=output_path,
            extraction_method="native_parallel",
            elapsed_seconds=elapsed,
        )

    def extract_hashes(self, config: ExtractionConfig) -> Dict[str, str]:
        """Extract row hashes using Redshift MD5 function."""
        if not config.pk_columns:
            raise ValueError("pk_columns required for hash extraction")

        pk_expr = (
            config.pk_columns[0]
            if len(config.pk_columns) == 1
            else " || '|' || ".join(config.pk_columns)
        )

        cols = config.columns or [
            c["name"] for c in self.get_columns(config.schema, config.table, config)
        ]
        col_exprs = [f"NVL(CAST({c} AS VARCHAR), '')" for c in cols]
        concat_hash = " || '|' || ".join(col_exprs)
        hash_expr = f"MD5({concat_hash})"

        query = f"""
            SELECT CAST({pk_expr} AS VARCHAR) as _pk, {hash_expr} as _hash
            FROM {config.schema}.{config.table}
        """
        if config.predicates:
            query += f" WHERE {' AND '.join(config.predicates)}"

        cursor = self._get_connection(config).cursor()
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
        self,
        schema: str,
        table: str,
        predicates: Optional[List[str]] = None,
        config: ExtractionConfig = None,
    ) -> int:
        query = f"SELECT COUNT(*) FROM {schema}.{table}"
        if predicates:
            query += f" WHERE {' AND '.join(predicates)}"
        cursor = self._get_connection(config).cursor()
        cursor.execute(query)
        count = cursor.fetchone()[0]
        cursor.close()
        return count

    def get_columns(
        self, schema: str, table: str, config: ExtractionConfig = None
    ) -> List[Dict[str, str]]:
        query = """
            SELECT column_name, data_type
            FROM information_schema.columns
            WHERE table_schema = %s AND table_name = %s
            ORDER BY ordinal_position
        """
        cursor = self._get_connection(config).cursor()
        cursor.execute(query, (schema, table))
        columns = [{"name": row[0], "type": row[1]} for row in cursor.fetchall()]
        cursor.close()
        return columns

    def detect_primary_key(
        self, schema: str, table: str, config: ExtractionConfig = None
    ) -> List[str]:
        query = """
            SELECT kcu.column_name
            FROM information_schema.table_constraints tc
            JOIN information_schema.key_column_usage kcu
                ON tc.constraint_name = kcu.constraint_name
                AND tc.table_schema = kcu.table_schema
            WHERE tc.table_schema = %s
            AND tc.table_name = %s
            AND tc.constraint_type = 'PRIMARY KEY'
            ORDER BY kcu.ordinal_position
        """
        cursor = self._get_connection(config).cursor()
        try:
            cursor.execute(query, (schema, table))
            pk_cols = [row[0] for row in cursor.fetchall()]
        except Exception:
            pk_cols = []
        cursor.close()
        return pk_cols
