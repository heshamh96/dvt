"""
Athena extractor for EL layer.

Supports native UNLOAD for S3 buckets.
Falls back to Spark JDBC for other targets.
"""

import time
import uuid
from pathlib import Path
from typing import Any, Dict, List, Optional

from dvt.federation.extractors.base import (
    BaseExtractor,
    ExtractionConfig,
    ExtractionResult,
)


class AthenaExtractor(BaseExtractor):
    """Athena-specific extractor with native S3 export support.

    Extraction priority:
    1. UNLOAD for S3 buckets - fastest, parallel
    2. Spark JDBC for local filesystem - parallel reads
    """

    adapter_types = ["athena"]

    def _get_connection(self, config: ExtractionConfig = None) -> Any:
        """Get or create an Athena database connection.

        If self.connection is None but connection_config is available,
        creates a new connection using pyathena.

        Args:
            config: Optional extraction config with connection_config

        Returns:
            Athena database connection
        """
        if self.connection is not None:
            return self.connection

        if self._lazy_connection is not None:
            return self._lazy_connection

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
            from pyathena import connect
        except ImportError:
            raise ImportError(
                "pyathena is required for Athena extraction. "
                "Install with: pip install pyathena"
            )

        from dvt.federation.auth.athena import AthenaAuthHandler

        handler = AthenaAuthHandler()
        kwargs = handler.get_native_connection_kwargs(conn_config)

        self._lazy_connection = connect(**kwargs)
        return self._lazy_connection

    def supports_native_export(self, bucket_type: str) -> bool:
        """Athena supports native export to S3."""
        return bucket_type == "s3"

    def get_native_export_bucket_types(self) -> List[str]:
        return ["s3"]

    def extract(
        self,
        config: ExtractionConfig,
        output_path: Path,
    ) -> ExtractionResult:
        """Extract data from Athena to Parquet."""
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
        """Extract using Athena UNLOAD."""
        start_time = time.time()

        query = self.build_export_query(config)
        export_id = str(uuid.uuid4())[:8]

        bucket_name = bucket_config.get("bucket")
        prefix = bucket_config.get("prefix", "").rstrip("/")
        s3_path = f"s3://{bucket_name}/{prefix}/{config.source_name}_{export_id}/"

        unload_sql = f"""
            UNLOAD ({query})
            TO '{s3_path}'
            WITH (format = 'PARQUET', compression = 'ZSTD')
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
        """Extract row hashes using Presto/Trino MD5 function."""
        if not config.pk_columns:
            raise ValueError("pk_columns required for hash extraction")

        pk_expr = (
            config.pk_columns[0]
            if len(config.pk_columns) == 1
            else "CONCAT(" + ", '|', ".join(config.pk_columns) + ")"
        )

        cols = config.columns or [
            c["name"] for c in self.get_columns(config.schema, config.table)
        ]
        col_exprs = [f"COALESCE(CAST({c} AS VARCHAR), '')" for c in cols]
        concat_hash = ", '|', ".join(col_exprs)
        hash_expr = f"TO_HEX(MD5(TO_UTF8(CONCAT({concat_hash}))))"

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
        # Athena doesn't support parameterized queries for metadata,
        # so sanitize inputs by stripping quotes
        safe_schema = schema.replace("'", "")
        safe_table = table.replace("'", "")
        query = f"""
            SELECT column_name, data_type
            FROM information_schema.columns
            WHERE table_schema = '{safe_schema}' AND table_name = '{safe_table}'
            ORDER BY ordinal_position
        """
        cursor = self._get_connection(config).cursor()
        cursor.execute(query)
        columns = [{"name": row[0], "type": row[1]} for row in cursor.fetchall()]
        cursor.close()
        return columns

    def detect_primary_key(
        self, schema: str, table: str, config: ExtractionConfig = None
    ) -> List[str]:
        # Athena doesn't support primary keys
        return []
