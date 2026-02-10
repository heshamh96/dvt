"""
DuckDB extractor for EL layer.

Uses native COPY for Parquet export (fast).
Falls back to Spark JDBC if COPY fails.
"""

import time
from pathlib import Path
from typing import Any, Dict, List, Optional

from dvt.federation.extractors.base import (
    BaseExtractor,
    ExtractionConfig,
    ExtractionResult,
)


class DuckDBExtractor(BaseExtractor):
    """DuckDB-specific extractor using native COPY.

    DuckDB has built-in Parquet support via COPY.
    Falls back to Spark JDBC if COPY fails.
    """

    adapter_types = ["duckdb"]

    def _get_connection(self, config: ExtractionConfig = None) -> Any:
        """Get or create a DuckDB database connection.

        If self.connection is None but connection_config is available,
        creates a new connection using duckdb.

        Args:
            config: Optional extraction config with connection_config

        Returns:
            DuckDB database connection
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
            import duckdb
        except ImportError:
            raise ImportError(
                "duckdb is required for DuckDB extraction. "
                "Install with: pip install duckdb"
            )

        # DuckDB connection is typically just a path to the database file
        database = conn_config.get("database") or conn_config.get("path", ":memory:")
        self._lazy_connection = duckdb.connect(database)
        return self._lazy_connection

    def extract(self, config: ExtractionConfig, output_path: Path) -> ExtractionResult:
        """Extract data from DuckDB to Parquet using COPY."""
        try:
            return self._extract_copy(config, output_path)
        except Exception as e:
            self._log(f"COPY failed ({e}), falling back to Spark JDBC...")
            return self._extract_jdbc(config, output_path)

    def _extract_copy(
        self, config: ExtractionConfig, output_path: Path
    ) -> ExtractionResult:
        """Extract using DuckDB COPY TO."""
        start_time = time.time()

        query = self.build_export_query(config)
        output_path.parent.mkdir(parents=True, exist_ok=True)

        copy_sql = (
            f"COPY ({query}) TO '{output_path}' (FORMAT 'parquet', COMPRESSION 'zstd')"
        )
        cursor = self._get_connection(config).cursor()
        cursor.execute(copy_sql)
        cursor.close()

        # Get row count
        count_cursor = self._get_connection(config).cursor()
        count_cursor.execute(f"SELECT COUNT(*) FROM ({query})")
        row_count = count_cursor.fetchone()[0]
        count_cursor.close()

        elapsed = time.time() - start_time
        self._log(
            f"Extracted {row_count:,} rows from {config.source_name} via COPY in {elapsed:.1f}s"
        )

        return ExtractionResult(
            success=True,
            source_name=config.source_name,
            row_count=row_count,
            output_path=output_path,
            extraction_method="copy",
            elapsed_seconds=elapsed,
        )

    def extract_hashes(self, config: ExtractionConfig) -> Dict[str, str]:
        """Extract row hashes using DuckDB MD5 function."""
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
        col_exprs = [f"COALESCE(CAST({c} AS VARCHAR), '')" for c in cols]
        hash_expr = f"MD5(CONCAT_WS('|', {', '.join(col_exprs)}))"

        query = f"""
            SELECT CAST({pk_expr} AS VARCHAR) as _pk, {hash_expr} as _hash
            FROM {config.schema}.{config.table}
        """
        if config.predicates:
            query += f" WHERE {' AND '.join(config.predicates)}"

        cursor = self._get_connection(config).cursor()
        cursor.execute(query)
        hashes = {row[0]: row[1] for row in cursor.fetchall()}
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
        query = f"DESCRIBE {schema}.{table}"
        cursor = self._get_connection(config).cursor()
        cursor.execute(query)
        columns = [{"name": row[0], "type": row[1]} for row in cursor.fetchall()]
        cursor.close()
        return columns

    def detect_primary_key(self, schema: str, table: str) -> List[str]:
        # DuckDB doesn't enforce PKs, return empty
        return []
