"""
Exasol extractor for EL layer.
Uses Spark JDBC for extraction.
"""

from pathlib import Path
from typing import Any, Dict, List, Optional

from dvt.federation.extractors.base import (
    BaseExtractor,
    ExtractionConfig,
    ExtractionResult,
)


class ExasolExtractor(BaseExtractor):
    """Exasol-specific extractor using Spark JDBC."""

    adapter_types = ["exasol"]

    def _get_connection(self, config: ExtractionConfig = None) -> Any:
        """Get or create an Exasol database connection."""
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
                "No connection provided and no connection_config available."
            )

        try:
            import pyexasol
        except ImportError:
            raise ImportError(
                "pyexasol is required for Exasol extraction. Install with: pip install pyexasol"
            )

        from dvt.federation.auth.exasol import ExasolAuthHandler

        handler = ExasolAuthHandler()
        kwargs = handler.get_native_connection_kwargs(conn_config)
        self._lazy_connection = pyexasol.connect(**kwargs)
        return self._lazy_connection

    def extract(self, config: ExtractionConfig, output_path: Path) -> ExtractionResult:
        """Extract data from Exasol to Parquet using Spark JDBC."""
        return self._extract_jdbc(config, output_path)

    def extract_hashes(self, config: ExtractionConfig) -> Dict[str, str]:
        """Extract row hashes using Exasol HASH_MD5 function."""
        if not config.pk_columns:
            raise ValueError("pk_columns required for hash extraction")

        pk_expr = (
            config.pk_columns[0]
            if len(config.pk_columns) == 1
            else f"CONCAT({', '.join(config.pk_columns)})"
        )

        cols = config.columns or [
            c["name"] for c in self.get_columns(config.schema, config.table)
        ]
        col_exprs = [f"COALESCE(TO_CHAR({c}), '')" for c in cols]
        hash_expr = f"HASH_MD5(CONCAT({', '.join(col_exprs)}))"

        query = f"""
            SELECT TO_CHAR({pk_expr}) as _pk, {hash_expr} as _hash
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
            SELECT COLUMN_NAME, COLUMN_TYPE
            FROM EXA_ALL_COLUMNS
            WHERE COLUMN_SCHEMA = ? AND COLUMN_TABLE = ?
            ORDER BY COLUMN_ORDINAL_POSITION
        """
        cursor = self._get_connection(config).cursor()
        cursor.execute(query, (schema.upper(), table.upper()))
        columns = [{"name": row[0], "type": row[1]} for row in cursor.fetchall()]
        cursor.close()
        return columns

    def detect_primary_key(
        self, schema: str, table: str, config: ExtractionConfig = None
    ) -> List[str]:
        query = """
            SELECT cc.COLUMN_NAME
            FROM EXA_ALL_CONSTRAINTS c
            JOIN EXA_ALL_CONSTRAINT_COLUMNS cc
                ON c.CONSTRAINT_SCHEMA = cc.CONSTRAINT_SCHEMA
                AND c.CONSTRAINT_TABLE = cc.CONSTRAINT_TABLE
                AND c.CONSTRAINT_NAME = cc.CONSTRAINT_NAME
            WHERE c.CONSTRAINT_SCHEMA = ?
            AND c.CONSTRAINT_TABLE = ?
            AND c.CONSTRAINT_TYPE = 'PRIMARY KEY'
            ORDER BY cc.ORDINAL_POSITION
        """
        cursor = self._get_connection(config).cursor()
        try:
            cursor.execute(query, (schema.upper(), table.upper()))
            pk_cols = [row[0] for row in cursor.fetchall()]
        except Exception:
            pk_cols = []
        cursor.close()
        return pk_cols
