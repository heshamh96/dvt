"""
IBM DB2 extractor for EL layer.
Uses Spark JDBC for extraction.
"""

from pathlib import Path
from typing import Any, Dict, List, Optional

from dvt.federation.extractors.base import (
    BaseExtractor,
    ExtractionConfig,
    ExtractionResult,
)


class DB2Extractor(BaseExtractor):
    """IBM DB2-specific extractor using Spark JDBC."""

    adapter_types = ["db2"]

    def _get_connection(self, config: ExtractionConfig = None) -> Any:
        """Get or create a DB2 database connection."""
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
            import ibm_db_dbi
        except ImportError:
            raise ImportError(
                "ibm_db is required for DB2 extraction. Install with: pip install ibm_db"
            )

        from dvt.federation.auth.db2 import DB2AuthHandler

        handler = DB2AuthHandler()
        kwargs = handler.get_native_connection_kwargs(conn_config)
        self._lazy_connection = ibm_db_dbi.connect(**kwargs)
        return self._lazy_connection

    def extract(self, config: ExtractionConfig, output_path: Path) -> ExtractionResult:
        """Extract data from DB2 to Parquet using Spark JDBC."""
        return self._extract_jdbc(config, output_path)

    def extract_hashes(self, config: ExtractionConfig) -> Dict[str, str]:
        """Extract row hashes using DB2 HASH function."""
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
        col_exprs = [f"COALESCE(CAST({c} AS VARCHAR(32000)), '')" for c in cols]
        hash_expr = f"LOWER(HEX(HASH(CONCAT({', '.join(col_exprs)}), 2)))"

        query = f"""
            SELECT CAST({pk_expr} AS VARCHAR(1000)) as _pk, {hash_expr} as _hash
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
        query = """
            SELECT COLNAME, TYPENAME
            FROM SYSCAT.COLUMNS
            WHERE TABSCHEMA = ? AND TABNAME = ?
            ORDER BY COLNO
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
            SELECT COLNAME
            FROM SYSCAT.KEYCOLUSE
            WHERE TABSCHEMA = ? AND TABNAME = ?
            ORDER BY COLSEQ
        """
        cursor = self._get_connection(config).cursor()
        try:
            cursor.execute(query, (schema.upper(), table.upper()))
            pk_cols = [row[0] for row in cursor.fetchall()]
        except Exception:
            pk_cols = []
        cursor.close()
        return pk_cols
