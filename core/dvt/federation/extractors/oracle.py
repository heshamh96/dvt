"""
Oracle extractor for EL layer.
Uses Spark JDBC for extraction.
"""

from pathlib import Path
from typing import Any, Dict, List, Optional

from dvt.federation.extractors.base import (
    BaseExtractor,
    ExtractionConfig,
    ExtractionResult,
)


class OracleExtractor(BaseExtractor):
    """Oracle-specific extractor using Spark JDBC."""

    adapter_types = ["oracle"]

    def _get_connection(self, config: ExtractionConfig = None) -> Any:
        """Get or create an Oracle database connection.

        If self.connection is None but connection_config is available,
        creates a new connection using oracledb (or cx_Oracle).

        Args:
            config: Optional extraction config with connection_config

        Returns:
            Oracle database connection
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

        # Try oracledb first (newer), then cx_Oracle
        try:
            import oracledb

            driver = oracledb
        except ImportError:
            try:
                import cx_Oracle

                driver = cx_Oracle
            except ImportError:
                raise ImportError(
                    "oracledb or cx_Oracle is required for Oracle extraction. "
                    "Install with: pip install oracledb"
                )

        from dvt.federation.auth.oracle import OracleAuthHandler

        handler = OracleAuthHandler()
        kwargs = handler.get_native_connection_kwargs(conn_config)

        self._lazy_connection = driver.connect(**kwargs)
        return self._lazy_connection

    def extract(self, config: ExtractionConfig, output_path: Path) -> ExtractionResult:
        """Extract data from Oracle to Parquet using Spark JDBC."""
        return self._extract_jdbc(config, output_path)

    def extract_hashes(self, config: ExtractionConfig) -> Dict[str, str]:
        """Extract row hashes using Oracle DBMS_CRYPTO.HASH."""
        if not config.pk_columns:
            raise ValueError("pk_columns required for hash extraction")

        pk_expr = (
            config.pk_columns[0]
            if len(config.pk_columns) == 1
            else " || '|' || ".join(config.pk_columns)
        )

        cols = config.columns or [
            c["name"] for c in self.get_columns(config.schema, config.table)
        ]
        col_exprs = [f"NVL(TO_CHAR({c}), '')" for c in cols]
        # Use STANDARD_HASH (12c+) — no special grants needed unlike DBMS_CRYPTO
        concat_hash = " || '|' || ".join(col_exprs)
        hash_expr = f"LOWER(STANDARD_HASH({concat_hash}, 'MD5'))"

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
            hashes.update({str(row[0]): row[1] for row in batch})
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
            SELECT COLUMN_NAME, DATA_TYPE
            FROM ALL_TAB_COLUMNS
            WHERE OWNER = :schema AND TABLE_NAME = :table
            ORDER BY COLUMN_ID
        """
        cursor = self._get_connection(config).cursor()
        cursor.execute(query, {"schema": schema.upper(), "table": table.upper()})
        columns = [{"name": row[0], "type": row[1]} for row in cursor.fetchall()]
        cursor.close()
        return columns

    def detect_primary_key(
        self, schema: str, table: str, config: ExtractionConfig = None
    ) -> List[str]:
        query = """
            SELECT cols.COLUMN_NAME
            FROM ALL_CONSTRAINTS cons
            JOIN ALL_CONS_COLUMNS cols
                ON cons.CONSTRAINT_NAME = cols.CONSTRAINT_NAME
                AND cons.OWNER = cols.OWNER
            WHERE cons.CONSTRAINT_TYPE = 'P'
            AND cons.OWNER = :schema
            AND cons.TABLE_NAME = :table
            ORDER BY cols.POSITION
        """
        cursor = self._get_connection(config).cursor()
        try:
            cursor.execute(query, {"schema": schema.upper(), "table": table.upper()})
            pk_cols = [row[0] for row in cursor.fetchall()]
        except Exception:
            pk_cols = []
        cursor.close()
        return pk_cols
