"""
Oracle extractor for EL layer.
Uses Spark JDBC for extraction.
"""

from pathlib import Path
from typing import Dict, List, Optional

from dvt.federation.extractors.base import (
    BaseExtractor,
    ExtractionConfig,
    ExtractionResult,
)


class OracleExtractor(BaseExtractor):
    """Oracle-specific extractor using Spark JDBC."""

    adapter_types = ["oracle"]

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
            else f"CONCAT({', '.join(config.pk_columns)})"
        )

        cols = config.columns or [
            c["name"] for c in self.get_columns(config.schema, config.table)
        ]
        col_exprs = [f"NVL(TO_CHAR({c}), '')" for c in cols]
        hash_expr = f"LOWER(RAWTOHEX(DBMS_CRYPTO.HASH(UTL_RAW.CAST_TO_RAW({' || '.join(col_exprs)}), 2)))"

        query = f"""
            SELECT TO_CHAR({pk_expr}) as _pk, {hash_expr} as _hash
            FROM {config.schema}.{config.table}
        """
        if config.predicates:
            query += f" WHERE {' AND '.join(config.predicates)}"

        cursor = self.connection.cursor()
        cursor.execute(query)
        hashes = {str(row[0]): row[1] for row in cursor.fetchall()}
        cursor.close()
        return hashes

    def get_row_count(
        self, schema: str, table: str, predicates: Optional[List[str]] = None
    ) -> int:
        query = f"SELECT COUNT(*) FROM {schema}.{table}"
        if predicates:
            query += f" WHERE {' AND '.join(predicates)}"
        cursor = self.connection.cursor()
        cursor.execute(query)
        count = cursor.fetchone()[0]
        cursor.close()
        return count

    def get_columns(self, schema: str, table: str) -> List[Dict[str, str]]:
        query = """
            SELECT COLUMN_NAME, DATA_TYPE
            FROM ALL_TAB_COLUMNS
            WHERE OWNER = :schema AND TABLE_NAME = :table
            ORDER BY COLUMN_ID
        """
        cursor = self.connection.cursor()
        cursor.execute(query, {"schema": schema.upper(), "table": table.upper()})
        columns = [{"name": row[0], "type": row[1]} for row in cursor.fetchall()]
        cursor.close()
        return columns

    def detect_primary_key(self, schema: str, table: str) -> List[str]:
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
        cursor = self.connection.cursor()
        try:
            cursor.execute(query, {"schema": schema.upper(), "table": table.upper()})
            pk_cols = [row[0] for row in cursor.fetchall()]
        except Exception:
            pk_cols = []
        cursor.close()
        return pk_cols
