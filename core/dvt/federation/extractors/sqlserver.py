"""
SQL Server extractor for EL layer.
Uses Spark JDBC for extraction.
Also handles Azure Synapse and Fabric.
"""

from pathlib import Path
from typing import Dict, List, Optional

from dvt.federation.extractors.base import (
    BaseExtractor,
    ExtractionConfig,
    ExtractionResult,
)


class SQLServerExtractor(BaseExtractor):
    """SQL Server-specific extractor using Spark JDBC.

    Also works for Azure Synapse and Microsoft Fabric.
    """

    adapter_types = ["sqlserver", "synapse", "fabric"]

    def extract(self, config: ExtractionConfig, output_path: Path) -> ExtractionResult:
        """Extract data from SQL Server to Parquet using Spark JDBC."""
        return self._extract_jdbc(config, output_path)

    def extract_hashes(self, config: ExtractionConfig) -> Dict[str, str]:
        """Extract row hashes using SQL Server HASHBYTES."""
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
        col_exprs = [f"ISNULL(CAST({c} AS NVARCHAR(MAX)), '')" for c in cols]
        hash_expr = f"LOWER(CONVERT(VARCHAR(32), HASHBYTES('MD5', CONCAT({', '.join(col_exprs)})), 2))"

        query = f"""
            SELECT CAST({pk_expr} AS NVARCHAR(MAX)) as _pk, {hash_expr} as _hash
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
        query = f"SELECT COUNT(*) FROM [{schema}].[{table}]"
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
            FROM INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?
            ORDER BY ORDINAL_POSITION
        """
        cursor = self.connection.cursor()
        cursor.execute(query, (schema, table))
        columns = [{"name": row[0], "type": row[1]} for row in cursor.fetchall()]
        cursor.close()
        return columns

    def detect_primary_key(self, schema: str, table: str) -> List[str]:
        query = """
            SELECT c.COLUMN_NAME
            FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS tc
            JOIN INFORMATION_SCHEMA.CONSTRAINT_COLUMN_USAGE c
                ON tc.CONSTRAINT_NAME = c.CONSTRAINT_NAME
            WHERE tc.TABLE_SCHEMA = ?
            AND tc.TABLE_NAME = ?
            AND tc.CONSTRAINT_TYPE = 'PRIMARY KEY'
            ORDER BY c.ORDINAL_POSITION
        """
        cursor = self.connection.cursor()
        try:
            cursor.execute(query, (schema, table))
            pk_cols = [row[0] for row in cursor.fetchall()]
        except Exception:
            pk_cols = []
        cursor.close()
        return pk_cols
