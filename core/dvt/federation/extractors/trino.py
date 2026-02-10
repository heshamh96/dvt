"""
Trino extractor for EL layer.
Uses Spark JDBC for extraction.
Also handles Starburst.
"""

from pathlib import Path
from typing import Dict, List, Optional

from dvt.federation.extractors.base import (
    BaseExtractor,
    ExtractionConfig,
    ExtractionResult,
)


class TrinoExtractor(BaseExtractor):
    """Trino-specific extractor using Spark JDBC.

    Also works for Starburst (Trino enterprise).
    """

    adapter_types = ["trino", "starburst"]

    def extract(self, config: ExtractionConfig, output_path: Path) -> ExtractionResult:
        """Extract data from Trino to Parquet using Spark JDBC."""
        return self._extract_jdbc(config, output_path)

    def extract_hashes(self, config: ExtractionConfig) -> Dict[str, str]:
        """Extract row hashes using Trino MD5 function."""
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
        col_exprs = [f"COALESCE(CAST({c} AS VARCHAR), '')" for c in cols]
        hash_expr = f"TO_HEX(MD5(TO_UTF8(CONCAT({', '.join(col_exprs)}))))"

        query = f"""
            SELECT CAST({pk_expr} AS VARCHAR) as _pk, {hash_expr} as _hash
            FROM {config.schema}.{config.table}
        """
        if config.predicates:
            query += f" WHERE {' AND '.join(config.predicates)}"

        cursor = self.connection.cursor()
        cursor.execute(query)
        hashes = {row[0]: row[1] for row in cursor.fetchall()}
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
        query = f"DESCRIBE {schema}.{table}"
        cursor = self.connection.cursor()
        cursor.execute(query)
        columns = [{"name": row[0], "type": row[1]} for row in cursor.fetchall()]
        cursor.close()
        return columns

    def detect_primary_key(self, schema: str, table: str) -> List[str]:
        # Trino doesn't enforce PKs
        return []
