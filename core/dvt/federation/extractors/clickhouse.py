"""
ClickHouse extractor for EL layer.
Uses Spark JDBC for extraction.
"""

from pathlib import Path
from typing import Dict, List, Optional

from dvt.federation.extractors.base import (
    BaseExtractor,
    ExtractionConfig,
    ExtractionResult,
)


class ClickHouseExtractor(BaseExtractor):
    """ClickHouse-specific extractor using Spark JDBC."""

    adapter_types = ["clickhouse"]

    def extract(self, config: ExtractionConfig, output_path: Path) -> ExtractionResult:
        """Extract data from ClickHouse to Parquet using Spark JDBC."""
        return self._extract_jdbc(config, output_path)

    def extract_hashes(self, config: ExtractionConfig) -> Dict[str, str]:
        """Extract row hashes using ClickHouse MD5 function."""
        if not config.pk_columns:
            raise ValueError("pk_columns required for hash extraction")

        pk_expr = (
            config.pk_columns[0]
            if len(config.pk_columns) == 1
            else f"concat({', '.join(config.pk_columns)})"
        )

        cols = config.columns or [
            c["name"] for c in self.get_columns(config.schema, config.table)
        ]
        col_exprs = [f"ifNull(toString({c}), '')" for c in cols]
        hash_expr = f"lower(hex(MD5(concat({', '.join(col_exprs)}))))"

        query = f"""
            SELECT toString({pk_expr}) as _pk, {hash_expr} as _hash
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
        cursor = self.connection.cursor()
        cursor.execute(f"DESCRIBE TABLE {schema}.{table}")
        columns = [{"name": row[0], "type": row[1]} for row in cursor.fetchall()]
        cursor.close()
        return columns

    def detect_primary_key(self, schema: str, table: str) -> List[str]:
        cursor = self.connection.cursor()
        try:
            cursor.execute(
                f"SELECT name FROM system.columns "
                f"WHERE database='{schema}' AND table='{table}' AND is_in_primary_key=1"
            )
            pk_cols = [row[0] for row in cursor.fetchall()]
        except Exception:
            pk_cols = []
        cursor.close()
        return pk_cols
