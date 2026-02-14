"""
Spark extractor for EL layer.
Uses Spark JDBC for extraction - writes to staging for consistency.
"""

from pathlib import Path
from typing import Dict, List, Optional

from dvt.federation.extractors.base import (
    BaseExtractor,
    ExtractionConfig,
    ExtractionResult,
)


class SparkExtractor(BaseExtractor):
    """Spark-specific extractor using Spark JDBC.

    Even though data may already be in Spark, we write to staging
    for consistency with the EL layer pipeline.
    """

    adapter_types = ["spark"]

    def extract(self, config: ExtractionConfig, output_path: Path) -> ExtractionResult:
        """Extract data from Spark to Parquet using Spark JDBC."""
        return self._extract_jdbc(config, output_path)

    def extract_hashes(self, config: ExtractionConfig) -> Dict[str, str]:
        """Extract row hashes using Spark SQL MD5 function."""
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
        col_exprs = [f"COALESCE(CAST({c} AS STRING), '')" for c in cols]
        hash_expr = f"MD5(CONCAT_WS('|', {', '.join(col_exprs)}))"

        query = f"""
            SELECT CAST({pk_expr} AS STRING) as _pk, {hash_expr} as _hash
            FROM {config.schema}.{config.table}
        """
        if config.predicates:
            query += f" WHERE {' AND '.join(config.predicates)}"

        cursor = self.connection.cursor()
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
        cursor.execute(f"DESCRIBE {schema}.{table}")
        columns = []
        for row in cursor.fetchall():
            if row[0] and not row[0].startswith("#") and row[0].strip():
                columns.append({"name": row[0], "type": row[1]})
        cursor.close()
        return columns

    def detect_primary_key(self, schema: str, table: str) -> List[str]:
        # Spark SQL doesn't support PKs
        return []
