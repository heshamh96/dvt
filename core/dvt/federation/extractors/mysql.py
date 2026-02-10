"""
MySQL extractor for EL layer.

Uses Spark JDBC for parallel extraction.
"""

from pathlib import Path
from typing import Dict, List, Optional

from dvt.federation.extractors.base import (
    BaseExtractor,
    ExtractionConfig,
    ExtractionResult,
)


class MySQLExtractor(BaseExtractor):
    """MySQL-specific extractor using Spark JDBC.

    Also works for MySQL-compatible databases:
    - MariaDB, TiDB, SingleStore, StarRocks, Doris
    """

    adapter_types = [
        "mysql",
        "mariadb",
        "tidb",
        "singlestore",
        "starrocks",
        "doris",
    ]

    def extract(
        self,
        config: ExtractionConfig,
        output_path: Path,
    ) -> ExtractionResult:
        """Extract data from MySQL to Parquet using Spark JDBC."""
        return self._extract_jdbc(config, output_path)

    def extract_hashes(
        self,
        config: ExtractionConfig,
    ) -> Dict[str, str]:
        """Extract row hashes using MySQL MD5 function."""
        if not config.pk_columns:
            raise ValueError("pk_columns required for hash extraction")

        # Build MySQL-specific hash query
        pk_expr = (
            config.pk_columns[0]
            if len(config.pk_columns) == 1
            else f"CONCAT_WS('|', {', '.join(config.pk_columns)})"
        )

        # Get columns if not specified
        if config.columns:
            cols = config.columns
        else:
            col_info = self.get_columns(config.schema, config.table)
            cols = [c["name"] for c in col_info]

        # MySQL uses MD5() and CONCAT_WS(), IFNULL for null handling
        col_exprs = [f"IFNULL(CAST({c} AS CHAR), '')" for c in cols]
        hash_expr = f"MD5(CONCAT_WS('|', {', '.join(col_exprs)}))"

        query = f"""
            SELECT
                CAST({pk_expr} AS CHAR) as _pk,
                {hash_expr} as _hash
            FROM {config.schema}.{config.table}
        """

        if config.predicates:
            where_clause = " AND ".join(config.predicates)
            query += f" WHERE {where_clause}"

        cursor = self.connection.cursor()
        cursor.execute(query)

        hashes = {}
        for row in cursor.fetchall():
            hashes[row[0]] = row[1]

        cursor.close()
        return hashes

    def get_row_count(
        self,
        schema: str,
        table: str,
        predicates: Optional[List[str]] = None,
    ) -> int:
        """Get row count using COUNT(*)."""
        query = f"SELECT COUNT(*) FROM {schema}.{table}"
        if predicates:
            query += f" WHERE {' AND '.join(predicates)}"

        cursor = self.connection.cursor()
        cursor.execute(query)
        count = cursor.fetchone()[0]
        cursor.close()
        return count

    def get_columns(
        self,
        schema: str,
        table: str,
    ) -> List[Dict[str, str]]:
        """Get column metadata from information_schema."""
        query = """
            SELECT COLUMN_NAME, DATA_TYPE
            FROM information_schema.COLUMNS
            WHERE TABLE_SCHEMA = %s AND TABLE_NAME = %s
            ORDER BY ORDINAL_POSITION
        """
        cursor = self.connection.cursor()
        cursor.execute(query, (schema, table))

        columns = []
        for row in cursor.fetchall():
            columns.append({"name": row[0], "type": row[1]})

        cursor.close()
        return columns

    def detect_primary_key(
        self,
        schema: str,
        table: str,
    ) -> List[str]:
        """Detect primary key from information_schema."""
        query = """
            SELECT COLUMN_NAME
            FROM information_schema.KEY_COLUMN_USAGE
            WHERE TABLE_SCHEMA = %s
            AND TABLE_NAME = %s
            AND CONSTRAINT_NAME = 'PRIMARY'
            ORDER BY ORDINAL_POSITION
        """
        cursor = self.connection.cursor()
        try:
            cursor.execute(query, (schema, table))
            pk_cols = [row[0] for row in cursor.fetchall()]
        except Exception:
            pk_cols = []
        finally:
            cursor.close()

        return pk_cols
