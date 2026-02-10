"""
Generic fallback extractor for EL layer.

Works with any DB-API 2.0 compliant connection.
Uses Spark JDBC for extraction.
"""

import hashlib
from pathlib import Path
from typing import Any, Dict, List, Optional

from dvt.federation.extractors.base import (
    BaseExtractor,
    ExtractionConfig,
    ExtractionResult,
)


class GenericExtractor(BaseExtractor):
    """Generic extractor that works with any DB-API 2.0 connection.

    Uses Spark JDBC for extraction, Python-side hashing for incremental.
    Used as fallback for databases without optimized extractors.
    """

    # Handles all adapter types as fallback
    adapter_types = ["*"]

    def extract(
        self,
        config: ExtractionConfig,
        output_path: Path,
    ) -> ExtractionResult:
        """Extract data from source to Parquet using Spark JDBC."""
        return self._extract_jdbc(config, output_path)

    def extract_hashes(
        self,
        config: ExtractionConfig,
    ) -> Dict[str, str]:
        """Extract row hashes using Python-side hashing.

        Less efficient than database-side hashing but works universally.
        """
        if not config.pk_columns:
            raise ValueError("pk_columns required for hash extraction")

        query = self.build_export_query(config)

        cursor = self.connection.cursor()
        cursor.execute(query)

        columns = [desc[0] for desc in cursor.description]
        hashes = {}

        for row in cursor.fetchall():
            row_dict = dict(zip(columns, row))

            # Build PK value
            if len(config.pk_columns) == 1:
                pk_value = str(row_dict.get(config.pk_columns[0], ""))
            else:
                pk_value = "|".join(
                    str(row_dict.get(col, "")) for col in config.pk_columns
                )

            # Compute hash of all values
            values = [str(row_dict.get(col, "")) for col in sorted(columns)]
            row_str = "|".join(values)
            row_hash = hashlib.md5(row_str.encode()).hexdigest()

            hashes[pk_value] = row_hash

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
        """Get column names by querying a single row.

        Note: This is a fallback that doesn't get type info.
        Database-specific extractors should override with proper metadata queries.
        """
        # Query one row to get column names
        query = f"SELECT * FROM {schema}.{table} LIMIT 1"

        cursor = self.connection.cursor()
        try:
            cursor.execute(query)
            if cursor.description:
                columns = [
                    {"name": desc[0], "type": "unknown"} for desc in cursor.description
                ]
            else:
                columns = []
        except Exception:
            columns = []
        finally:
            cursor.close()

        return columns

    def detect_primary_key(
        self,
        schema: str,
        table: str,
    ) -> List[str]:
        """Try to detect primary key.

        Generic implementation tries information_schema, returns empty list if not found.
        """
        # Try with ? placeholders (SQLite-style)
        try:
            query = """
                SELECT column_name
                FROM information_schema.key_column_usage
                WHERE table_schema = ? AND table_name = ?
                AND constraint_name = 'PRIMARY'
                ORDER BY ordinal_position
            """
            cursor = self.connection.cursor()
            cursor.execute(query, (schema, table))
            pk_cols = [row[0] for row in cursor.fetchall()]
            cursor.close()
            return pk_cols
        except Exception:
            pass

        # Try with %s placeholders (PostgreSQL-style)
        try:
            query = """
                SELECT column_name
                FROM information_schema.key_column_usage
                WHERE table_schema = %s AND table_name = %s
                AND constraint_name = 'PRIMARY'
                ORDER BY ordinal_position
            """
            cursor = self.connection.cursor()
            cursor.execute(query, (schema, table))
            pk_cols = [row[0] for row in cursor.fetchall()]
            cursor.close()
            return pk_cols
        except Exception:
            pass

        return []
