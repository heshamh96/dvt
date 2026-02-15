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

    def _get_connection(self, config: ExtractionConfig = None) -> Any:
        """Get connection for generic extractor.

        The generic extractor requires a connection to be provided either
        directly or via connection_config. This is a fallback that raises
        a helpful error if no connection is available.
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
                "GenericExtractor requires a connection. "
                "Either provide a connection directly or ensure connection_config "
                "is available. For database-specific extractors with lazy connection "
                "support, use the appropriate extractor (e.g., PostgresExtractor, "
                "SnowflakeExtractor)."
            )

        raise ValueError(
            "No connection provided to GenericExtractor. "
            "Please provide a connection when initializing the extractor."
        )

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

        cursor = self._get_connection(config).cursor()
        cursor.execute(query)

        columns = [desc[0] for desc in cursor.description]
        hashes = {}

        while True:
            batch = cursor.fetchmany(config.batch_size)
            if not batch:
                break
            for row in batch:
                row_dict = dict(zip(columns, row))

                # Build PK value
                if len(config.pk_columns) == 1:
                    pk_value = str(row_dict.get(config.pk_columns[0], ""))
                else:
                    pk_value = "|".join(
                        str(row_dict.get(col, "")) for col in config.pk_columns
                    )

                # Compute hash of all values (use natural column order, not sorted,
                # to stay consistent with DB-specific extractors)
                values = [str(row_dict.get(col, "")) for col in columns]
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
        config: ExtractionConfig = None,
    ) -> int:
        """Get row count using COUNT(*)."""
        query = f"SELECT COUNT(*) FROM {schema}.{table}"
        if predicates:
            query += f" WHERE {' AND '.join(predicates)}"

        cursor = self._get_connection(config).cursor()
        cursor.execute(query)
        count = cursor.fetchone()[0]
        cursor.close()
        return count

    def get_columns(
        self,
        schema: str,
        table: str,
        config: ExtractionConfig = None,
    ) -> List[Dict[str, str]]:
        """Get column names by querying a single row.

        Note: This is a fallback that doesn't get type info.
        Database-specific extractors should override with proper metadata queries.
        """
        # Query one row to get column names
        # Use ANSI SQL FETCH FIRST — more portable than LIMIT (fails on SQL Server, Oracle, DB2)
        query = f"SELECT * FROM {schema}.{table} FETCH FIRST 1 ROWS ONLY"

        cursor = self._get_connection(config).cursor()
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
        config: ExtractionConfig = None,
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
            cursor = self._get_connection(config).cursor()
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
            cursor = self._get_connection(config).cursor()
            cursor.execute(query, (schema, table))
            pk_cols = [row[0] for row in cursor.fetchall()]
            cursor.close()
            return pk_cols
        except Exception:
            pass

        return []
