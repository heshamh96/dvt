"""
Trino extractor for EL layer.
Uses Spark JDBC for extraction.
Also handles Starburst.
"""

from pathlib import Path
from typing import Any, Dict, List, Optional

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

    def _get_connection(self, config: ExtractionConfig = None) -> Any:
        """Get or create a Trino database connection.

        If self.connection is None but connection_config is available,
        creates a new connection using trino.dbapi.

        Args:
            config: Optional extraction config with connection_config

        Returns:
            Trino database connection
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

        try:
            from trino.dbapi import connect
        except ImportError:
            raise ImportError(
                "trino is required for Trino extraction. "
                "Install with: pip install trino"
            )

        from dvt.federation.auth.trino import TrinoAuthHandler

        handler = TrinoAuthHandler()
        kwargs = handler.get_native_connection_kwargs(conn_config)

        self._lazy_connection = connect(**kwargs)
        return self._lazy_connection

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

        cursor = self._get_connection(config).cursor()
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
        query = f"DESCRIBE {schema}.{table}"
        cursor = self._get_connection(config).cursor()
        cursor.execute(query)
        columns = [{"name": row[0], "type": row[1]} for row in cursor.fetchall()]
        cursor.close()
        return columns

    def detect_primary_key(self, schema: str, table: str) -> List[str]:
        # Trino doesn't enforce PKs
        return []
