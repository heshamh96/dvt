"""
ClickHouse extractor for EL layer.
Uses Spark JDBC for extraction.
"""

from pathlib import Path
from typing import Any, Dict, List, Optional

from dvt.federation.extractors.base import (
    BaseExtractor,
    ExtractionConfig,
    ExtractionResult,
)


class ClickHouseExtractor(BaseExtractor):
    """ClickHouse-specific extractor using Spark JDBC."""

    adapter_types = ["clickhouse"]

    def _get_connection(self, config: ExtractionConfig = None) -> Any:
        """Get or create a ClickHouse database connection.

        If self.connection is None but connection_config is available,
        creates a new connection using clickhouse_connect.

        Args:
            config: Optional extraction config with connection_config

        Returns:
            ClickHouse database connection
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
            import clickhouse_connect
        except ImportError:
            raise ImportError(
                "clickhouse-connect is required for ClickHouse extraction. "
                "Install with: pip install clickhouse-connect"
            )

        from dvt.federation.auth.clickhouse import ClickHouseAuthHandler

        handler = ClickHouseAuthHandler()
        kwargs = handler.get_native_connection_kwargs(conn_config)

        # clickhouse_connect uses get_client instead of connect
        client = clickhouse_connect.get_client(**kwargs)

        # Wrap client to provide cursor() interface
        class ClickHouseConnectionWrapper:
            def __init__(self, ch_client):
                self._client = ch_client

            def cursor(self):
                return ClickHouseCursorWrapper(self._client)

        class ClickHouseCursorWrapper:
            def __init__(self, ch_client):
                self._client = ch_client
                self._result = None

            def execute(self, query, params=None):
                self._result = self._client.query(query)

            def fetchone(self):
                if self._result and self._result.result_rows:
                    return self._result.result_rows[0]
                return None

            def fetchall(self):
                if self._result:
                    return self._result.result_rows
                return []

            def close(self):
                self._result = None

        self._lazy_connection = ClickHouseConnectionWrapper(client)
        return self._lazy_connection

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
        cursor = self._get_connection(config).cursor()
        cursor.execute(f"DESCRIBE TABLE {schema}.{table}")
        columns = [{"name": row[0], "type": row[1]} for row in cursor.fetchall()]
        cursor.close()
        return columns

    def detect_primary_key(
        self, schema: str, table: str, config: ExtractionConfig = None
    ) -> List[str]:
        cursor = self._get_connection(config).cursor()
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
