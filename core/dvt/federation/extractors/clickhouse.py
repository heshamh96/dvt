"""
ClickHouse extractor for EL layer.

Extraction priority:
1. Pipe-based: clickhouse-client --query | PyArrow streaming (if CLI on PATH)
2. Spark JDBC: parallel reads (default fallback)
"""

import os
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

    cli_tool = "clickhouse-client"

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
                self._row_idx = 0

            def execute(self, query, params=None):
                self._result = self._client.query(query)
                self._row_idx = 0

            def fetchone(self):
                if self._result and self._row_idx < len(self._result.result_rows):
                    row = self._result.result_rows[self._row_idx]
                    self._row_idx += 1
                    return row
                return None

            def fetchmany(self, size=1):
                if not self._result:
                    return []
                rows = self._result.result_rows
                end = min(self._row_idx + size, len(rows))
                batch = rows[self._row_idx : end]
                self._row_idx = end
                return batch

            def fetchall(self):
                if self._result:
                    remaining = self._result.result_rows[self._row_idx :]
                    self._row_idx = len(self._result.result_rows)
                    return remaining
                return []

            def close(self):
                self._result = None
                self._row_idx = 0

        self._lazy_connection = ClickHouseConnectionWrapper(client)
        return self._lazy_connection

    def _build_extraction_command(self, config: ExtractionConfig) -> List[str]:
        """Build clickhouse-client command for CSVWithNames output."""
        conn_config = config.connection_config or self.connection_config or {}
        query = self.build_export_query(config)
        cmd = [
            "clickhouse-client",
            "--host",
            conn_config.get("host", "localhost"),
            "--port",
            str(conn_config.get("port", 9000)),
            "--user",
            conn_config.get("user", "default"),
            "--database",
            conn_config.get("database", "default"),
            "--query",
            f"{query} FORMAT CSVWithNames",
        ]
        return cmd

    def _build_extraction_env(self, config: ExtractionConfig) -> Dict[str, str]:
        """Build env with CLICKHOUSE_PASSWORD for clickhouse-client subprocess."""
        conn_config = config.connection_config or self.connection_config or {}
        env = os.environ.copy()
        password = conn_config.get("password", "")
        if password:
            env["CLICKHOUSE_PASSWORD"] = str(password)
        return env

    def extract(self, config: ExtractionConfig, output_path: Path) -> ExtractionResult:
        """Extract data from ClickHouse to Parquet.

        Tries pipe (clickhouse-client) first, falls back to Spark JDBC.
        """
        if self._has_cli_tool():
            try:
                return self._extract_via_pipe(config, output_path)
            except Exception as e:
                self._log(f"Pipe extraction failed ({e}), falling back to JDBC...")

        return self._extract_jdbc(config, output_path)

    def extract_hashes(self, config: ExtractionConfig) -> Dict[str, str]:
        """Extract row hashes using ClickHouse MD5 function."""
        if not config.pk_columns:
            raise ValueError("pk_columns required for hash extraction")

        pk_expr = (
            config.pk_columns[0]
            if len(config.pk_columns) == 1
            else "concat(" + ", '|', ".join(config.pk_columns) + ")"
        )

        cols = config.columns or [
            c["name"] for c in self.get_columns(config.schema, config.table)
        ]
        col_exprs = [f"ifNull(toString({c}), '')" for c in cols]
        concat_hash = ", '|', ".join(col_exprs)
        hash_expr = f"lower(hex(MD5(concat({concat_hash}))))"

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
            # Use {schema:Identifier} / {table:Identifier} to avoid SQL injection
            # clickhouse_connect doesn't support standard parameterized queries
            # for system tables, so we sanitize by stripping quotes
            safe_schema = schema.replace("'", "")
            safe_table = table.replace("'", "")
            cursor.execute(
                f"SELECT name FROM system.columns "
                f"WHERE database = '{safe_schema}' AND table = '{safe_table}' "
                f"AND is_in_primary_key = 1"
            )
            pk_cols = [row[0] for row in cursor.fetchall()]
        except Exception:
            pk_cols = []
        cursor.close()
        return pk_cols
