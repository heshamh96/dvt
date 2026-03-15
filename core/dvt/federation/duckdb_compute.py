"""
DuckDB Compute Engine for multi-source extraction models.

When a model references multiple remote sources, DVT:
1. Uses Sling to stream each source → DuckDB (in-memory)
2. Runs the model SQL in DuckDB (user writes DuckDB SQL dialect)
3. Uses Sling to stream the DuckDB result → target model table

This module manages the ephemeral DuckDB instance for cross-engine compute.
"""

import logging
import tempfile
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)


class DuckDBCompute:
    """Manages an ephemeral DuckDB instance for cross-engine model execution."""

    def __init__(self, memory_limit: str = "4GB", threads: int = 4) -> None:
        self._conn = None
        self._db_path: Optional[str] = None
        self._memory_limit = memory_limit
        self._threads = threads

    @property
    def db_path(self) -> str:
        """Path to the DuckDB database file (for Sling connection)."""
        if self._db_path is None:
            # Use a temp file so Sling can connect to it
            self._db_path = tempfile.mktemp(suffix=".duckdb", prefix="dvt_compute_")
        return self._db_path

    @property
    def conn(self):
        """Lazy-initialize the DuckDB connection."""
        if self._conn is None:
            try:
                import duckdb
            except ImportError:
                raise RuntimeError(
                    "DuckDB is required for multi-source extraction models but is not installed. "
                    "Run 'dvt sync' to install dependencies."
                )

            self._conn = duckdb.connect(self.db_path)
            self._conn.execute(f"SET memory_limit = '{self._memory_limit}'")
            self._conn.execute(f"SET threads = {self._threads}")
            logger.info(
                f"DuckDB compute initialized: {self.db_path} "
                f"(memory={self._memory_limit}, threads={self._threads})"
            )
        return self._conn

    def execute_model_sql(self, sql: str) -> Any:
        """Execute model SQL in DuckDB and return the relation.

        The model SQL references tables that were previously loaded via Sling.

        Args:
            sql: The model's compiled SQL in DuckDB dialect.

        Returns:
            DuckDB relation with the result.
        """
        logger.info(f"DuckDB compute: executing model SQL ({len(sql)} chars)")
        return self.conn.execute(sql)

    def create_result_table(self, sql: str, table_name: str) -> int:
        """Execute model SQL and store the result as a DuckDB table.

        Args:
            sql: The model's compiled SQL.
            table_name: Name of the result table in DuckDB.

        Returns:
            Row count of the result.
        """
        self.conn.execute(f"CREATE OR REPLACE TABLE {table_name} AS ({sql})")
        result = self.conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()
        row_count = result[0] if result else 0
        logger.info(
            f"DuckDB compute: result table '{table_name}' created with {row_count} rows"
        )
        return row_count

    def list_tables(self) -> List[str]:
        """List all tables in the DuckDB instance."""
        result = self.conn.execute("SHOW TABLES").fetchall()
        return [row[0] for row in result]

    def close(self) -> None:
        """Close the DuckDB connection and clean up temp file."""
        if self._conn is not None:
            try:
                self._conn.close()
            except Exception:
                pass
            self._conn = None

        if self._db_path is not None:
            import os

            try:
                os.unlink(self._db_path)
                # Also remove WAL and other temp files
                for suffix in [".wal", ".tmp"]:
                    wal = self._db_path + suffix
                    if os.path.exists(wal):
                        os.unlink(wal)
            except OSError:
                pass
            self._db_path = None

        logger.info("DuckDB compute: closed and cleaned up")

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()
