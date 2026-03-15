"""
Sling client wrapper for DVT.

Provides methods for the three Sling operations DVT performs:
1. extract_to_target() — stream source table → target model table (Sling Direct)
2. extract_to_duckdb() — stream source table → DuckDB in-memory (for multi-source)
3. load_from_duckdb() — stream DuckDB query result → target model table
4. load_seed() — stream CSV file → target table
5. load_cross_target() — stream from one target → another target
"""

import logging
from typing import Any, Dict, List, Optional

from dvt.extraction.connection_mapper import map_to_sling_url

logger = logging.getLogger(__name__)


class SlingClient:
    """Wrapper around the Sling Python package for DVT operations."""

    def __init__(self) -> None:
        try:
            from sling import Replication, ReplicationStream

            self._Replication = Replication
            self._ReplicationStream = ReplicationStream
            self._available = True
        except ImportError:
            self._available = False

    @property
    def available(self) -> bool:
        return self._available

    def _check_available(self) -> None:
        if not self._available:
            raise RuntimeError(
                "DVT106: Sling is required for cross-engine extraction but was not found. "
                "Run 'dvt sync' or install Sling manually."
            )

    def extract_to_target(
        self,
        source_config: Dict[str, Any],
        target_config: Dict[str, Any],
        source_query: str,
        target_table: str,
        mode: str = "full-refresh",
        primary_key: Optional[List[str]] = None,
    ) -> Dict[str, Any]:
        """Extract data from a source via Sling and load directly into the target table.

        This is the Sling Direct path — used for single-source extraction models.

        Args:
            source_config: profiles.yml output config for the source connection.
            target_config: profiles.yml output config for the target connection.
            source_query: SQL query to execute on the source (the model's compiled SQL).
            target_table: Fully-qualified target table name (schema.model_name).
            mode: Sling mode — 'full-refresh', 'incremental', 'truncate'.
            primary_key: Column(s) for merge in incremental mode.

        Returns:
            Dict with execution results (rows, duration, status).
        """
        self._check_available()

        src_url = map_to_sling_url(source_config)
        tgt_url = map_to_sling_url(target_config)

        stream_config = {
            "object": target_table,
            "mode": mode,
        }
        if primary_key:
            stream_config["primary_key"] = primary_key

        logger.info(
            f"Sling Direct: {source_config.get('type', '?')} → "
            f"{target_config.get('type', '?')} [{target_table}] mode={mode}"
        )

        replication = self._Replication(
            source=src_url,
            target=tgt_url,
            streams={
                f"custom_sql": self._ReplicationStream(
                    sql=source_query,
                    **stream_config,
                ),
            },
        )
        replication.run()

        return {"status": "success", "target_table": target_table, "mode": mode}

    def extract_to_duckdb(
        self,
        source_config: Dict[str, Any],
        duckdb_path: str,
        source_query: str,
        duckdb_table: str,
    ) -> Dict[str, Any]:
        """Extract data from a source via Sling into a DuckDB table.

        Used for the DuckDB Compute path — multi-source extraction.

        Args:
            source_config: profiles.yml output config for the source.
            duckdb_path: Path to the DuckDB database file (or ":memory:").
            source_query: SQL to execute on the source.
            duckdb_table: Table name within DuckDB to load into.

        Returns:
            Dict with execution results.
        """
        self._check_available()

        src_url = map_to_sling_url(source_config)
        tgt_url = f"duckdb://{duckdb_path}"

        logger.info(
            f"Sling → DuckDB: {source_config.get('type', '?')} → "
            f"duckdb [{duckdb_table}]"
        )

        replication = self._Replication(
            source=src_url,
            target=tgt_url,
            streams={
                f"custom_sql": self._ReplicationStream(
                    sql=source_query,
                    object=duckdb_table,
                    mode="full-refresh",
                ),
            },
        )
        replication.run()

        return {"status": "success", "duckdb_table": duckdb_table}

    def load_from_duckdb(
        self,
        duckdb_path: str,
        target_config: Dict[str, Any],
        source_query: str,
        target_table: str,
        mode: str = "full-refresh",
    ) -> Dict[str, Any]:
        """Load data from DuckDB into the target via Sling.

        Used after DuckDB Compute processes multi-source model SQL.

        Args:
            duckdb_path: Path to the DuckDB database file.
            target_config: profiles.yml output config for the target.
            source_query: SQL query to run in DuckDB for the result set.
            target_table: Fully-qualified target table name.
            mode: Sling mode for the target write.

        Returns:
            Dict with execution results.
        """
        self._check_available()

        src_url = f"duckdb://{duckdb_path}"
        tgt_url = map_to_sling_url(target_config)

        logger.info(
            f"DuckDB → Sling: duckdb → "
            f"{target_config.get('type', '?')} [{target_table}]"
        )

        replication = self._Replication(
            source=src_url,
            target=tgt_url,
            streams={
                f"custom_sql": self._ReplicationStream(
                    sql=source_query,
                    object=target_table,
                    mode=mode,
                ),
            },
        )
        replication.run()

        return {"status": "success", "target_table": target_table}

    def load_seed(
        self,
        csv_path: str,
        target_config: Dict[str, Any],
        target_table: str,
        mode: str = "truncate",
    ) -> Dict[str, Any]:
        """Load a CSV seed file into the target via Sling.

        Args:
            csv_path: Path to the CSV file.
            target_config: profiles.yml output config for the target.
            target_table: Fully-qualified target table name.
            mode: 'truncate' (default) or 'full-refresh' (drop + create).

        Returns:
            Dict with execution results.
        """
        self._check_available()

        tgt_url = map_to_sling_url(target_config)

        logger.info(f"Sling Seed: {csv_path} → {target_table} mode={mode}")

        replication = self._Replication(
            source=f"file://.",
            target=tgt_url,
            streams={
                f"file://{csv_path}": self._ReplicationStream(
                    object=target_table,
                    mode=mode,
                ),
            },
        )
        replication.run()

        return {"status": "success", "target_table": target_table, "csv": csv_path}

    def load_cross_target(
        self,
        source_config: Dict[str, Any],
        target_config: Dict[str, Any],
        source_table: str,
        target_table: str,
        mode: str = "full-refresh",
        target_options: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        """Move data from one target to another via Sling.

        Used for cross-target materialization (e.g., Snowflake → S3 bucket).

        Args:
            source_config: profiles.yml output config for the source target.
            target_config: profiles.yml output config for the destination target.
            source_table: Table to read from the source target.
            target_table: Object to write on the destination (table name or path).
            mode: Sling mode.
            target_options: Extra Sling target options (format, etc.).

        Returns:
            Dict with execution results.
        """
        self._check_available()

        src_url = map_to_sling_url(source_config)
        tgt_url = map_to_sling_url(target_config)

        stream_kwargs: Dict[str, Any] = {
            "object": target_table,
            "mode": mode,
        }
        if target_options:
            stream_kwargs["target_options"] = target_options

        logger.info(
            f"Sling Cross-Target: {source_config.get('type', '?')} → "
            f"{target_config.get('type', '?')} [{target_table}]"
        )

        replication = self._Replication(
            source=src_url,
            target=tgt_url,
            streams={
                source_table: self._ReplicationStream(**stream_kwargs),
            },
        )
        replication.run()

        return {"status": "success", "target_table": target_table}
