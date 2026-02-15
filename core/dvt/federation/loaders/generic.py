"""
Generic loader with optional pipe-based loading for on-prem databases.

Load priority:
1. Pipe-based: Spark result -> temp Parquet -> PyArrow batch -> CLI stdin
   (when cli_tool_for_adapter detects a known CLI tool)
2. Spark JDBC: parallel writes (default fallback)

Used as the fallback when no specialized loader is available for an adapter type.
All adapters can use JDBC through Spark, so this provides universal compatibility.
"""

import os
import shutil
import time
from pathlib import Path
from typing import Any, Dict, List, Optional

from dvt.federation.loaders.base import BaseLoader, LoadConfig, LoadResult

# Map adapter types to their CLI tools and load commands
_CLI_TOOL_MAP: Dict[str, str] = {
    "mysql": "mysql",
    "mariadb": "mysql",
    "tidb": "mysql",
    "singlestore": "mysql",
    "sqlserver": "bcp",
    "synapse": "bcp",
    "fabric": "bcp",
    "clickhouse": "clickhouse-client",
}


class GenericLoader(BaseLoader):
    """Generic loader with pipe optimization for known on-prem databases.

    Used as fallback for adapters without specialized bulk load support.
    When a CLI tool is detected for the adapter type, uses pipe-based loading
    for constant-memory data transfer. Otherwise falls back to Spark JDBC.
    """

    adapter_types = []  # Fallback - handles any adapter not in registry

    def _detect_cli_tool(self, config: LoadConfig) -> Optional[str]:
        """Detect CLI tool for this adapter type from config."""
        adapter_type = (config.connection_config or {}).get("type", "")
        tool = _CLI_TOOL_MAP.get(adapter_type)
        if tool and shutil.which(tool):
            return tool
        return None

    def _build_load_command(self, config: LoadConfig) -> List[str]:
        """Build CLI load command based on adapter type."""
        conn_config = config.connection_config or {}
        adapter_type = conn_config.get("type", "")

        if adapter_type in ("mysql", "mariadb", "tidb", "singlestore"):
            return self._build_mysql_load_command(config)
        elif adapter_type in ("sqlserver", "synapse", "fabric"):
            return self._build_sqlserver_load_command(config)
        elif adapter_type == "clickhouse":
            return self._build_clickhouse_load_command(config)
        else:
            raise NotImplementedError(
                f"No pipe load command for adapter: {adapter_type}"
            )

    def _build_load_env(self, config: LoadConfig) -> Dict[str, str]:
        """Build environment variables based on adapter type."""
        conn_config = config.connection_config or {}
        adapter_type = conn_config.get("type", "")
        env = os.environ.copy()

        if adapter_type in ("mysql", "mariadb", "tidb", "singlestore"):
            password = conn_config.get("password", "")
            if password:
                env["MYSQL_PWD"] = str(password)

        return env

    def _build_mysql_load_command(self, config: LoadConfig) -> List[str]:
        """Build mysql LOAD DATA LOCAL command."""
        conn_config = config.connection_config or {}
        return [
            "mysql",
            "-h",
            conn_config.get("host", "localhost"),
            "-P",
            str(conn_config.get("port", 3306)),
            "-u",
            conn_config.get("user", "root"),
            conn_config.get("database", ""),
            "--local-infile=1",
            "-e",
            (
                f"LOAD DATA LOCAL INFILE '/dev/stdin' INTO TABLE {config.table_name} "
                f"FIELDS TERMINATED BY ',' ENCLOSED BY '\"' "
                f"LINES TERMINATED BY '\\n' IGNORE 1 LINES"
            ),
        ]

    def _build_sqlserver_load_command(self, config: LoadConfig) -> List[str]:
        """Build bcp IN command."""
        conn_config = config.connection_config or {}
        server = conn_config.get("host", "localhost")
        port = conn_config.get("port", 1433)
        return [
            "bcp",
            config.table_name,
            "in",
            "/dev/stdin",
            "-S",
            f"{server},{port}",
            "-U",
            conn_config.get("user", ""),
            "-P",
            conn_config.get("password", ""),
            "-d",
            conn_config.get("database", ""),
            "-c",
            "-t",
            ",",
            "-C",
            "65001",  # UTF-8
        ]

    def _build_clickhouse_load_command(self, config: LoadConfig) -> List[str]:
        """Build clickhouse-client INSERT command."""
        conn_config = config.connection_config or {}
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
            f"INSERT INTO {config.table_name} FORMAT CSVWithNames",
        ]
        password = conn_config.get("password", "")
        if password:
            cmd.extend(["--password", str(password)])
        return cmd

    def _load_pipe(
        self,
        df: Any,
        config: LoadConfig,
        adapter: Any = None,
        tool_name: str = "",
    ) -> LoadResult:
        """Load via pipe: Spark result -> temp Parquet -> PyArrow -> CLI stdin.

        DDL contract (via adapter):
        - dvt run (default):         TRUNCATE + INSERT (preserves table structure)
        - dvt run --full-refresh:    DROP + CREATE + INSERT (rebuilds structure)

        Steps:
        1. DDL via adapter (TRUNCATE or DROP+CREATE per contract)
        2. CREATE TABLE IF NOT EXISTS via adapter
        3. Write Spark DataFrame to temp Parquet
        4. Stream Parquet -> CSV via PyArrow batches -> CLI stdin pipe
        5. Clean up temp Parquet

        Memory: ~1-10MB (PyArrow batch + CSV buffer).
        """
        start_time = time.time()

        # Temporarily set cli_tool for _load_via_pipe
        original_cli_tool = self.cli_tool
        self.cli_tool = tool_name

        # DDL via adapter — respects full_refresh vs truncate contract
        # (MySQL/ClickHouse: DDL auto-commits, always visible to subprocess)
        # (SQL Server: committed DDL visible to subprocess)
        if adapter:
            self._execute_ddl(adapter, config)
            self._create_table_with_adapter(adapter, df, config)

        # Write Spark result to temp Parquet
        temp_parquet = str(
            Path(os.environ.get("DVT_STAGING_DIR", "/tmp"))
            / f"_dvt_pipe_load_{config.table_name.replace('.', '_')}.parquet"
        )
        self._log(
            f"Writing Spark result to temp Parquet for pipe load ({tool_name})..."
        )
        df.write.mode("overwrite").option("compression", "zstd").parquet(temp_parquet)

        try:
            row_count = self._load_via_pipe(temp_parquet, config)
        finally:
            self.cli_tool = original_cli_tool
            shutil.rmtree(temp_parquet, ignore_errors=True)

        elapsed = time.time() - start_time
        self._log(f"Loaded {row_count:,} rows via pipe ({tool_name}) in {elapsed:.1f}s")

        return LoadResult(
            success=True,
            table_name=config.table_name,
            row_count=row_count,
            load_method="pipe",
            elapsed_seconds=elapsed,
        )

    def load(
        self,
        df: Any,  # pyspark.sql.DataFrame
        config: LoadConfig,
        adapter: Any = None,
    ) -> LoadResult:
        """Load using pipe (if CLI tool available) or Spark JDBC.

        Args:
            df: PySpark DataFrame to load
            config: Load configuration
            adapter: Optional dbt adapter for DDL operations

        Returns:
            LoadResult with success status and metadata
        """
        # Try pipe load for known on-prem adapters
        tool = self._detect_cli_tool(config)
        if tool:
            try:
                return self._load_pipe(df, config, adapter, tool_name=tool)
            except Exception as e:
                self._log(f"Pipe load failed ({e}), falling back to JDBC...")

        return self._load_jdbc(df, config, adapter)
