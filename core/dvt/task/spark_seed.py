# coding=utf-8
"""Spark-based seed runner for DVT.

This module provides a Spark-based implementation for loading CSV seed files.
DVT always uses Spark for seeding to provide:
- Better performance with large files via batch JDBC writes
- Cross-target seeding support (seed to any target in profiles.yml)
- Consistent federation architecture
- Column type overrides via seed config
- Native connector support for Snowflake/BigQuery/Redshift

Usage:
    dvt seed                          # Use default compute and target
    dvt seed --compute my_cluster     # Specify compute engine
    dvt seed --target prod            # Specify destination target
    dvt seed -c spark_local -t dev    # Both flags
"""
import sys
from pathlib import Path
from typing import Any, Dict, Optional

from dvt.artifacts.schemas.results import NodeStatus, RunStatus
from dvt.config import RuntimeConfig
from dvt.contracts.graph.manifest import Manifest
from dvt.contracts.graph.nodes import SeedNode
from dvt.events.types import LogSeedResult, LogStartLine
from dvt.task import group_lookup
from dvt.task.base import BaseRunner
from dbt_common.events.base_types import EventLevel
from dbt_common.events.functions import fire_event
from dbt_common.ui import green, red, yellow


def _log(msg: str) -> None:
    """Write message to stderr."""
    sys.stderr.write(msg + "\n")
    sys.stderr.flush()


def is_spark_available() -> bool:
    """Check if PySpark is available."""
    try:
        from pyspark.sql import SparkSession  # noqa: F401

        return True
    except ImportError:
        return False


class SparkSeedRunner(BaseRunner):
    """Spark-based seed runner for DVT.

    Uses Spark to load CSV files and write to target databases via JDBC.
    Supports --compute and --target CLI flags for flexible seeding.
    """

    def __init__(
        self,
        config: RuntimeConfig,
        adapter: Any,
        node: SeedNode,
        node_index: int,
        num_nodes: int,
    ):
        super().__init__(config, adapter, node, node_index, num_nodes)
        self.spark_manager = None
        self._compute_name = None
        self._target_name = None

    def get_node_representation(self) -> str:
        """Get the node representation for display."""
        display_quote_policy = {"database": False, "schema": False, "identifier": False}
        relation = self.adapter.Relation.create_from(
            self.config, self.node, quote_policy=display_quote_policy
        )
        # exclude the database from output if it's the default
        if self.node.database == self.config.credentials.database:
            relation = relation.include(database=False)
        return str(relation)

    def describe_node(self) -> str:
        return f"seed file {self.get_node_representation()}"

    def compile(self, manifest: Manifest) -> SeedNode:
        """Seeds don't need compilation - return node as-is."""
        return self.node

    def before_execute(self) -> None:
        fire_event(
            LogStartLine(
                description=self.describe_node(),
                index=self.node_index,
                total=self.num_nodes,
                node_info=self.node.node_info,
            )
        )

    def after_execute(self, result: Any) -> None:
        """Called after execute completes."""
        self.print_result_line(result)

    def execute(self, compiled_node: Any, manifest: Manifest) -> Any:
        """Execute seed using Spark.

        Resolution order for compute: CLI --compute > seed config > computes.yml default
        Resolution order for target: CLI --target > seed config > profiles.yml default

        Note: Spark session is initialized ONCE in SeedTask.run() and shared
        across all seed threads. This avoids race conditions from per-seed
        singleton resets.

        Returns:
            RunResult with execution status
        """
        from dvt.artifacts.schemas.run import RunResult
        from dvt.federation.spark_manager import SparkManager

        seed = self.node

        try:
            # Resolve compute engine (for logging purposes)
            self._compute_name = self._resolve_compute(seed)

            # Use the shared SparkManager instance (initialized by SeedTask.run())
            # Do NOT reset the singleton here - that causes race conditions
            self.spark_manager = SparkManager.get_instance()

            # Get existing Spark session (already created by SeedTask.run())
            spark = self.spark_manager.get_or_create_session()

            # Read CSV with Spark
            df = self._read_csv_with_spark(spark, seed)
            row_count = df.count()

            # Apply schema overrides from seed config
            df = self._apply_schema_overrides(df, seed)

            # Determine target for this seed
            self._target_name = self._resolve_target(seed)

            # Get connection config for target
            connection = self._get_connection_for_target(self._target_name)

            # Build table name
            table_name = self._get_full_table_name(seed, connection)

            # Write to target via JDBC
            self._write_with_jdbc(df, seed, connection, table_name)

            return RunResult(
                status=RunStatus.Success,
                timing=[],
                thread_id="",
                execution_time=0.0,
                adapter_response={},
                message=f"Loaded {row_count} rows via Spark → {self._target_name}",
                failures=None,
                node=seed,
            )

        except Exception as e:
            _log(red(f"❌ Spark seed failed: {e}"))
            return RunResult(
                status=RunStatus.Error,
                timing=[],
                thread_id="",
                execution_time=0.0,
                adapter_response={},
                message=str(e),
                failures=None,
                node=seed,
            )

    def _resolve_compute(self, seed: SeedNode) -> Optional[str]:
        """Resolve which compute engine to use.

        Priority: CLI --compute > seed config compute > computes.yml default
        """
        # Check CLI flag (args object has uppercase attribute names)
        cli_compute = getattr(self.config.args, "COMPUTE", None)
        if not cli_compute:
            cli_compute = getattr(self.config.args, "compute", None)
        if cli_compute:
            return cli_compute

        # Check seed-level config
        if hasattr(seed, "config") and seed.config:
            seed_compute = seed.config.get("compute")
            if seed_compute:
                return seed_compute

        # Return None to use default from computes.yml
        return None

    def _resolve_target(self, seed: SeedNode) -> str:
        """Resolve which target to seed to.

        Priority: CLI --target > seed config target > profiles.yml default
        """
        # Check CLI flag
        cli_target = getattr(self.config.args, "TARGET", None)
        if not cli_target:
            cli_target = getattr(self.config.args, "target", None)
        if cli_target:
            return cli_target

        # Check seed-level config
        if hasattr(seed, "config") and seed.config:
            seed_target = seed.config.get("target")
            if seed_target:
                return seed_target

        # Default from profile
        return self.config.target_name

    def _get_connection_for_target(self, target_name: str) -> Dict[str, Any]:
        """Get connection configuration for a target."""
        # Get the profile's target config
        if hasattr(self.config, "credentials"):
            creds = self.config.credentials
            return {
                "type": creds.type,
                "host": getattr(creds, "host", None),
                "port": getattr(creds, "port", None),
                "database": getattr(creds, "database", None),
                "schema": getattr(creds, "schema", None),
                "user": getattr(creds, "user", None),
                "password": getattr(creds, "password", None),
                "account": getattr(creds, "account", None),
            }
        return {}

    def _read_csv_with_spark(self, spark: Any, seed: SeedNode) -> Any:
        """Read CSV file using Spark.

        Handles:
        - Header detection
        - Type inference
        - Null value handling
        - Quote character handling
        """
        # Build CSV path
        csv_path = Path(seed.root_path) / seed.original_file_path

        _log(f"  📄 Reading CSV: {csv_path}")

        return spark.read.csv(
            str(csv_path),
            header=True,
            inferSchema=True,
            nullValue="",
            quote='"',
            escape='"',
            multiLine=True,
        )

    def _apply_schema_overrides(self, df: Any, seed: SeedNode) -> Any:
        """Apply column type overrides from seed config.

        Example seed config:
            seeds:
              my_seed:
                +column_types:
                  id: bigint
                  created_at: timestamp
        """
        if not hasattr(seed, "config"):
            return df

        column_types = seed.config.get("column_types", {})

        for col_name, col_type in column_types.items():
            if col_name in df.columns:
                df = df.withColumn(col_name, df[col_name].cast(col_type))

        return df

    def _get_full_table_name(self, seed: SeedNode, connection: Dict[str, Any]) -> str:
        """Get fully qualified table name for the seed."""
        schema = seed.schema
        alias = seed.alias or seed.name
        return f"{schema}.{alias}"

    def _write_with_jdbc(
        self,
        df: Any,
        seed: SeedNode,
        connection: Dict[str, Any],
        table_name: str,
    ) -> None:
        """Write DataFrame to target database using JDBC.

        Normal mode: Use overwrite with truncate=True (TRUNCATE instead of DROP)
        --full-refresh mode: DROP CASCADE first, then recreate table
        """
        from dvt.federation.spark_manager import SparkManager

        spark_manager = SparkManager.get_instance()

        # Build JDBC URL
        jdbc_url = spark_manager.get_jdbc_url(connection)

        # Get driver class
        adapter_type = connection.get("type", "")
        driver = spark_manager.get_jdbc_driver(adapter_type)

        # Check CLI --full-refresh flag
        full_refresh = getattr(self.config.args, "FULL_REFRESH", False)
        if not full_refresh:
            full_refresh = getattr(self.config.args, "full_refresh", False)

        # Get batch size
        batch_size = 10000
        if hasattr(seed, "config") and seed.config:
            batch_size = seed.config.get("batch_size", 10000) or 10000

        # Write to database - handle None values
        user = connection.get("user") or ""
        password = connection.get("password") or ""

        properties = {
            "user": user,
            "password": password,
            "driver": driver,
            "batchsize": str(batch_size),
        }

        if full_refresh:
            # Full refresh: DROP CASCADE + recreate table
            _log(f"  💾 Writing to {table_name} via JDBC (full-refresh: DROP CASCADE)...")
            self._drop_table_cascade(connection, table_name)
            # After DROP CASCADE, table doesn't exist - Spark's DROP will be a no-op
            df.write.jdbc(
                url=jdbc_url,
                table=table_name,
                mode="overwrite",
                properties=properties,
            )
        else:
            # Normal mode: Use truncate=True to TRUNCATE instead of DROP
            # This preserves dependent views
            _log(f"  💾 Writing to {table_name} via JDBC (truncate mode)...")
            properties["truncate"] = "true"
            df.write.jdbc(
                url=jdbc_url,
                table=table_name,
                mode="overwrite",
                properties=properties,
            )

        row_count = df.count()
        _log(green(f"  ✅ Loaded {row_count} rows"))

    def _execute_sql_native(
        self, connection: Dict[str, Any], sql: str
    ) -> bool:
        """Execute SQL using native Python driver. Returns True on success."""
        adapter_type = connection.get("type", "")

        if adapter_type == "postgres":
            return self._execute_sql_postgres(connection, sql)
        elif adapter_type == "snowflake":
            return self._execute_sql_snowflake(connection, sql)
        else:
            _log(f"    Native SQL exec not supported for {adapter_type}")
            return False

    def _execute_sql_postgres(self, connection: Dict[str, Any], sql: str) -> bool:
        """Execute SQL on Postgres using psycopg2."""
        try:
            import psycopg2
        except ImportError:
            _log("    psycopg2 not installed, skipping native SQL")
            return False

        conn = None
        try:
            conn = psycopg2.connect(
                host=connection.get("host") or "localhost",
                port=connection.get("port") or 5432,
                database=connection.get("database") or "postgres",
                user=connection.get("user") or "",
                password=connection.get("password") or "",
            )
            conn.autocommit = True
            cur = conn.cursor()
            cur.execute(sql)
            cur.close()
            return True
        except Exception as e:
            _log(f"    Postgres SQL failed: {str(e)[:60]}")
            return False
        finally:
            if conn:
                conn.close()

    def _execute_sql_snowflake(self, connection: Dict[str, Any], sql: str) -> bool:
        """Execute SQL on Snowflake."""
        try:
            import snowflake.connector
        except ImportError:
            _log("    snowflake-connector not installed, skipping native SQL")
            return False

        conn = None
        try:
            conn = snowflake.connector.connect(
                account=connection.get("account") or "",
                user=connection.get("user") or "",
                password=connection.get("password") or "",
                database=connection.get("database"),
                schema=connection.get("schema"),
            )
            cur = conn.cursor()
            cur.execute(sql)
            cur.close()
            return True
        except Exception as e:
            _log(f"    Snowflake SQL failed: {str(e)[:60]}")
            return False
        finally:
            if conn:
                conn.close()

    def _drop_table_cascade(
        self,
        connection: Dict[str, Any],
        table_name: str,
    ) -> bool:
        """Drop table with CASCADE (destroys dependent views). Returns True on success."""
        adapter_type = connection.get("type", "")

        # DROP CASCADE syntax varies by database
        if adapter_type in ("postgres", "redshift", "snowflake"):
            drop_sql = f"DROP TABLE IF EXISTS {table_name} CASCADE"
        else:
            # databricks, bigquery don't support CASCADE
            drop_sql = f"DROP TABLE IF EXISTS {table_name}"

        _log(f"    Executing: {drop_sql}")
        success = self._execute_sql_native(connection, drop_sql)
        if success:
            _log(f"    ✓ Table dropped with CASCADE")
        return success

    def print_result_line(self, result: Any) -> None:
        """Print result line for the seed."""
        model = result.node
        group = group_lookup.get(model.unique_id)
        level = EventLevel.ERROR if result.status == NodeStatus.Error else EventLevel.INFO
        fire_event(
            LogSeedResult(
                status=result.status,
                result_message=result.message,
                index=self.node_index,
                total=self.num_nodes,
                execution_time=result.execution_time,
                schema=self.node.schema,
                relation=model.alias,
                node_info=model.node_info,
                group=group,
            ),
            level=level,
        )
