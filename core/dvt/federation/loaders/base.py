"""
Federation loader — single JDBC + adapter loader for all targets.

Loads DataFrames into target databases using:
- Spark JDBC (df.write.jdbc()) for bulk INSERT with parallel writers
- dbt adapter for DDL (CREATE/DROP/TRUNCATE) with proper dialect quoting

This replaces the previous per-adapter loader hierarchy (PostgresLoader,
DatabricksLoader, SnowflakeLoader, etc.) which had COPY/pipe paths that
added complexity without significant benefit for federation workloads.

Usage:
    from dvt.federation.loaders import get_loader

    loader = get_loader(on_progress=print)
    result = loader.load(df, config, adapter=adapter)
"""

import time
from dataclasses import dataclass
from typing import Any, Callable, Dict, List, Optional


@dataclass
class LoadConfig:
    """Configuration for a single load operation."""

    table_name: str  # Fully qualified table name (schema.table or catalog.schema.table)
    mode: str = "overwrite"  # 'overwrite', 'append'
    truncate: bool = True  # Use TRUNCATE instead of DROP for overwrite
    full_refresh: bool = False  # --full-refresh: DROP + CREATE + INSERT
    connection_config: Optional[Dict[str, Any]] = None  # profiles.yml connection
    jdbc_config: Optional[Dict[str, Any]] = None  # jdbc_load settings from computes.yml
    # Phase 4 additions (placeholders):
    incremental_strategy: Optional[str] = None  # 'append', 'delete+insert', 'merge'
    unique_key: Optional[List[str]] = None  # For merge/delete+insert


@dataclass
class LoadResult:
    """Result of a load operation."""

    success: bool
    table_name: str
    row_count: int = 0
    error: Optional[str] = None
    load_method: str = "jdbc"  # 'jdbc', 'merge', 'delete+insert'
    elapsed_seconds: float = 0.0


class FederationLoader:
    """Single loader for all federation targets.

    Uses Spark JDBC for data transfer + dbt adapter for DDL.
    All adapters (Postgres, Databricks, Snowflake, BigQuery, etc.)
    use the same JDBC + adapter pattern.
    """

    def __init__(
        self,
        on_progress: Optional[Callable[[str], None]] = None,
    ):
        """Initialize loader.

        Args:
            on_progress: Optional callback for progress messages
        """
        self.on_progress = on_progress or (lambda msg: None)

    def _log(self, message: str) -> None:
        """Log a progress message."""
        self.on_progress(message)

    def load(
        self,
        df: Any,  # pyspark.sql.DataFrame
        config: LoadConfig,
        adapter: Optional[Any] = None,
    ) -> LoadResult:
        """Load DataFrame into target database.

        Dispatches to the appropriate load strategy:
        - Default: JDBC INSERT (append or overwrite with DDL via adapter)
        - merge: Temp table + MERGE SQL (upsert via unique_key)
        - delete+insert: Temp table + DELETE matching + INSERT

        Args:
            df: PySpark DataFrame to load
            config: Load configuration
            adapter: Optional dbt adapter for DDL operations

        Returns:
            LoadResult with success status and metadata
        """
        strategy = config.incremental_strategy

        if strategy == "merge" and config.unique_key and adapter:
            return self._load_merge(df, config, adapter)
        elif strategy == "delete+insert" and config.unique_key and adapter:
            return self._load_delete_insert(df, config, adapter)
        else:
            # Default: append (incremental), overwrite (table/full-refresh)
            return self._load_jdbc(df, config, adapter)

    # =========================================================================
    # DDL Operations via Adapter
    # =========================================================================

    def _commit(self, adapter: Any) -> None:
        """Commit the adapter connection.

        All adapter.execute() calls in this loader use auto_begin=True, which
        makes dbt properly track the transaction (transaction_open=True).
        This means adapter.connections.commit() works correctly on all adapters:

        - PostgreSQL: sends COMMIT, sets transaction_open=False
        - Databricks/Spark: no-op (auto-commit at DB level)
        - Snowflake/BigQuery: handled by adapter internals

        No fallbacks, no raw handle access. One path, works right.
        """
        adapter.connections.commit()

    def _execute_ddl(
        self,
        adapter: Any,
        config: LoadConfig,
    ) -> None:
        """Execute DDL operations via adapter.

        Handles table preparation (TRUNCATE or DROP+CREATE) using
        properly quoted identifiers from the adapter.

        Args:
            adapter: dbt adapter instance
            config: Load configuration
        """
        from dvt.federation.adapter_manager import get_quoted_table_name

        quoted_table = get_quoted_table_name(adapter, config.table_name)

        with adapter.connection_named("dvt_loader"):
            if config.full_refresh:
                # Full refresh: DROP + CREATE (Spark will create)
                self._log(f"Dropping {config.table_name}...")
                try:
                    adapter.execute(
                        f"DROP TABLE IF EXISTS {quoted_table} CASCADE",
                        auto_begin=True,
                    )
                except Exception:
                    # Try without CASCADE (some DBs don't support it)
                    try:
                        adapter.execute(
                            f"DROP TABLE IF EXISTS {quoted_table}",
                            auto_begin=True,
                        )
                    except Exception:
                        pass  # Table might not exist
            elif config.truncate:
                # Truncate: faster than DROP+CREATE, preserves structure
                self._log(f"Truncating {config.table_name}...")
                try:
                    adapter.execute(f"TRUNCATE TABLE {quoted_table}", auto_begin=True)
                except Exception:
                    # Table might not exist - will be created by Spark
                    pass
            # Commit DDL so it's visible to other connections (JDBC)
            self._commit(adapter)

    def _ensure_schema_exists(
        self,
        adapter: Any,
        config: LoadConfig,
    ) -> None:
        """Ensure the target schema exists.

        Args:
            adapter: dbt adapter instance
            config: Load configuration
        """
        from dvt.federation.adapter_manager import parse_table_name

        parts = parse_table_name(config.table_name)
        schema = parts.get("schema")

        if schema:
            with adapter.connection_named("dvt_loader"):
                try:
                    adapter.execute(
                        f"CREATE SCHEMA IF NOT EXISTS {schema}", auto_begin=True
                    )
                except Exception:
                    pass  # Schema might already exist or we might not have permissions
                self._commit(adapter)

    def _create_table_with_adapter(
        self,
        adapter: Any,
        df: Any,  # pyspark.sql.DataFrame
        config: LoadConfig,
    ) -> None:
        """Create target table via adapter DDL with properly quoted columns.

        Generates a CREATE TABLE IF NOT EXISTS statement using dialect-aware
        quoting for both the table name and all column names. This preserves
        original column names (including spaces, special characters) by
        wrapping them in dialect-appropriate quotes.

        Args:
            adapter: dbt adapter instance
            df: PySpark DataFrame whose schema defines the table structure
            config: Load configuration with table_name
        """
        from dvt.federation.adapter_manager import get_quoted_table_name
        from dvt.utils.identifiers import build_create_table_sql

        adapter_type = adapter.type()
        quoted_table = get_quoted_table_name(adapter, config.table_name)
        create_sql = build_create_table_sql(df, adapter_type, quoted_table)

        self._log(f"Creating table {config.table_name} via adapter DDL...")
        with adapter.connection_named("dvt_loader"):
            try:
                adapter.execute(create_sql, auto_begin=True)
            except Exception as e:
                # Table might already exist (IF NOT EXISTS not supported everywhere)
                self._log(f"Create table note: {e}")
            # Commit DDL so it's visible to other connections (JDBC)
            self._commit(adapter)

    # =========================================================================
    # Spark JDBC Load
    # =========================================================================

    def _load_jdbc(
        self,
        df: Any,  # pyspark.sql.DataFrame
        config: LoadConfig,
        adapter: Optional[Any] = None,
    ) -> LoadResult:
        """Load using Spark JDBC with parallel writes.

        If an adapter is provided, DDL is executed via adapter first,
        then data is loaded via Spark JDBC in append mode.

        If no adapter is provided, falls back to pure Spark JDBC
        which handles DDL internally.

        Args:
            df: PySpark DataFrame to load
            config: Load configuration
            adapter: Optional dbt adapter for DDL

        Returns:
            LoadResult with success status and metadata
        """
        start_time = time.time()

        try:
            if not config.connection_config:
                raise ValueError("connection_config required for JDBC load")

            from dvt.federation.auth import get_auth_handler
            from dvt.federation.spark_manager import SparkManager

            adapter_type = config.connection_config.get("type", "")

            spark_manager = SparkManager.get_instance()
            jdbc_url = spark_manager.get_jdbc_url(config.connection_config)
            jdbc_driver = spark_manager.get_jdbc_driver(adapter_type)

            if not jdbc_driver:
                raise ValueError(f"No JDBC driver for adapter: {adapter_type}")

            # Get auth handler and validate
            auth_handler = get_auth_handler(adapter_type)
            is_valid, error_msg = auth_handler.validate(config.connection_config)
            if not is_valid:
                raise ValueError(error_msg)

            # Get JDBC auth properties
            jdbc_props = auth_handler.get_jdbc_properties(config.connection_config)

            # Get JDBC load settings from config
            jdbc_settings = config.jdbc_config or {}
            num_partitions = jdbc_settings.get("num_partitions", 4)
            batch_size = jdbc_settings.get("batch_size", 10000)

            # Build JDBC properties
            properties = {
                **jdbc_props,
                "driver": jdbc_driver,
                "batchsize": str(batch_size),
            }

            # Determine mode and DDL handling
            if adapter and config.mode == "overwrite":
                # DDL via adapter (proper quoting), data via Spark JDBC append
                self._execute_ddl(adapter, config)
                # Create table with properly quoted column names via adapter
                self._create_table_with_adapter(adapter, df, config)
                write_mode = "append"  # Table cleared by DDL, just append
            elif adapter and config.mode == "append":
                # Incremental append: ensure table exists (CREATE IF NOT EXISTS),
                # then append data. No TRUNCATE or DROP.
                self._create_table_with_adapter(adapter, df, config)
                write_mode = "append"
            else:
                # Pure Spark JDBC mode (no adapter)
                write_mode = config.mode
                # Set truncate property for Spark to handle
                if (
                    config.mode == "overwrite"
                    and config.truncate
                    and not config.full_refresh
                ):
                    properties["truncate"] = "true"

            # Repartition for parallel writes
            if num_partitions > 1:
                df = df.repartition(num_partitions)
                self._log(f"Using {num_partitions} parallel JDBC writers")

            self._log(f"Loading {config.table_name} via Spark JDBC...")
            df.write.jdbc(
                url=jdbc_url,
                table=config.table_name,
                mode=write_mode,
                properties=properties,
            )

            row_count = df.count()
            elapsed = time.time() - start_time

            self._log(f"Loaded {row_count:,} rows via JDBC in {elapsed:.1f}s")

            return LoadResult(
                success=True,
                table_name=config.table_name,
                row_count=row_count,
                load_method="jdbc",
                elapsed_seconds=elapsed,
            )

        except Exception as e:
            elapsed = time.time() - start_time
            self._log(f"JDBC load failed: {e}")
            return LoadResult(
                success=False,
                table_name=config.table_name,
                error=str(e),
                elapsed_seconds=elapsed,
            )

    # =========================================================================
    # MERGE Load (Phase 4)
    # =========================================================================

    def _get_staging_table_name(self, config: LoadConfig) -> str:
        """Generate a staging table name for merge/delete+insert operations.

        Creates a temp table name in the same schema as the target table.
        Uses a deterministic name based on the target table to avoid collisions.

        Args:
            config: Load configuration with table_name

        Returns:
            Staging table name like 'schema._dvt_staging_my_model'
        """
        from dvt.federation.adapter_manager import parse_table_name

        parts = parse_table_name(config.table_name)
        schema = parts.get("schema") or ""
        identifier = parts.get("identifier") or "model"
        staging_id = f"_dvt_staging_{identifier}"
        if schema:
            return f"{schema}.{staging_id}"
        return staging_id

    def _build_unique_index_sql(
        self,
        adapter: Any,
        table_name: str,
        unique_key: List[str],
    ) -> str:
        """Build CREATE UNIQUE INDEX SQL for PostgreSQL ON CONFLICT support.

        PostgreSQL's INSERT ... ON CONFLICT requires an existing unique index
        or constraint. This builds the DDL; the caller should execute it in
        the same connection/transaction as the MERGE itself.

        Args:
            adapter: dbt adapter instance
            table_name: Target table name (may be schema-qualified)
            unique_key: List of column names forming the unique key

        Returns:
            CREATE UNIQUE INDEX IF NOT EXISTS SQL string
        """
        from dvt.federation.adapter_manager import (
            get_quoted_table_name,
            parse_table_name,
        )
        from dvt.utils.identifiers import quote_identifier

        adapter_type = adapter.type()
        parts = parse_table_name(table_name)
        identifier = parts.get("identifier") or "model"
        key_suffix = "_".join(unique_key)[:40]
        index_name = f"_dvt_uq_{identifier}_{key_suffix}"

        quoted_cols = ", ".join(
            quote_identifier(col, adapter_type) for col in unique_key
        )
        quoted_table = get_quoted_table_name(adapter, table_name)

        return (
            f"CREATE UNIQUE INDEX IF NOT EXISTS {index_name} "
            f"ON {quoted_table} ({quoted_cols})"
        )

    def _write_to_staging_table(
        self,
        df: Any,
        staging_table: str,
        config: LoadConfig,
    ) -> None:
        """Write DataFrame to a staging table in the target database via JDBC.

        Creates (or overwrites) a staging table that MERGE/DELETE+INSERT
        can reference. Both the target table and staging table must be in
        the same database for the MERGE SQL to work natively.

        Args:
            df: PySpark DataFrame to write
            staging_table: Fully qualified staging table name
            config: Load configuration (for connection + JDBC settings)
        """
        from dvt.federation.auth import get_auth_handler
        from dvt.federation.spark_manager import SparkManager

        adapter_type = config.connection_config.get("type", "")
        spark_manager = SparkManager.get_instance()
        jdbc_url = spark_manager.get_jdbc_url(config.connection_config)
        jdbc_driver = spark_manager.get_jdbc_driver(adapter_type)

        auth_handler = get_auth_handler(adapter_type)
        jdbc_props = auth_handler.get_jdbc_properties(config.connection_config)

        jdbc_settings = config.jdbc_config or {}
        num_partitions = jdbc_settings.get("num_partitions", 4)
        batch_size = jdbc_settings.get("batch_size", 10000)

        properties = {
            **jdbc_props,
            "driver": jdbc_driver,
            "batchsize": str(batch_size),
        }

        if num_partitions > 1:
            df = df.repartition(num_partitions)

        self._log(f"Writing to staging table {staging_table}...")
        df.write.jdbc(
            url=jdbc_url,
            table=staging_table,
            mode="overwrite",
            properties=properties,
        )

    def _drop_staging_table(self, adapter: Any, staging_table: str) -> None:
        """Drop the staging table after merge/delete+insert.

        Args:
            adapter: dbt adapter instance
            staging_table: Fully qualified staging table name
        """
        from dvt.federation.adapter_manager import get_quoted_table_name

        quoted = get_quoted_table_name(adapter, staging_table)
        with adapter.connection_named("dvt_loader"):
            try:
                adapter.execute(f"DROP TABLE IF EXISTS {quoted}", auto_begin=True)
            except Exception:
                pass  # Best-effort cleanup
            self._commit(adapter)

    def _load_merge(
        self,
        df: Any,
        config: LoadConfig,
        adapter: Any,
    ) -> LoadResult:
        """Load using MERGE via temp table pattern.

        1. Ensure target table exists (CREATE IF NOT EXISTS)
        2. Write result DataFrame to a staging table via Spark JDBC
        3. Execute dialect-specific MERGE SQL via the adapter
        4. Drop the staging table

        The MERGE executes entirely inside the target database — both
        the target table and staging table are in the same DB.

        Args:
            df: PySpark DataFrame to load
            config: Load configuration (must have unique_key)
            adapter: dbt adapter for MERGE SQL execution

        Returns:
            LoadResult with load_method='merge'
        """
        start_time = time.time()
        staging_table = self._get_staging_table_name(config)

        try:
            # 1. Ensure target table exists
            self._create_table_with_adapter(adapter, df, config)

            adapter_type = config.connection_config.get("type", "")

            # 2. Write to staging table
            self._write_to_staging_table(df, staging_table, config)

            # 3. Execute MERGE SQL (with unique index creation in same transaction for PG)
            columns = [f.name for f in df.schema.fields]
            merge_sql = self._build_merge_sql(
                adapter_type=adapter_type,
                adapter=adapter,
                target_table=config.table_name,
                staging_table=staging_table,
                unique_key=config.unique_key,
                columns=columns,
            )

            self._log(f"Executing MERGE into {config.table_name}...")
            with adapter.connection_named("dvt_loader"):
                # Ensure unique index exists for Postgres ON CONFLICT
                # Must be in the same connection as the MERGE to avoid
                # the index being lost when a separate connection closes.
                if adapter_type == "postgres" and config.unique_key:
                    index_sql = self._build_unique_index_sql(
                        adapter, config.table_name, config.unique_key
                    )
                    try:
                        adapter.execute(index_sql, auto_begin=True)
                    except Exception:
                        pass  # Index may already exist
                adapter.execute(merge_sql, auto_begin=True)
                self._commit(adapter)

            row_count = df.count()
            elapsed = time.time() - start_time
            self._log(f"Merged {row_count:,} rows in {elapsed:.1f}s")

            return LoadResult(
                success=True,
                table_name=config.table_name,
                row_count=row_count,
                load_method="merge",
                elapsed_seconds=elapsed,
            )

        except Exception as e:
            elapsed = time.time() - start_time
            self._log(f"MERGE load failed: {e}")
            return LoadResult(
                success=False,
                table_name=config.table_name,
                error=str(e),
                elapsed_seconds=elapsed,
            )
        finally:
            # Always clean up staging table
            try:
                self._drop_staging_table(adapter, staging_table)
            except Exception:
                pass

    def _build_merge_sql(
        self,
        adapter_type: str,
        adapter: Any,
        target_table: str,
        staging_table: str,
        unique_key: List[str],
        columns: List[str],
    ) -> str:
        """Generate dialect-specific MERGE SQL.

        Different databases have different MERGE syntax:
        - Postgres: INSERT ... ON CONFLICT ... DO UPDATE SET
        - MySQL: INSERT ... ON DUPLICATE KEY UPDATE
        - Redshift: DELETE + INSERT (no native MERGE)
        - Databricks/Snowflake/BigQuery/SQL Server: MERGE INTO ... USING ... ON

        Args:
            adapter_type: Target database adapter type
            adapter: dbt adapter for quoting
            target_table: Target table name
            staging_table: Staging table name
            unique_key: Columns for match condition
            columns: All column names in the DataFrame

        Returns:
            SQL string for the merge operation
        """
        from dvt.federation.adapter_manager import get_quoted_table_name
        from dvt.utils.identifiers import quote_identifier

        qt = get_quoted_table_name(adapter, target_table)
        qs = get_quoted_table_name(adapter, staging_table)

        # Quote column names and unique keys
        qcols = [quote_identifier(c, adapter_type) for c in columns]
        qkeys = [quote_identifier(k, adapter_type) for k in unique_key]
        non_key_cols = [c for c in columns if c not in unique_key]
        q_non_key = [quote_identifier(c, adapter_type) for c in non_key_cols]

        at = adapter_type.lower()

        if at == "postgres":
            # INSERT ... ON CONFLICT (...) DO UPDATE SET ...
            col_list = ", ".join(qcols)
            key_list = ", ".join(qkeys)
            select_list = ", ".join(qcols)
            update_set = (
                ", ".join(f"{c} = EXCLUDED.{c}" for c in q_non_key)
                if q_non_key
                else qkeys[0] + " = EXCLUDED." + qkeys[0]
            )
            return (
                f"INSERT INTO {qt} ({col_list}) "
                f"SELECT {select_list} FROM {qs} "
                f"ON CONFLICT ({key_list}) DO UPDATE SET {update_set}"
            )

        elif at == "mysql":
            # INSERT ... ON DUPLICATE KEY UPDATE ...
            col_list = ", ".join(qcols)
            select_list = ", ".join(qcols)
            update_set = (
                ", ".join(f"{c} = VALUES({c})" for c in q_non_key)
                if q_non_key
                else qkeys[0] + " = VALUES(" + qkeys[0] + ")"
            )
            return (
                f"INSERT INTO {qt} ({col_list}) "
                f"SELECT {select_list} FROM {qs} "
                f"ON DUPLICATE KEY UPDATE {update_set}"
            )

        elif at == "redshift":
            # Redshift has no native MERGE — use DELETE + INSERT
            on_clause = " AND ".join(f"{qt}.{k} = {qs}.{k}" for k in qkeys)
            col_list = ", ".join(qcols)
            return (
                f"DELETE FROM {qt} USING {qs} WHERE {on_clause};\n"
                f"INSERT INTO {qt} ({col_list}) SELECT {col_list} FROM {qs}"
            )

        else:
            # Standard MERGE INTO for Databricks, Snowflake, BigQuery, SQL Server
            on_clause = " AND ".join(f"target.{k} = stg.{k}" for k in qkeys)

            if q_non_key:
                update_set = ", ".join(f"target.{c} = stg.{c}" for c in q_non_key)
            else:
                # All columns are keys — just update the first key (no-op update)
                update_set = f"target.{qkeys[0]} = stg.{qkeys[0]}"

            col_list = ", ".join(qcols)
            val_list = ", ".join(f"stg.{c}" for c in qcols)

            return (
                f"MERGE INTO {qt} AS target "
                f"USING {qs} AS stg "
                f"ON {on_clause} "
                f"WHEN MATCHED THEN UPDATE SET {update_set} "
                f"WHEN NOT MATCHED THEN INSERT ({col_list}) VALUES ({val_list})"
            )

    # =========================================================================
    # DELETE+INSERT Load (Phase 4)
    # =========================================================================

    def _load_delete_insert(
        self,
        df: Any,
        config: LoadConfig,
        adapter: Any,
    ) -> LoadResult:
        """Load using DELETE+INSERT via temp table pattern.

        1. Ensure target table exists (CREATE IF NOT EXISTS)
        2. Write result DataFrame to a staging table via Spark JDBC
        3. DELETE matching rows from target (by unique_key)
        4. INSERT all staging rows into target
        5. Drop the staging table

        Args:
            df: PySpark DataFrame to load
            config: Load configuration (must have unique_key)
            adapter: dbt adapter for SQL execution

        Returns:
            LoadResult with load_method='delete+insert'
        """
        start_time = time.time()
        staging_table = self._get_staging_table_name(config)

        try:
            # 1. Ensure target table exists
            self._create_table_with_adapter(adapter, df, config)

            # 2. Write to staging table
            self._write_to_staging_table(df, staging_table, config)

            # 3. Execute DELETE + INSERT
            adapter_type = config.connection_config.get("type", "")
            columns = [f.name for f in df.schema.fields]
            delete_sql, insert_sql = self._build_delete_insert_sql(
                adapter_type=adapter_type,
                adapter=adapter,
                target_table=config.table_name,
                staging_table=staging_table,
                unique_key=config.unique_key,
                columns=columns,
            )

            self._log(f"Executing DELETE+INSERT into {config.table_name}...")
            with adapter.connection_named("dvt_loader"):
                adapter.execute(delete_sql, auto_begin=True)
                adapter.execute(insert_sql, auto_begin=True)
                self._commit(adapter)

            row_count = df.count()
            elapsed = time.time() - start_time
            self._log(f"Delete+inserted {row_count:,} rows in {elapsed:.1f}s")

            return LoadResult(
                success=True,
                table_name=config.table_name,
                row_count=row_count,
                load_method="delete+insert",
                elapsed_seconds=elapsed,
            )

        except Exception as e:
            elapsed = time.time() - start_time
            self._log(f"DELETE+INSERT load failed: {e}")
            return LoadResult(
                success=False,
                table_name=config.table_name,
                error=str(e),
                elapsed_seconds=elapsed,
            )
        finally:
            # Always clean up staging table
            try:
                self._drop_staging_table(adapter, staging_table)
            except Exception:
                pass

    def _build_delete_insert_sql(
        self,
        adapter_type: str,
        adapter: Any,
        target_table: str,
        staging_table: str,
        unique_key: List[str],
        columns: List[str],
    ) -> tuple:
        """Generate DELETE + INSERT SQL for delete+insert strategy.

        Args:
            adapter_type: Target database adapter type
            adapter: dbt adapter for quoting
            target_table: Target table name
            staging_table: Staging table name
            unique_key: Columns for match condition
            columns: All column names

        Returns:
            Tuple of (delete_sql, insert_sql)
        """
        from dvt.federation.adapter_manager import get_quoted_table_name
        from dvt.utils.identifiers import quote_identifier

        qt = get_quoted_table_name(adapter, target_table)
        qs = get_quoted_table_name(adapter, staging_table)

        qkeys = [quote_identifier(k, adapter_type) for k in unique_key]
        qcols = [quote_identifier(c, adapter_type) for c in columns]

        at = adapter_type.lower()

        if at in ("postgres", "redshift"):
            # DELETE ... USING ... WHERE
            on_clause = " AND ".join(f"{qt}.{k} = {qs}.{k}" for k in qkeys)
            delete_sql = f"DELETE FROM {qt} USING {qs} WHERE {on_clause}"
        else:
            # Standard: DELETE WHERE unique_key IN (SELECT ... FROM staging)
            if len(qkeys) == 1:
                delete_sql = (
                    f"DELETE FROM {qt} WHERE {qkeys[0]} IN "
                    f"(SELECT {qkeys[0]} FROM {qs})"
                )
            else:
                # Multi-column key: use EXISTS with correlated subquery
                corr_clause = " AND ".join(f"{qt}.{k} = stg.{k}" for k in qkeys)
                delete_sql = (
                    f"DELETE FROM {qt} WHERE EXISTS "
                    f"(SELECT 1 FROM {qs} AS stg WHERE {corr_clause})"
                )

        col_list = ", ".join(qcols)
        insert_sql = f"INSERT INTO {qt} ({col_list}) SELECT {col_list} FROM {qs}"

        return delete_sql, insert_sql


# Backward compatibility alias
BaseLoader = FederationLoader
