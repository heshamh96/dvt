"""
PostgreSQL loader with COPY FROM optimization.

Load priority:
1. PostgreSQL COPY FROM - fast streaming load (no staging needed)
2. Spark JDBC with adapter DDL - fallback

COPY FROM is significantly faster than JDBC INSERT for large datasets
because it streams data directly to the database without per-row overhead.
"""

import time
from io import StringIO
from typing import Any, Optional

from dvt.federation.loaders.base import BaseLoader, LoadConfig, LoadResult


class PostgresLoader(BaseLoader):
    """PostgreSQL loader using COPY FROM.

    Uses psycopg2's copy_expert() for efficient bulk loading.
    No staging bucket required - streams data directly to database.

    Also works for PostgreSQL-compatible databases:
    - Greenplum, Materialize, RisingWave, CrateDB, AlloyDB, TimescaleDB
    """

    adapter_types = [
        "postgres",
        "greenplum",
        "materialize",
        "risingwave",
        "cratedb",
        "alloydb",
        "timescaledb",
    ]

    def load(
        self,
        df: Any,
        config: LoadConfig,
        adapter: Optional[Any] = None,
    ) -> LoadResult:
        """Load DataFrame to PostgreSQL.

        Tries COPY FROM first (faster), falls back to Spark JDBC.

        Args:
            df: PySpark DataFrame to load
            config: Load configuration
            adapter: dbt adapter for DDL operations

        Returns:
            LoadResult with success status and metadata
        """
        try:
            return self._load_copy(df, config, adapter)
        except Exception as e:
            self._log(f"COPY failed ({e}), falling back to JDBC...")
            return self._load_jdbc(df, config, adapter)

    def _load_copy(
        self,
        df: Any,
        config: LoadConfig,
        adapter: Optional[Any] = None,
    ) -> LoadResult:
        """Load via PostgreSQL COPY FROM.

        Steps:
        1. Execute DDL via adapter (TRUNCATE or DROP)
        2. Create table via adapter DDL with properly quoted columns
        3. Stream data via COPY FROM STDIN

        Args:
            df: PySpark DataFrame to load
            config: Load configuration
            adapter: dbt adapter for DDL operations

        Returns:
            LoadResult with success status and metadata
        """
        start_time = time.time()

        try:
            import psycopg2
        except ImportError:
            raise ImportError(
                "psycopg2 required for PostgreSQL COPY. "
                "Install with: pip install psycopg2-binary"
            )

        if not config.connection_config:
            raise ValueError("connection_config required for COPY")

        # Execute DDL via adapter if provided
        if adapter and config.mode == "overwrite":
            self._execute_ddl(adapter, config)

        # Create table with properly quoted column names via adapter DDL
        if adapter:
            self._create_table_with_adapter(adapter, df, config)

        # Get quoted table name for COPY SQL
        if adapter:
            from dvt.federation.adapter_manager import get_quoted_table_name

            quoted_table = get_quoted_table_name(adapter, config.table_name)
        else:
            quoted_table = config.table_name

        # Now COPY data into the existing table
        from dvt.federation.auth.postgres import PostgresAuthHandler

        handler = PostgresAuthHandler()
        connect_kwargs = handler.get_native_connection_kwargs(config.connection_config)

        conn = None
        try:
            conn = psycopg2.connect(**connect_kwargs)
            conn.autocommit = False
            cursor = conn.cursor()

            # Collect DataFrame to driver
            self._log("Collecting DataFrame for COPY...")
            rows = df.collect()
            columns = df.columns

            if not rows:
                self._log("No rows to load")
                conn.commit()
                return LoadResult(
                    success=True,
                    table_name=config.table_name,
                    row_count=0,
                    load_method="copy",
                    elapsed_seconds=time.time() - start_time,
                )

            # Build CSV buffer with tab-separated values
            buffer = StringIO()
            for row in rows:
                values = []
                for v in row:
                    if v is None:
                        values.append("\\N")  # PostgreSQL NULL representation
                    else:
                        # Escape special characters
                        str_val = str(v)
                        str_val = str_val.replace("\\", "\\\\")
                        str_val = str_val.replace("\t", "\\t")
                        str_val = str_val.replace("\n", "\\n")
                        str_val = str_val.replace("\r", "\\r")
                        values.append(str_val)
                buffer.write("\t".join(values) + "\n")
            buffer.seek(0)

            # Use COPY FROM STDIN
            self._log(f"Loading {config.table_name} via COPY FROM...")
            columns_str = ", ".join(f'"{col}"' for col in columns)
            copy_sql = (
                f"COPY {quoted_table} ({columns_str}) "
                f"FROM STDIN WITH (FORMAT csv, DELIMITER E'\\t', NULL '\\N')"
            )
            cursor.copy_expert(copy_sql, buffer)

            conn.commit()
            cursor.close()

            row_count = len(rows)
            elapsed = time.time() - start_time

            self._log(f"Loaded {row_count:,} rows via COPY in {elapsed:.1f}s")

            return LoadResult(
                success=True,
                table_name=config.table_name,
                row_count=row_count,
                load_method="copy",
                elapsed_seconds=elapsed,
            )

        except Exception as e:
            if conn:
                conn.rollback()
            raise
        finally:
            if conn:
                conn.close()
