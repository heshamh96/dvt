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


class _SparkRowPipe:
    """File-like object that streams Spark DataFrame rows as TSV.

    psycopg2's copy_expert() calls read() on this object repeatedly.
    Each read() returns a chunk of tab-separated rows.
    Uses toLocalIterator() so only one partition is in driver memory at a time.
    """

    def __init__(self, df: Any, columns: list, batch_rows: int = 10000):
        self.iterator = df.toLocalIterator()
        self.columns = columns
        self.batch_rows = batch_rows
        self.rows_written = 0
        self._buffer = b""
        self._exhausted = False

    def read(self, size: int = -1) -> bytes:
        """Read up to size bytes of TSV data."""
        if self._exhausted and not self._buffer:
            return b""

        # Determine whether to read all remaining data or a specific amount
        read_all = size is None or size < 0

        # Fill buffer until we have enough or iterator exhausted
        while (read_all or len(self._buffer) < size) and not self._exhausted:
            try:
                row = next(self.iterator)
                values = []
                for v in row:
                    if v is None:
                        values.append("\\N")
                    else:
                        str_val = str(v)
                        str_val = str_val.replace("\\", "\\\\")
                        str_val = str_val.replace("\t", "\\t")
                        str_val = str_val.replace("\n", "\\n")
                        str_val = str_val.replace("\r", "\\r")
                        values.append(str_val)
                self._buffer += ("\t".join(values) + "\n").encode("utf-8")
                self.rows_written += 1
            except StopIteration:
                self._exhausted = True
                break

        if not read_all and size > 0:
            result = self._buffer[:size]
            self._buffer = self._buffer[size:]
        else:
            result = self._buffer
            self._buffer = b""
        return result


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

        Tries streaming COPY FROM first (constant memory via toLocalIterator),
        then buffered COPY FROM, then falls back to Spark JDBC.

        Args:
            df: PySpark DataFrame to load
            config: Load configuration
            adapter: dbt adapter for DDL operations

        Returns:
            LoadResult with success status and metadata
        """
        # Try streaming COPY first (constant memory)
        try:
            return self._load_copy_streaming(df, config, adapter)
        except Exception as e:
            self._log(f"Streaming COPY failed ({e}), trying buffered COPY...")

        # Try buffered COPY (legacy, full dataset in memory)
        try:
            return self._load_copy(df, config, adapter)
        except Exception as e:
            self._log(f"COPY failed ({e}), falling back to JDBC...")

        # Fallback to JDBC
        return self._load_jdbc(df, config, adapter)

    def _load_copy_streaming(
        self,
        df: Any,
        config: LoadConfig,
        adapter: Optional[Any] = None,
    ) -> LoadResult:
        """Load via streaming COPY FROM using toLocalIterator.

        Uses _SparkRowPipe to stream Spark rows one partition at a time
        through psycopg2's copy_expert. Memory: O(partition_size) instead
        of O(dataset).

        Falls back to buffered COPY for small datasets where
        toLocalIterator overhead isn't worthwhile.
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

        # For small datasets, toLocalIterator has per-partition scheduling
        # overhead that isn't worth it. Fall back to buffered COPY.
        try:
            num_partitions = df.rdd.getNumPartitions()
            if num_partitions <= 1:
                raise ValueError("Single partition — buffered COPY is more efficient")
        except Exception as e:
            if "Single partition" in str(e):
                raise
            # If we can't check partitions, proceed with streaming

        # Get quoted table name
        if adapter:
            from dvt.federation.adapter_manager import get_quoted_table_name

            quoted_table = get_quoted_table_name(adapter, config.table_name)
        else:
            quoted_table = config.table_name

        # Build CREATE TABLE DDL from DataFrame schema
        from dvt.utils.identifiers import build_create_table_sql

        adapter_type = config.connection_config.get("type", "postgres")
        create_sql = build_create_table_sql(df, adapter_type, quoted_table)

        # Open a single psycopg2 connection for DDL + COPY
        from dvt.federation.auth.postgres import PostgresAuthHandler

        handler = PostgresAuthHandler()
        connect_kwargs = handler.get_native_connection_kwargs(config.connection_config)

        conn = None
        try:
            conn = psycopg2.connect(**connect_kwargs)
            conn.autocommit = False
            cursor = conn.cursor()

            # DDL: DROP + CREATE for overwrite mode
            if config.mode == "overwrite":
                self._log(f"Dropping {config.table_name}...")
                cursor.execute(f"DROP TABLE IF EXISTS {quoted_table} CASCADE")

            self._log(f"Creating table {config.table_name} via adapter DDL...")
            cursor.execute(create_sql)

            # Stream via _SparkRowPipe (toLocalIterator under the hood)
            columns = df.columns
            self._log("Streaming DataFrame via toLocalIterator for COPY...")
            pipe = _SparkRowPipe(df, columns, batch_rows=config.streaming_batch_size)

            columns_str = ", ".join(f'"{col}"' for col in columns)
            copy_sql = (
                f"COPY {quoted_table} ({columns_str}) "
                f"FROM STDIN WITH (FORMAT csv, DELIMITER E'\\t', NULL '\\N')"
            )
            cursor.copy_expert(copy_sql, pipe)

            conn.commit()
            cursor.close()

            row_count = pipe.rows_written
            elapsed = time.time() - start_time

            self._log(f"Loaded {row_count:,} rows via streaming COPY in {elapsed:.1f}s")

            return LoadResult(
                success=True,
                table_name=config.table_name,
                row_count=row_count,
                load_method="copy_streaming",
                elapsed_seconds=elapsed,
            )

        except Exception as e:
            if conn:
                conn.rollback()
            raise
        finally:
            if conn:
                conn.close()

    def _load_copy(
        self,
        df: Any,
        config: LoadConfig,
        adapter: Optional[Any] = None,
    ) -> LoadResult:
        """Load via PostgreSQL COPY FROM.

        All DDL and data loading happens on a single psycopg2 connection
        to avoid transaction visibility issues with the dbt adapter's
        connection management (which may rollback uncommitted DDL).

        Steps:
        1. DROP/TRUNCATE table (if overwrite mode)
        2. CREATE TABLE with properly quoted columns
        3. Stream data via COPY FROM STDIN
        4. COMMIT

        Args:
            df: PySpark DataFrame to load
            config: Load configuration
            adapter: dbt adapter (used only for table name quoting)

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

        # Get quoted table name
        if adapter:
            from dvt.federation.adapter_manager import get_quoted_table_name

            quoted_table = get_quoted_table_name(adapter, config.table_name)
        else:
            quoted_table = config.table_name

        # Build CREATE TABLE DDL from DataFrame schema
        from dvt.utils.identifiers import build_create_table_sql

        adapter_type = config.connection_config.get("type", "postgres")
        create_sql = build_create_table_sql(df, adapter_type, quoted_table)

        # Open a single psycopg2 connection for DDL + COPY
        from dvt.federation.auth.postgres import PostgresAuthHandler

        handler = PostgresAuthHandler()
        connect_kwargs = handler.get_native_connection_kwargs(config.connection_config)

        conn = None
        try:
            conn = psycopg2.connect(**connect_kwargs)
            conn.autocommit = False
            cursor = conn.cursor()

            # DDL: Always DROP + CREATE for overwrite mode.
            # We always re-create the table from the current DataFrame schema
            # rather than truncating, because:
            # 1. The old schema may have NOT NULL constraints from previous runs
            #    that don't match the current data (Spark nullable inference is
            #    unreliable for federated sources).
            # 2. Column types/names may have changed between runs.
            # 3. CREATE TABLE IF NOT EXISTS would be a no-op on an existing table.
            if config.mode == "overwrite":
                self._log(f"Dropping {config.table_name}...")
                cursor.execute(f"DROP TABLE IF EXISTS {quoted_table} CASCADE")

            # DDL: CREATE TABLE with properly quoted column names
            self._log(f"Creating table {config.table_name} via adapter DDL...")
            cursor.execute(create_sql)

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

            # COPY FROM STDIN
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
