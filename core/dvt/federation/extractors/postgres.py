"""
PostgreSQL extractor for EL layer.

Extraction priority:
1. Pipe-based: psql COPY TO STDOUT | PyArrow streaming (if psql on PATH)
2. In-process streaming COPY: psycopg2 copy_expert + PyArrow batch reader
3. In-process buffered COPY: psycopg2 copy_expert + in-memory buffer
4. Spark JDBC: parallel reads (default fallback)
"""

import os
import tempfile
import time
from io import BytesIO, StringIO
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional

try:
    import pyarrow as pa
    import pyarrow.csv as pa_csv
    import pyarrow.parquet as pq

    PYARROW_AVAILABLE = True
except ImportError:
    PYARROW_AVAILABLE = False

from dvt.federation.extractors.base import (
    BaseExtractor,
    ExtractionConfig,
    ExtractionResult,
)


class PostgresExtractor(BaseExtractor):
    """PostgreSQL-specific extractor using COPY for efficiency.

    Extraction priority:
    1. PostgreSQL COPY TO STDOUT (fast, single-threaded)
    2. Spark JDBC (parallel reads, requires connection_config)

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
        # Note: redshift has its own extractor with native S3 UNLOAD
    ]

    cli_tool = "psql"

    def _get_connection(self, config: ExtractionConfig = None) -> Any:
        """Get or create a database connection.

        If self.connection is None but connection_config is available
        (either from config or from the extractor's connection_config),
        creates a new connection using psycopg2.

        Args:
            config: Optional extraction config with connection_config

        Returns:
            Database connection
        """
        if self.connection is not None:
            return self.connection

        # Return cached lazy connection if available
        if self._lazy_connection is not None:
            return self._lazy_connection

        # Try to get connection_config from config or instance
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
            import psycopg2
        except ImportError:
            raise ImportError(
                "psycopg2 is required for PostgreSQL extraction. "
                "Install with: pip install psycopg2-binary"
            )

        self._lazy_connection = psycopg2.connect(
            host=conn_config.get("host", "localhost"),
            port=conn_config.get("port", 5432),
            database=conn_config.get("database", "postgres"),
            user=conn_config.get("user", "postgres"),
            password=conn_config.get("password", ""),
        )
        return self._lazy_connection

    def _build_extraction_command(self, config: ExtractionConfig) -> List[str]:
        """Build psql COPY TO STDOUT command."""
        conn_config = config.connection_config or self.connection_config or {}
        query = self.build_export_query(config)
        return [
            "psql",
            "-h",
            conn_config.get("host", "localhost"),
            "-p",
            str(conn_config.get("port", 5432)),
            "-U",
            conn_config.get("user", "postgres"),
            "-d",
            conn_config.get("database", "postgres"),
            "-c",
            f"COPY ({query}) TO STDOUT WITH (FORMAT csv, HEADER)",
            "--no-psqlrc",
            "--quiet",
        ]

    def _build_extraction_env(self, config: ExtractionConfig) -> Dict[str, str]:
        """Build env with PGPASSWORD for psql subprocess."""
        conn_config = config.connection_config or self.connection_config or {}
        env = os.environ.copy()
        password = conn_config.get("password", "")
        if password:
            env["PGPASSWORD"] = str(password)
        return env

    def extract(
        self,
        config: ExtractionConfig,
        output_path: Path,
    ) -> ExtractionResult:
        """Extract data from PostgreSQL to Parquet.

        Tries pipe (psql) first, then streaming COPY, buffered COPY,
        then falls back to Spark JDBC.
        """
        # Try pipe extraction first (psql + PyArrow streaming, ~64KB memory)
        if self._has_cli_tool():
            try:
                return self._extract_via_pipe(config, output_path)
            except Exception as e:
                self._log(f"Pipe extraction failed ({e}), trying streaming COPY...")

        # Try streaming COPY (constant memory via OS page cache)
        try:
            return self._extract_copy_streaming(config, output_path)
        except Exception as e:
            self._log(f"Streaming COPY failed ({e}), trying buffered COPY...")

        # Try buffered COPY (legacy, full dataset in memory)
        try:
            return self._extract_copy(config, output_path)
        except Exception as e:
            self._log(f"COPY failed ({e}), falling back to Spark JDBC...")

        # Fallback to Spark JDBC (parallel reads)
        return self._extract_jdbc(config, output_path)

    def _extract_copy_streaming(
        self,
        config: ExtractionConfig,
        output_path: Path,
    ) -> ExtractionResult:
        """Extract using PostgreSQL COPY TO STDOUT with streaming PyArrow.

        Writes COPY output to a temp file (OS page cache handles buffering),
        then streams through PyArrow CSV reader in batches to Parquet.
        Memory: O(batch_size) instead of O(dataset).
        """
        start_time = time.time()

        if not PYARROW_AVAILABLE:
            raise ImportError("pyarrow required for Parquet. Run 'dvt sync'.")

        import pyarrow.csv as pa_csv
        import pyarrow.parquet as pq

        conn = self._get_connection(config)
        query = self.build_export_query(config)
        copy_query = f"COPY ({query}) TO STDOUT WITH (FORMAT CSV, HEADER)"

        with tempfile.NamedTemporaryFile(mode="w+b", suffix=".csv") as tmp:
            # Phase A: PostgreSQL COPY -> temp file (OS page cache handles this)
            cursor = conn.cursor()
            cursor.copy_expert(copy_query, tmp)
            tmp.flush()
            tmp.seek(0)

            # Phase B: Stream-read CSV in batches -> write Parquet incrementally
            read_options = pa_csv.ReadOptions(
                block_size=config.batch_size * 512  # ~512 bytes/row estimate
            )
            streaming_reader = pa_csv.open_csv(tmp, read_options=read_options)

            # Ensure output directory exists
            output_path.parent.mkdir(parents=True, exist_ok=True)

            writer = None
            row_count = 0
            try:
                for batch in streaming_reader:
                    if writer is None:
                        writer = pq.ParquetWriter(
                            str(output_path),
                            batch.schema,
                            compression="zstd",
                        )
                    writer.write_batch(batch)
                    row_count += batch.num_rows
            finally:
                if writer:
                    writer.close()

            cursor.close()

        elapsed = time.time() - start_time
        self._log(
            f"Extracted {row_count:,} rows from {config.source_name} "
            f"via streaming COPY in {elapsed:.1f}s"
        )

        return ExtractionResult(
            success=True,
            source_name=config.source_name,
            row_count=row_count,
            output_path=output_path,
            extraction_method="copy_streaming",
            elapsed_seconds=elapsed,
        )

    def _extract_copy(
        self,
        config: ExtractionConfig,
        output_path: Path,
    ) -> ExtractionResult:
        """Extract using PostgreSQL COPY TO STDOUT (buffered).

        Uses psycopg2's copy_expert with in-memory StringIO buffer.
        Legacy path — kept as fallback for streaming COPY.
        """
        start_time = time.time()

        # Get or create connection
        conn = self._get_connection(config)

        # Build the query
        query = self.build_export_query(config)
        copy_query = f"COPY ({query}) TO STDOUT WITH (FORMAT CSV, HEADER)"

        # Use copy_expert for streaming
        buffer = StringIO()
        cursor = conn.cursor()
        cursor.copy_expert(copy_query, buffer)

        buffer.seek(0)

        if not PYARROW_AVAILABLE:
            raise ImportError("pyarrow required for Parquet. Run 'dvt sync'.")

        # Import here since we checked PYARROW_AVAILABLE
        import pyarrow.csv as pa_csv
        import pyarrow.parquet as pq

        # Convert CSV to Parquet via PyArrow
        table = pa_csv.read_csv(BytesIO(buffer.getvalue().encode()))

        # Ensure output directory exists
        output_path.parent.mkdir(parents=True, exist_ok=True)
        pq.write_table(table, output_path, compression="zstd")

        row_count = table.num_rows
        elapsed = time.time() - start_time

        self._log(
            f"Extracted {row_count:,} rows from {config.source_name} "
            f"via COPY in {elapsed:.1f}s"
        )

        return ExtractionResult(
            success=True,
            source_name=config.source_name,
            row_count=row_count,
            output_path=output_path,
            extraction_method="copy",
            elapsed_seconds=elapsed,
        )

    def extract_hashes(
        self,
        config: ExtractionConfig,
    ) -> Dict[str, str]:
        """Extract row hashes using PostgreSQL MD5 function."""
        if not config.pk_columns:
            raise ValueError("pk_columns required for hash extraction")

        # Build PostgreSQL-specific hash query
        # PostgreSQL uses MD5() and CONCAT_WS()
        pk_expr = (
            config.pk_columns[0]
            if len(config.pk_columns) == 1
            else f"CONCAT_WS('|', {', '.join(config.pk_columns)})"
        )

        # Get columns if not specified
        if config.columns:
            cols = config.columns
        else:
            col_info = self.get_columns(config.schema, config.table)
            cols = [c["name"] for c in col_info]

        # Build hash expression - cast all columns to text
        col_exprs = [f"COALESCE({c}::text, '')" for c in cols]
        hash_expr = f"MD5(CONCAT_WS('|', {', '.join(col_exprs)}))"

        query = f"""
            SELECT
                ({pk_expr})::text as _pk,
                {hash_expr} as _hash
            FROM {config.schema}.{config.table}
        """

        if config.predicates:
            where_clause = " AND ".join(config.predicates)
            query += f" WHERE {where_clause}"

        conn = self._get_connection(config)
        cursor = conn.cursor()
        cursor.execute(query)

        hashes = {}
        while True:
            batch = cursor.fetchmany(config.batch_size)
            if not batch:
                break
            for row in batch:
                hashes[row[0]] = row[1]

        cursor.close()
        return hashes

    def get_row_count(
        self,
        schema: str,
        table: str,
        predicates: Optional[List[str]] = None,
    ) -> int:
        """Get row count using COUNT(*)."""
        query = f"SELECT COUNT(*) FROM {schema}.{table}"
        if predicates:
            query += f" WHERE {' AND '.join(predicates)}"

        conn = self._get_connection()
        cursor = conn.cursor()
        cursor.execute(query)
        count = cursor.fetchone()[0]
        cursor.close()
        return count

    def get_columns(
        self,
        schema: str,
        table: str,
    ) -> List[Dict[str, str]]:
        """Get column metadata from information_schema."""
        query = """
            SELECT column_name, data_type
            FROM information_schema.columns
            WHERE table_schema = %s AND table_name = %s
            ORDER BY ordinal_position
        """
        conn = self._get_connection()
        cursor = conn.cursor()
        cursor.execute(query, (schema, table))

        columns = []
        for row in cursor.fetchall():
            columns.append({"name": row[0], "type": row[1]})

        cursor.close()
        return columns

    def detect_primary_key(
        self,
        schema: str,
        table: str,
    ) -> List[str]:
        """Detect primary key from pg_index."""
        query = """
            SELECT a.attname
            FROM pg_index i
            JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey)
            WHERE i.indrelid = %s::regclass
            AND i.indisprimary
            ORDER BY array_position(i.indkey, a.attnum)
        """
        conn = self._get_connection()
        cursor = conn.cursor()
        try:
            cursor.execute(query, (f"{schema}.{table}",))
            pk_cols = [row[0] for row in cursor.fetchall()]
        except Exception:
            pk_cols = []
        finally:
            cursor.close()

        # If no primary key found, try to find a unique index
        if not pk_cols:
            pk_cols = self._detect_unique_index(schema, table)

        return pk_cols

    def _detect_unique_index(
        self,
        schema: str,
        table: str,
    ) -> List[str]:
        """Fallback: detect unique index columns."""
        query = """
            SELECT a.attname
            FROM pg_index i
            JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey)
            WHERE i.indrelid = %s::regclass
            AND i.indisunique
            AND NOT i.indisprimary
            ORDER BY array_position(i.indkey, a.attnum)
            LIMIT 10
        """
        conn = self._get_connection()
        cursor = conn.cursor()
        try:
            cursor.execute(query, (f"{schema}.{table}",))
            return [row[0] for row in cursor.fetchall()]
        except Exception:
            return []
        finally:
            cursor.close()
