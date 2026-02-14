# coding=utf-8
"""Unit tests for streaming EL pipeline enhancements.

Tests:
1. _SparkRowPipe: TSV streaming from Spark DataFrame iterator
2. fetchmany-based extract_hashes: batched hash extraction
3. Streaming PostgresExtractor: tempfile + PyArrow CSV streaming
4. Streaming DatabricksExtractor: fetchmany + PyArrow ParquetWriter
5. Fallback chains: streaming -> buffered -> JDBC
"""

from unittest.mock import MagicMock, patch

import pytest


# =============================================================================
# _SparkRowPipe Tests
# =============================================================================


class TestSparkRowPipe:
    """Tests for _SparkRowPipe file-like object."""

    def _make_pipe(self, rows, columns=None, batch_rows=10000):
        """Create a _SparkRowPipe from a list of mock Row objects."""
        from dvt.federation.loaders.postgres import _SparkRowPipe

        mock_df = MagicMock()
        mock_df.toLocalIterator.return_value = iter(rows)
        mock_df.columns = columns or ["col1", "col2"]
        return _SparkRowPipe(mock_df, mock_df.columns, batch_rows=batch_rows)

    def test_basic_rows(self):
        """Should produce tab-separated rows with newlines."""
        from dvt.federation.loaders.postgres import _SparkRowPipe

        rows = [
            MagicMock(__iter__=lambda self: iter(["a", "b"])),
            MagicMock(__iter__=lambda self: iter(["c", "d"])),
        ]
        # Use simple tuples instead of MagicMock for iteration
        rows = [("a", "b"), ("c", "d")]
        pipe = self._make_pipe(rows)

        result = pipe.read(-1)
        assert result == b"a\tb\nc\td\n"
        assert pipe.rows_written == 2

    def test_null_handling(self):
        """Should represent None as \\N (PostgreSQL NULL)."""
        rows = [("value", None), (None, "other")]
        pipe = self._make_pipe(rows)

        result = pipe.read(-1)
        assert result == b"value\t\\N\n\\N\tother\n"

    def test_escape_tab(self):
        """Should escape tab characters in values."""
        rows = [("has\ttab", "normal")]
        pipe = self._make_pipe(rows)

        result = pipe.read(-1)
        assert result == b"has\\ttab\tnormal\n"

    def test_escape_newline(self):
        """Should escape newline characters in values."""
        rows = [("has\nnewline", "normal")]
        pipe = self._make_pipe(rows)

        result = pipe.read(-1)
        assert result == b"has\\nnewline\tnormal\n"

    def test_escape_carriage_return(self):
        """Should escape carriage return characters."""
        rows = [("has\rreturn", "normal")]
        pipe = self._make_pipe(rows)

        result = pipe.read(-1)
        assert result == b"has\\rreturn\tnormal\n"

    def test_escape_backslash(self):
        """Should escape backslash characters."""
        rows = [("has\\slash", "normal")]
        pipe = self._make_pipe(rows)

        result = pipe.read(-1)
        assert result == b"has\\\\slash\tnormal\n"

    def test_chunked_reads(self):
        """Should support multiple read() calls with size parameter."""
        rows = [("hello", "world")]
        pipe = self._make_pipe(rows)

        # "hello\tworld\n" = 12 bytes
        chunk1 = pipe.read(5)
        chunk2 = pipe.read(5)
        chunk3 = pipe.read(100)  # remainder

        assert chunk1 + chunk2 + chunk3 == b"hello\tworld\n"

    def test_empty_dataframe(self):
        """Should return empty bytes for empty DataFrame."""
        pipe = self._make_pipe([])

        result = pipe.read(-1)
        assert result == b""
        assert pipe.rows_written == 0

    def test_unicode_data(self):
        """Should handle unicode characters correctly."""
        rows = [("cafe\u0301", "\u00fc\u00e4\u00f6")]
        pipe = self._make_pipe(rows)

        result = pipe.read(-1)
        assert "caf\u00e9" in result.decode("utf-8") or "cafe\u0301" in result.decode(
            "utf-8"
        )

    def test_numeric_values(self):
        """Should convert numeric values to strings."""
        rows = [(42, 3.14, True)]
        pipe = self._make_pipe(rows, columns=["int", "float", "bool"])

        result = pipe.read(-1)
        assert b"42\t3.14\tTrue\n" == result

    def test_sequential_exhaustion(self):
        """After reading all data, subsequent reads return empty bytes."""
        rows = [("a", "b")]
        pipe = self._make_pipe(rows)

        _ = pipe.read(-1)
        assert pipe.read(-1) == b""
        assert pipe.read(100) == b""


# =============================================================================
# fetchmany-based extract_hashes Tests
# =============================================================================


class TestFetchmanyExtractHashes:
    """Tests for the fetchmany-based extract_hashes pattern."""

    def _mock_cursor_with_batches(self, batches):
        """Create a mock cursor that returns batches then empty list."""
        cursor = MagicMock()
        cursor.fetchmany.side_effect = batches + [[]]
        return cursor

    def test_pattern_a_expanded_loop(self):
        """Pattern A: hashes built via expanded for loop (postgres, mysql, etc)."""
        # Simulate the pattern used in postgres/snowflake/bigquery/mysql
        batches = [
            [("pk1", "hash1"), ("pk2", "hash2")],
            [("pk3", "hash3")],
        ]
        cursor = self._mock_cursor_with_batches(batches)

        hashes = {}
        batch_size = 100000
        while True:
            batch = cursor.fetchmany(batch_size)
            if not batch:
                break
            for row in batch:
                hashes[row[0]] = row[1]

        assert hashes == {"pk1": "hash1", "pk2": "hash2", "pk3": "hash3"}
        assert cursor.fetchmany.call_count == 3  # 2 batches + 1 empty

    def test_pattern_b_dict_comprehension(self):
        """Pattern B: hashes built via dict comprehension (databricks, etc)."""
        batches = [
            [("pk1", "hash1"), ("pk2", "hash2")],
            [("pk3", "hash3")],
        ]
        cursor = self._mock_cursor_with_batches(batches)

        hashes = {}
        batch_size = 100000
        while True:
            batch = cursor.fetchmany(batch_size)
            if not batch:
                break
            hashes.update({row[0]: row[1] for row in batch})

        assert hashes == {"pk1": "hash1", "pk2": "hash2", "pk3": "hash3"}

    def test_pattern_c_with_str_cast(self):
        """Pattern C: keys cast to str (sqlserver, oracle)."""
        batches = [
            [(123, "hash1"), (456, "hash2")],
        ]
        cursor = self._mock_cursor_with_batches(batches)

        hashes = {}
        batch_size = 100000
        while True:
            batch = cursor.fetchmany(batch_size)
            if not batch:
                break
            hashes.update({str(row[0]): row[1] for row in batch})

        assert hashes == {"123": "hash1", "456": "hash2"}

    def test_pattern_c_with_strip(self):
        """Pattern C variant: keys and values stripped (teradata)."""
        batches = [
            [("  pk1  ", "  hash1  "), ("pk2 ", " hash2")],
        ]
        cursor = self._mock_cursor_with_batches(batches)

        hashes = {}
        batch_size = 100000
        while True:
            batch = cursor.fetchmany(batch_size)
            if not batch:
                break
            hashes.update({str(row[0]).strip(): str(row[1]).strip() for row in batch})

        assert hashes == {"pk1": "hash1", "pk2": "hash2"}

    def test_pattern_d_python_hashing(self):
        """Pattern D: Python-side hashlib.md5 hashing (generic extractor)."""
        import hashlib

        batches = [
            [("alice", 30, "NYC"), ("bob", 25, "LA")],
        ]
        cursor = self._mock_cursor_with_batches(batches)
        cursor.description = [("name",), ("age",), ("city",)]

        columns = [desc[0] for desc in cursor.description]
        pk_columns = ["name"]
        hashes = {}

        while True:
            batch = cursor.fetchmany(100000)
            if not batch:
                break
            for row in batch:
                row_dict = dict(zip(columns, row))
                pk_value = str(row_dict.get(pk_columns[0], ""))
                values = [str(row_dict.get(col, "")) for col in sorted(columns)]
                row_str = "|".join(values)
                row_hash = hashlib.md5(row_str.encode()).hexdigest()
                hashes[pk_value] = row_hash

        assert "alice" in hashes
        assert "bob" in hashes
        assert len(hashes) == 2

    def test_empty_result_set(self):
        """Should return empty dict for empty result set."""
        cursor = self._mock_cursor_with_batches([])

        hashes = {}
        while True:
            batch = cursor.fetchmany(100000)
            if not batch:
                break
            hashes.update({row[0]: row[1] for row in batch})

        assert hashes == {}

    def test_single_row(self):
        """Should handle single-row result set."""
        batches = [[("pk1", "hash1")]]
        cursor = self._mock_cursor_with_batches(batches)

        hashes = {}
        while True:
            batch = cursor.fetchmany(100000)
            if not batch:
                break
            hashes.update({row[0]: row[1] for row in batch})

        assert hashes == {"pk1": "hash1"}

    def test_duplicate_pk_last_wins(self):
        """When same PK appears in multiple batches, last value wins."""
        batches = [
            [("pk1", "hash_v1")],
            [("pk1", "hash_v2")],
        ]
        cursor = self._mock_cursor_with_batches(batches)

        hashes = {}
        while True:
            batch = cursor.fetchmany(100000)
            if not batch:
                break
            hashes.update({row[0]: row[1] for row in batch})

        assert hashes == {"pk1": "hash_v2"}


# =============================================================================
# Streaming Extraction Tests
# =============================================================================


class TestStreamingPostgresExtractor:
    """Tests for PostgresExtractor._extract_copy_streaming()."""

    def test_fallback_chain_streaming_to_buffered(self):
        """When streaming fails, should fall back to buffered COPY."""
        from dvt.federation.extractors.postgres import PostgresExtractor

        extractor = PostgresExtractor.__new__(PostgresExtractor)
        extractor._on_progress = None
        extractor._lazy_connection = None

        config = MagicMock()
        config.source_name = "test"
        output_path = MagicMock()

        # Make streaming raise, buffered succeed
        mock_result = MagicMock()
        mock_result.extraction_method = "copy"

        with (
            patch.object(
                extractor,
                "_extract_copy_streaming",
                side_effect=Exception("streaming failed"),
            ),
            patch.object(
                extractor, "_extract_copy", return_value=mock_result
            ) as mock_buffered,
            patch.object(extractor, "_log"),
        ):
            result = extractor.extract(config, output_path)

        mock_buffered.assert_called_once_with(config, output_path)
        assert result.extraction_method == "copy"

    def test_fallback_chain_all_to_jdbc(self):
        """When both COPY methods fail, should fall back to JDBC."""
        from dvt.federation.extractors.postgres import PostgresExtractor

        extractor = PostgresExtractor.__new__(PostgresExtractor)
        extractor._on_progress = None
        extractor._lazy_connection = None

        config = MagicMock()
        config.source_name = "test"
        output_path = MagicMock()

        mock_result = MagicMock()
        mock_result.extraction_method = "jdbc"

        with (
            patch.object(
                extractor,
                "_extract_copy_streaming",
                side_effect=Exception("streaming failed"),
            ),
            patch.object(
                extractor,
                "_extract_copy",
                side_effect=Exception("buffered failed"),
            ),
            patch.object(
                extractor, "_extract_jdbc", return_value=mock_result
            ) as mock_jdbc,
            patch.object(extractor, "_log"),
        ):
            result = extractor.extract(config, output_path)

        mock_jdbc.assert_called_once_with(config, output_path)
        assert result.extraction_method == "jdbc"


class TestStreamingDatabricksExtractor:
    """Tests for DatabricksExtractor._extract_native_cursor() streaming."""

    def test_streaming_cursor_with_batches(self):
        """Should use fetchmany in a loop and write batches to ParquetWriter."""
        import tempfile
        from pathlib import Path

        from dvt.federation.extractors.databricks import DatabricksExtractor

        extractor = DatabricksExtractor.__new__(DatabricksExtractor)
        extractor._on_progress = None
        extractor._lazy_connection = None

        # Mock connection and cursor
        mock_cursor = MagicMock()
        mock_cursor.description = [("id",), ("name",)]
        mock_cursor.fetchmany.side_effect = [
            [(1, "alice"), (2, "bob")],
            [(3, "charlie")],
            [],  # end of data
        ]

        mock_conn = MagicMock()
        mock_conn.cursor.return_value = mock_cursor

        tmpdir = tempfile.mkdtemp()
        output_path = Path(tmpdir) / "test.parquet"

        try:
            with (
                patch.object(extractor, "_get_connection", return_value=mock_conn),
                patch.object(
                    extractor,
                    "build_export_query",
                    return_value="SELECT *",
                ),
                patch.object(extractor, "_log"),
            ):
                config = MagicMock()
                config.source_name = "test"
                config.batch_size = 2

                result = extractor._extract_native_cursor(config, output_path)

            assert result.success is True
            assert result.row_count == 3
            assert result.extraction_method == "native_cursor_streaming"
            assert output_path.exists()

            # Verify the Parquet file is readable
            import pyarrow.parquet as pq

            table = pq.read_table(output_path)
            assert table.num_rows == 3
            assert table.column_names == ["id", "name"]
        finally:
            import shutil

            shutil.rmtree(tmpdir, ignore_errors=True)


class TestStreamingPostgresLoader:
    """Tests for PostgresLoader._load_copy_streaming() and fallback chain."""

    def test_fallback_chain_streaming_to_buffered(self):
        """When streaming fails, should fall back to buffered COPY."""
        from dvt.federation.loaders.postgres import PostgresLoader

        loader = PostgresLoader.__new__(PostgresLoader)
        loader._on_progress = None

        df = MagicMock()
        config = MagicMock()
        config.table_name = "test_table"
        adapter = MagicMock()

        mock_result = MagicMock()
        mock_result.load_method = "copy"

        with (
            patch.object(
                loader,
                "_load_copy_streaming",
                side_effect=Exception("streaming failed"),
            ),
            patch.object(
                loader, "_load_copy", return_value=mock_result
            ) as mock_buffered,
            patch.object(loader, "_log"),
        ):
            result = loader.load(df, config, adapter)

        mock_buffered.assert_called_once_with(df, config, adapter)
        assert result.load_method == "copy"

    def test_fallback_chain_all_to_jdbc(self):
        """When both COPY methods fail, should fall back to JDBC."""
        from dvt.federation.loaders.postgres import PostgresLoader

        loader = PostgresLoader.__new__(PostgresLoader)
        loader._on_progress = None

        df = MagicMock()
        config = MagicMock()
        config.table_name = "test_table"
        adapter = MagicMock()

        mock_result = MagicMock()
        mock_result.load_method = "jdbc"

        with (
            patch.object(
                loader,
                "_load_copy_streaming",
                side_effect=Exception("streaming failed"),
            ),
            patch.object(
                loader,
                "_load_copy",
                side_effect=Exception("buffered failed"),
            ),
            patch.object(loader, "_load_jdbc", return_value=mock_result) as mock_jdbc,
            patch.object(loader, "_log"),
        ):
            result = loader.load(df, config, adapter)

        mock_jdbc.assert_called_once_with(df, config, adapter)
        assert result.load_method == "jdbc"


# =============================================================================
# LoadConfig Tests
# =============================================================================


class TestLoadConfig:
    """Tests for LoadConfig with streaming_batch_size field."""

    def test_default_streaming_batch_size(self):
        """streaming_batch_size should default to 10000."""
        from dvt.federation.loaders.base import LoadConfig

        config = LoadConfig(table_name="test_table")
        assert config.streaming_batch_size == 10000

    def test_custom_streaming_batch_size(self):
        """Should accept custom streaming_batch_size."""
        from dvt.federation.loaders.base import LoadConfig

        config = LoadConfig(table_name="test_table", streaming_batch_size=50000)
        assert config.streaming_batch_size == 50000
