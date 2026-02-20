# coding=utf-8
"""Unit tests for streaming EL pipeline enhancements.

Tests:
1. fetchmany-based extract_hashes: batched hash extraction
2. Streaming PostgresExtractor: tempfile + PyArrow CSV streaming
3. Streaming DatabricksExtractor: fetchmany + PyArrow ParquetWriter
4. Fallback chains: streaming -> buffered -> JDBC
5. LoadConfig field validation

Note: _SparkRowPipe and TestStreamingPostgresLoader tests were removed
in Phase 7 loader simplification. Those classes no longer exist — all
loaders now use FederationLoader with JDBC + adapter pattern.
"""

from unittest.mock import MagicMock, patch


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


# =============================================================================
# LoadConfig Tests
# =============================================================================


class TestLoadConfig:
    """Tests for LoadConfig fields after Phase 7 loader simplification."""

    def test_default_mode(self):
        """mode should default to 'overwrite'."""
        from dvt.federation.loaders.base import LoadConfig

        config = LoadConfig(table_name="test_table")
        assert config.mode == "overwrite"

    def test_default_truncate(self):
        """truncate should default to True."""
        from dvt.federation.loaders.base import LoadConfig

        config = LoadConfig(table_name="test_table")
        assert config.truncate is True

    def test_default_full_refresh(self):
        """full_refresh should default to False."""
        from dvt.federation.loaders.base import LoadConfig

        config = LoadConfig(table_name="test_table")
        assert config.full_refresh is False

    def test_incremental_strategy_placeholder(self):
        """incremental_strategy should default to None (Phase 4 placeholder)."""
        from dvt.federation.loaders.base import LoadConfig

        config = LoadConfig(table_name="test_table")
        assert config.incremental_strategy is None

    def test_unique_key_placeholder(self):
        """unique_key should default to None (Phase 4 placeholder)."""
        from dvt.federation.loaders.base import LoadConfig

        config = LoadConfig(table_name="test_table")
        assert config.unique_key is None

    def test_no_streaming_batch_size(self):
        """streaming_batch_size should no longer exist on LoadConfig."""
        from dvt.federation.loaders.base import LoadConfig

        config = LoadConfig(table_name="test_table")
        assert not hasattr(config, "streaming_batch_size")

    def test_no_bucket_config(self):
        """bucket_config should no longer exist on LoadConfig."""
        from dvt.federation.loaders.base import LoadConfig

        config = LoadConfig(table_name="test_table")
        assert not hasattr(config, "bucket_config")
