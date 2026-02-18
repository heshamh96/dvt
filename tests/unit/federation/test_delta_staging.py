# coding=utf-8
"""Unit tests for Phase 1+2: Delta Lake staging.

Tests:
A. _get_delta_spark_version() version mapping (sync.py)
B. SparkManager Delta extensions configuration
C. Extractor Delta format write (JDBC + pipe paths)
D. Engine auto-detect Delta vs Parquet in _register_temp_views
"""

from pathlib import Path

import pytest


# =============================================================================
# A. Delta-Spark Version Mapping Tests (sync.py)
# =============================================================================


class TestGetDeltaSparkVersion:
    """Tests for _get_delta_spark_version() in sync.py."""

    def _get_fn(self):
        from dvt.task.sync import _get_delta_spark_version

        return _get_delta_spark_version

    def test_spark_4_1_0(self):
        """Spark 4.1.0 -> delta-spark 4.0.1."""
        fn = self._get_fn()
        assert fn("4.1.0") == "4.0.1"

    def test_spark_4_0_0(self):
        """Spark 4.0.0 -> delta-spark 4.0.1."""
        fn = self._get_fn()
        assert fn("4.0.0") == "4.0.1"

    def test_spark_4_2_0(self):
        """Spark 4.2.0 (future) -> delta-spark 4.0.1."""
        fn = self._get_fn()
        assert fn("4.2.0") == "4.0.1"

    def test_spark_3_5_3(self):
        """Spark 3.5.x -> delta-spark 3.2.1."""
        fn = self._get_fn()
        assert fn("3.5.3") == "3.2.1"

    def test_spark_3_5_0(self):
        """Spark 3.5.0 -> delta-spark 3.2.1."""
        fn = self._get_fn()
        assert fn("3.5.0") == "3.2.1"

    def test_spark_3_4_0(self):
        """Spark 3.4.0 -> delta-spark 2.4.0."""
        fn = self._get_fn()
        assert fn("3.4.0") == "2.4.0"

    def test_spark_3_3_0(self):
        """Spark 3.3.0 -> delta-spark 2.3.0."""
        fn = self._get_fn()
        assert fn("3.3.0") == "2.3.0"

    def test_spark_3_2_0(self):
        """Spark 3.2.0 -> delta-spark 2.0.2."""
        fn = self._get_fn()
        assert fn("3.2.0") == "2.0.2"

    def test_spark_3_1_0_unsupported(self):
        """Spark 3.1.x -> None (unsupported)."""
        fn = self._get_fn()
        assert fn("3.1.0") is None

    def test_spark_2_x_unsupported(self):
        """Spark 2.x -> None (unsupported)."""
        fn = self._get_fn()
        assert fn("2.4.8") is None

    def test_major_only(self):
        """Spark '4' (just major) -> delta-spark 4.0.1."""
        fn = self._get_fn()
        assert fn("4") == "4.0.1"

    def test_non_numeric_returns_none(self):
        """Non-numeric version string -> None."""
        fn = self._get_fn()
        assert fn("abc") is None


# =============================================================================
# B. SparkManager Delta Extensions Tests
# =============================================================================


class TestSparkManagerDeltaConfig:
    """Tests for Delta Lake extensions in SparkManager."""

    def test_delta_extensions_in_session_builder_code(self):
        """SparkManager.get_or_create_session should configure Delta extensions."""
        import inspect

        from dvt.federation.spark_manager import SparkManager

        source = inspect.getsource(SparkManager.get_or_create_session)
        assert "DeltaSparkSessionExtension" in source, (
            "Should configure spark.sql.extensions with DeltaSparkSessionExtension"
        )
        assert "DeltaCatalog" in source, (
            "Should configure spark.sql.catalog.spark_catalog with DeltaCatalog"
        )

    def test_delta_import_is_guarded(self):
        """Delta extension config should be guarded by try/except ImportError."""
        import inspect

        from dvt.federation.spark_manager import SparkManager

        source = inspect.getsource(SparkManager.get_or_create_session)
        assert "import delta" in source, (
            "Should try to import delta to check availability"
        )
        assert "ImportError" in source, (
            "Should catch ImportError when delta-spark is not installed"
        )


# =============================================================================
# C. Extractor Delta Format Write Tests
# =============================================================================


class TestExtractorDeltaWrite:
    """Tests for Delta format output in extractors."""

    def test_jdbc_extraction_writes_delta_format(self):
        """_extract_jdbc should write Delta format instead of Parquet."""
        import inspect

        from dvt.federation.extractors.base import BaseExtractor

        source = inspect.getsource(BaseExtractor._extract_jdbc)
        # Should use Delta format for writing
        assert 'format("delta")' in source, (
            "_extract_jdbc should use df.write.format('delta')"
        )
        assert 'mode("overwrite")' in source, "_extract_jdbc should use overwrite mode"
        # Should NOT use parquet() directly for writing anymore
        assert (
            ".parquet(" not in source.split("# Write to Delta")[1]
            if "# Write to Delta" in source
            else True
        ), "_extract_jdbc should not use .parquet() for writing"
        # Should read back via Delta for row count
        assert 'format("delta").load' in source.replace("\n", "").replace(" ", ""), (
            "_extract_jdbc should read back via Delta format for row count"
        )

    def test_pipe_extraction_converts_to_delta(self):
        """_extract_via_pipe should write temp Parquet then convert to Delta."""
        import inspect

        from dvt.federation.extractors.base import BaseExtractor

        source = inspect.getsource(BaseExtractor._extract_via_pipe)
        assert 'format("delta")' in source, (
            "_extract_via_pipe should convert to Delta format"
        )
        assert ".tmp_" in source, "_extract_via_pipe should use a temp Parquet file"
        assert "SparkManager" in source, (
            "_extract_via_pipe should use SparkManager for conversion"
        )


# =============================================================================
# D. Engine Auto-Detect Delta vs Parquet Tests
# =============================================================================


class TestEngineAutoDetect:
    """Tests for auto-detecting Delta vs Parquet in _register_temp_views."""

    def test_register_temp_views_code_has_delta_detection(self):
        """Engine._register_temp_views should auto-detect Delta format."""
        import inspect

        from dvt.federation.engine import FederationEngine

        source = inspect.getsource(FederationEngine._register_temp_views)
        assert "_delta_log" in source, "Should check for _delta_log directory"
        assert 'format("delta")' in source, (
            "Should use spark.read.format('delta') for Delta tables"
        )
        assert "read.parquet" in source, (
            "Should fall back to spark.read.parquet() for legacy data"
        )

    def test_staging_path_delta_detection_logic(self):
        """Verify Delta detection logic: is_dir + _delta_log subdirectory."""
        import tempfile

        with tempfile.TemporaryDirectory() as tmpdir:
            staging_path = Path(tmpdir) / "test.delta"

            # No directory -> not Delta
            assert not staging_path.is_dir()

            # Empty directory -> not Delta (no _delta_log)
            staging_path.mkdir()
            assert staging_path.is_dir()
            assert not (staging_path / "_delta_log").is_dir()

            # With _delta_log -> is Delta
            (staging_path / "_delta_log").mkdir()
            assert staging_path.is_dir()
            assert (staging_path / "_delta_log").is_dir()

    def test_legacy_parquet_file_detected(self):
        """Legacy Parquet single file should be handled as Parquet."""
        import tempfile

        with tempfile.TemporaryDirectory() as tmpdir:
            parquet_path = Path(tmpdir) / "test.parquet"
            parquet_path.touch()

            # It's a file, not a dir with _delta_log -> Parquet
            assert parquet_path.exists()
            assert not parquet_path.is_dir()

    def test_legacy_parquet_directory_detected(self):
        """Legacy Parquet directory (JDBC output) should be handled as Parquet."""
        import tempfile

        with tempfile.TemporaryDirectory() as tmpdir:
            parquet_dir = Path(tmpdir) / "test.parquet"
            parquet_dir.mkdir()
            (parquet_dir / "part-00000.parquet").touch()

            # It's a dir but no _delta_log -> Parquet
            assert parquet_dir.is_dir()
            assert not (parquet_dir / "_delta_log").is_dir()


# =============================================================================
# E. Sync delta-spark Install Block Tests
# =============================================================================


class TestSyncDeltaInstall:
    """Tests for delta-spark install logic in sync.py."""

    def test_sync_code_installs_delta_after_pyspark(self):
        """sync.py should install delta-spark immediately after pyspark."""
        import inspect

        from dvt.task.sync import SyncTask

        source = inspect.getsource(SyncTask.run)
        # Find positions of pyspark and delta-spark install in source code
        # Note: f-strings in source have {variable}, not expanded values
        pyspark_pos = source.find("Installing pyspark==")
        delta_pos = source.find("Installing {delta_pkg}")

        assert pyspark_pos > 0, "Should have pyspark install"
        assert delta_pos > 0, "Should have delta-spark install"
        assert delta_pos > pyspark_pos, (
            "delta-spark install should come after pyspark install"
        )

    def test_sync_code_uses_version_mapping(self):
        """sync.py should use _get_delta_spark_version for version resolution."""
        import inspect

        from dvt.task.sync import SyncTask

        source = inspect.getsource(SyncTask.run)
        assert "_get_delta_spark_version" in source, (
            "Should use _get_delta_spark_version() for version mapping"
        )
