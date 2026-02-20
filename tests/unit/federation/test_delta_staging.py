# coding=utf-8
"""Unit tests for Phase 1+2+3: Delta Lake staging and model {{ this }} resolution.

Tests:
A. _get_delta_spark_version() version mapping (sync.py)
B. SparkManager Delta extensions configuration
C. Extractor Delta format write (JDBC + pipe paths)
D. Engine auto-detect Delta vs Parquet in _register_temp_views
E. Sync delta-spark install block
F. Model staging ID generation (_get_model_staging_id)
G. Model self-view registration (_register_model_self_view)
H. Model staging save (_save_model_staging)
I. Model staging clear (_clear_model_staging)
J. is_incremental() federation resolution (_resolve_is_incremental_for_federation)
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
        """Spark 4.1.0 -> None (delta-spark 4.0.1 targets 4.0.x only)."""
        fn = self._get_fn()
        assert fn("4.1.0") is None

    def test_spark_4_0_0(self):
        """Spark 4.0.0 -> delta-spark 4.0.1."""
        fn = self._get_fn()
        assert fn("4.0.0") == "4.0.1"

    def test_spark_4_2_0(self):
        """Spark 4.2.0 (future) -> None (no compatible delta-spark yet)."""
        fn = self._get_fn()
        assert fn("4.2.0") is None

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
    """Tests for Delta Lake configuration in SparkManager."""

    def test_delta_configured_via_configure_spark_with_delta_pip(self):
        """SparkManager should use configure_spark_with_delta_pip for Delta setup."""
        import inspect

        from dvt.federation.spark_manager import SparkManager

        source = inspect.getsource(SparkManager.get_or_create_session)
        assert "configure_spark_with_delta_pip" in source, (
            "Should use configure_spark_with_delta_pip to set up Delta classpath"
        )

    def test_delta_import_is_guarded(self):
        """Delta config should be guarded by try/except ImportError."""
        import inspect

        from dvt.federation.spark_manager import SparkManager

        source = inspect.getsource(SparkManager.get_or_create_session)
        assert "ImportError" in source, (
            "Should catch ImportError when delta-spark is not installed"
        )


# =============================================================================
# C. Centralized Delta Conversion Tests (EL Layer)
# =============================================================================


class TestELLayerDeltaConversion:
    """Tests for centralized Parquet-to-Delta conversion in EL layer."""

    def test_el_layer_has_convert_to_delta_method(self):
        """ELLayer should have _convert_to_delta method."""
        from dvt.federation.el_layer import ELLayer

        assert hasattr(ELLayer, "_convert_to_delta"), (
            "ELLayer should have _convert_to_delta method"
        )

    def test_convert_to_delta_code_structure(self):
        """_convert_to_delta should read Parquet and write Delta."""
        import inspect

        from dvt.federation.el_layer import ELLayer

        source = inspect.getsource(ELLayer._convert_to_delta)
        assert "read.parquet" in source, "Should read the Parquet staging data"
        assert 'format("delta")' in source, "Should write in Delta format"
        assert 'mode("overwrite")' in source, "Should use overwrite mode"

    def test_convert_to_delta_has_fallback(self):
        """_convert_to_delta should fall back to Parquet on failure."""
        import inspect

        from dvt.federation.el_layer import ELLayer

        source = inspect.getsource(ELLayer._convert_to_delta)
        assert "except Exception" in source, (
            "Should catch exceptions for graceful fallback"
        )
        assert ".parquet" in source, "Should fall back to keeping Parquet on failure"

    def test_extract_source_calls_convert_to_delta(self):
        """_extract_source should call _convert_to_delta after extraction."""
        import inspect

        from dvt.federation.el_layer import ELLayer

        # The actual extraction logic is in _extract_source_locked (called
        # by _extract_source under a per-source lock for thread safety).
        source = inspect.getsource(ELLayer._extract_source_locked)
        assert "_convert_to_delta" in source, (
            "Should call _convert_to_delta after successful extraction"
        )
        assert "tmp_" in source, "Should use a temp Parquet path for extraction"

    def test_extractors_still_write_parquet(self):
        """Extractors should write Parquet natively (not Delta)."""
        import inspect

        from dvt.federation.extractors.base import BaseExtractor

        # JDBC path should write Parquet
        jdbc_source = inspect.getsource(BaseExtractor._extract_jdbc)
        assert ".parquet(" in jdbc_source, "JDBC extraction should write Parquet"

        # Pipe path should write Parquet via PyArrow
        pipe_source = inspect.getsource(BaseExtractor._extract_via_pipe)
        assert "ParquetWriter" in pipe_source, (
            "Pipe extraction should write Parquet via PyArrow"
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


# =============================================================================
# F. Model Staging ID Generation Tests
# =============================================================================


class TestModelStagingId:
    """Tests for _get_model_staging_id() in FederationEngine."""

    def _make_model(
        self,
        unique_id="model.my_project.my_model",
        name="my_model",
        schema="dvt_test",
        database=None,
    ):
        """Create a mock model node."""
        from unittest.mock import MagicMock

        model = MagicMock()
        model.unique_id = unique_id
        model.name = name
        model.schema = schema
        model.database = database
        return model

    def _get_engine(self):
        from dvt.federation.engine import FederationEngine

        return FederationEngine

    def test_returns_model_unique_id(self):
        """_get_model_staging_id should return model.unique_id."""
        engine_cls = self._get_engine()
        engine = engine_cls.__new__(engine_cls)
        model = self._make_model()
        result = engine._get_model_staging_id(model)
        assert result == "model.my_project.my_model"

    def test_different_unique_ids(self):
        """Different models should get different staging IDs."""
        engine_cls = self._get_engine()
        engine = engine_cls.__new__(engine_cls)
        model_a = self._make_model(unique_id="model.proj.model_a")
        model_b = self._make_model(unique_id="model.proj.model_b")
        assert engine._get_model_staging_id(model_a) != engine._get_model_staging_id(
            model_b
        )

    def test_stable_across_calls(self):
        """Same model should return same staging ID on repeated calls."""
        engine_cls = self._get_engine()
        engine = engine_cls.__new__(engine_cls)
        model = self._make_model()
        id1 = engine._get_model_staging_id(model)
        id2 = engine._get_model_staging_id(model)
        assert id1 == id2


# =============================================================================
# G. Model Self-View Registration Tests
# =============================================================================


class TestRegisterModelSelfView:
    """Tests for _register_model_self_view() in FederationEngine."""

    def test_method_exists(self):
        """FederationEngine should have _register_model_self_view method."""
        from dvt.federation.engine import FederationEngine

        assert hasattr(FederationEngine, "_register_model_self_view")

    def test_code_checks_staging_exists(self):
        """_register_model_self_view should check el_layer.staging_exists()."""
        import inspect
        from dvt.federation.engine import FederationEngine

        source = inspect.getsource(FederationEngine._register_model_self_view)
        assert "staging_exists" in source, "Should check if model staging exists"

    def test_code_registers_temp_view(self):
        """_register_model_self_view should register a temp view."""
        import inspect
        from dvt.federation.engine import FederationEngine

        source = inspect.getsource(FederationEngine._register_model_self_view)
        assert "createOrReplaceTempView" in source, "Should register temp view"

    def test_view_name_uses_this_suffix(self):
        """Temp view name should use _this suffix."""
        import inspect
        from dvt.federation.engine import FederationEngine

        source = inspect.getsource(FederationEngine._register_model_self_view)
        assert "_this" in source, "View name should include '_this' suffix"

    def test_code_adds_view_mappings(self):
        """Should add 1-part, 2-part, and 3-part reference forms to view_mappings."""
        import inspect
        from dvt.federation.engine import FederationEngine

        source = inspect.getsource(FederationEngine._register_model_self_view)
        # Check for the three mapping patterns
        assert "view_mappings[" in source, "Should mutate view_mappings dict"
        # Check schema.table (2-part) and database.schema.table (3-part) patterns
        assert "schema" in source.lower(), "Should handle schema in reference"

    def test_noop_when_no_staging(self):
        """Should return early (no-op) when model staging does not exist."""
        import inspect
        from dvt.federation.engine import FederationEngine

        source = inspect.getsource(FederationEngine._register_model_self_view)
        # Should have early return when staging_exists returns False
        assert "return" in source, "Should have early return for no staging"

    def test_reads_delta_format(self):
        """Should read Delta format when _delta_log exists."""
        import inspect
        from dvt.federation.engine import FederationEngine

        source = inspect.getsource(FederationEngine._register_model_self_view)
        assert 'format("delta")' in source, "Should read Delta format"

    def test_reads_parquet_fallback(self):
        """Should fall back to Parquet when Delta log not present."""
        import inspect
        from dvt.federation.engine import FederationEngine

        source = inspect.getsource(FederationEngine._register_model_self_view)
        assert "read.parquet" in source, "Should have Parquet fallback"

    def test_view_mappings_include_all_reference_forms(self):
        """Should map table name, schema.table, and database.schema.table forms."""
        import inspect
        from dvt.federation.engine import FederationEngine

        source = inspect.getsource(FederationEngine._register_model_self_view)
        # Count distinct view_mappings assignments — should have 3 patterns
        mapping_count = source.count("view_mappings[")
        assert mapping_count >= 3, (
            f"Should have at least 3 view_mapping entries (1-part, 2-part, 3-part), got {mapping_count}"
        )


# =============================================================================
# H. Model Staging Save Tests
# =============================================================================


class TestSaveModelStaging:
    """Tests for _save_model_staging() in FederationEngine."""

    def test_method_exists(self):
        """FederationEngine should have _save_model_staging method."""
        from dvt.federation.engine import FederationEngine

        assert hasattr(FederationEngine, "_save_model_staging")

    def test_code_writes_delta_format(self):
        """_save_model_staging should write in Delta format."""
        import inspect
        from dvt.federation.engine import FederationEngine

        source = inspect.getsource(FederationEngine._save_model_staging)
        assert 'format("delta")' in source, "Should write in Delta format"

    def test_code_uses_append_for_incremental(self):
        """Should use 'append' mode for incremental materializations."""
        import inspect
        from dvt.federation.engine import FederationEngine

        source = inspect.getsource(FederationEngine._save_model_staging)
        assert '"append"' in source, "Should use append mode for incremental"

    def test_code_uses_overwrite_for_table(self):
        """Should use 'overwrite' mode for table materializations."""
        import inspect
        from dvt.federation.engine import FederationEngine

        source = inspect.getsource(FederationEngine._save_model_staging)
        assert '"overwrite"' in source, "Should use overwrite mode for tables"

    def test_code_enables_column_mapping(self):
        """Should set delta.columnMapping.mode=name for spaces in column names."""
        import inspect
        from dvt.federation.engine import FederationEngine

        source = inspect.getsource(FederationEngine._save_model_staging)
        assert "columnMapping" in source, "Should enable column mapping mode"

    def test_code_sets_min_versions(self):
        """Should set minReaderVersion=2 and minWriterVersion=5 for column mapping."""
        import inspect
        from dvt.federation.engine import FederationEngine

        source = inspect.getsource(FederationEngine._save_model_staging)
        assert "minReaderVersion" in source, "Should set minReaderVersion"
        assert "minWriterVersion" in source, "Should set minWriterVersion"

    def test_code_enables_merge_schema(self):
        """Should set mergeSchema=true for schema evolution."""
        import inspect
        from dvt.federation.engine import FederationEngine

        source = inspect.getsource(FederationEngine._save_model_staging)
        assert "mergeSchema" in source, "Should enable mergeSchema"

    def test_code_has_error_handling(self):
        """Should catch exceptions (model staging is non-fatal)."""
        import inspect
        from dvt.federation.engine import FederationEngine

        source = inspect.getsource(FederationEngine._save_model_staging)
        assert "except Exception" in source, "Should handle errors gracefully"

    def test_staging_path_uses_delta_suffix(self):
        """Staging path should use .delta suffix."""
        import inspect
        from dvt.federation.engine import FederationEngine

        source = inspect.getsource(FederationEngine._save_model_staging)
        assert ".delta" in source, "Staging path should use .delta suffix"

    def test_mode_logic_checks_materialization(self):
        """Mode should be determined by coerced_materialization or original."""
        import inspect
        from dvt.federation.engine import FederationEngine

        source = inspect.getsource(FederationEngine._save_model_staging)
        assert "coerced_materialization" in source, (
            "Should check coerced materialization"
        )
        assert "original_materialization" in source, "Should fall back to original"


# =============================================================================
# I. Model Staging Clear Tests
# =============================================================================


class TestClearModelStaging:
    """Tests for _clear_model_staging() in FederationEngine."""

    def test_method_exists(self):
        """FederationEngine should have _clear_model_staging method."""
        from dvt.federation.engine import FederationEngine

        assert hasattr(FederationEngine, "_clear_model_staging")

    def test_code_delegates_to_state_manager(self):
        """_clear_model_staging should call el_layer.state_manager.clear_staging()."""
        import inspect
        from dvt.federation.engine import FederationEngine

        source = inspect.getsource(FederationEngine._clear_model_staging)
        assert "clear_staging" in source, (
            "Should delegate to state_manager.clear_staging"
        )

    def test_clear_staging_removes_delta_dir(self):
        """StateManager.clear_staging should remove Delta directory."""
        import tempfile
        from dvt.federation.state_manager import StateManager

        with tempfile.TemporaryDirectory() as tmpdir:
            sm = StateManager(Path(tmpdir))
            delta_path = Path(tmpdir) / "model.proj.my_model.delta"
            delta_path.mkdir()
            (delta_path / "_delta_log").mkdir()
            (delta_path / "part-00000.parquet").touch()

            assert delta_path.is_dir()
            sm.clear_staging("model.proj.my_model")
            assert not delta_path.exists()

    def test_clear_staging_removes_parquet_file(self):
        """StateManager.clear_staging should remove legacy Parquet file."""
        import tempfile
        from dvt.federation.state_manager import StateManager

        with tempfile.TemporaryDirectory() as tmpdir:
            sm = StateManager(Path(tmpdir))
            parquet_path = Path(tmpdir) / "model.proj.my_model.parquet"
            parquet_path.touch()

            assert parquet_path.exists()
            sm.clear_staging("model.proj.my_model")
            assert not parquet_path.exists()

    def test_clear_staging_removes_parquet_dir(self):
        """StateManager.clear_staging should remove legacy Parquet directory."""
        import tempfile
        from dvt.federation.state_manager import StateManager

        with tempfile.TemporaryDirectory() as tmpdir:
            sm = StateManager(Path(tmpdir))
            parquet_dir = Path(tmpdir) / "model.proj.my_model.parquet"
            parquet_dir.mkdir()
            (parquet_dir / "part-00000.parquet").touch()

            assert parquet_dir.is_dir()
            sm.clear_staging("model.proj.my_model")
            assert not parquet_dir.exists()

    def test_clear_staging_noop_when_nothing_exists(self):
        """StateManager.clear_staging should not error when nothing to clear."""
        import tempfile
        from dvt.federation.state_manager import StateManager

        with tempfile.TemporaryDirectory() as tmpdir:
            sm = StateManager(Path(tmpdir))
            # Should not raise
            sm.clear_staging("model.proj.nonexistent")


# =============================================================================
# J. is_incremental() Federation Resolution Tests
# =============================================================================


class TestIsIncrementalFederationResolution:
    """Tests for _resolve_is_incremental_for_federation() in runner_mixin."""

    def test_method_exists(self):
        """Runner mixin should have _resolve_is_incremental_for_federation method."""
        from dvt.dvt_runners.runner_mixin import DvtRunnerMixin

        assert hasattr(DvtRunnerMixin, "_resolve_is_incremental_for_federation")

    def test_code_checks_materialization(self):
        """Should only resolve for incremental materializations."""
        import inspect
        from dvt.dvt_runners.runner_mixin import DvtRunnerMixin

        source = inspect.getsource(
            DvtRunnerMixin._resolve_is_incremental_for_federation
        )
        assert '"incremental"' in source, "Should check for incremental materialization"

    def test_code_checks_full_refresh(self):
        """Should return False for --full-refresh."""
        import inspect
        from dvt.dvt_runners.runner_mixin import DvtRunnerMixin

        source = inspect.getsource(
            DvtRunnerMixin._resolve_is_incremental_for_federation
        )
        assert "FULL_REFRESH" in source, "Should check FULL_REFRESH flag"

    def test_code_uses_state_manager(self):
        """Should use StateManager to check Delta staging existence."""
        import inspect
        from dvt.dvt_runners.runner_mixin import DvtRunnerMixin

        source = inspect.getsource(
            DvtRunnerMixin._resolve_is_incremental_for_federation
        )
        assert "StateManager" in source, "Should use StateManager"
        assert "staging_exists" in source, "Should call staging_exists"

    def test_returns_dict_with_is_incremental(self):
        """Should return dict with 'is_incremental' key."""
        import inspect
        from dvt.dvt_runners.runner_mixin import DvtRunnerMixin

        source = inspect.getsource(
            DvtRunnerMixin._resolve_is_incremental_for_federation
        )
        assert '"is_incremental"' in source or "'is_incremental'" in source, (
            "Should return dict with is_incremental key"
        )

    def test_returns_lambda(self):
        """Should return lambda functions (not bare booleans)."""
        import inspect
        from dvt.dvt_runners.runner_mixin import DvtRunnerMixin

        source = inspect.getsource(
            DvtRunnerMixin._resolve_is_incremental_for_federation
        )
        assert "lambda:" in source, "Should return lambda functions"

    def test_non_incremental_returns_empty_dict(self):
        """Non-incremental materializations should return empty dict."""
        import inspect
        from dvt.dvt_runners.runner_mixin import DvtRunnerMixin

        source = inspect.getsource(
            DvtRunnerMixin._resolve_is_incremental_for_federation
        )
        assert "return {}" in source, "Should return empty dict for non-incremental"

    def test_compile_with_target_uses_extra_context(self):
        """_compile_with_target_adapter should pass extra_context from is_incremental resolution."""
        import inspect
        from dvt.dvt_runners.runner_mixin import DvtRunnerMixin

        source = inspect.getsource(DvtRunnerMixin._compile_with_target_adapter)
        assert "_resolve_is_incremental_for_federation" in source, (
            "Should call _resolve_is_incremental_for_federation"
        )
        assert "extra_context" in source, "Should use extra_context variable"

    def test_compile_node_receives_extra_context(self):
        """compile_node should receive extra_context as third argument."""
        import inspect
        from dvt.dvt_runners.runner_mixin import DvtRunnerMixin

        source = inspect.getsource(DvtRunnerMixin._compile_with_target_adapter)
        assert "compile_node" in source, "Should call compile_node"
        assert "extra_context" in source, "Should pass extra_context to compile_node"


# =============================================================================
# K. Engine execute() Wiring Tests
# =============================================================================


class TestEngineExecuteWiring:
    """Tests for model staging wiring in execute() flow."""

    def test_execute_clears_staging_on_full_refresh(self):
        """execute() should call _clear_model_staging when full_refresh is True."""
        import inspect
        from dvt.federation.engine import FederationEngine

        source = inspect.getsource(FederationEngine.execute)
        assert "_clear_model_staging" in source, (
            "execute() should call _clear_model_staging"
        )
        assert "full_refresh" in source.lower() or "FULL_REFRESH" in source, (
            "execute() should check full_refresh flag"
        )

    def test_execute_registers_model_self_view(self):
        """execute() should call _register_model_self_view."""
        import inspect
        from dvt.federation.engine import FederationEngine

        source = inspect.getsource(FederationEngine.execute)
        assert "_register_model_self_view" in source, (
            "execute() should call _register_model_self_view"
        )

    def test_execute_saves_model_staging(self):
        """execute() should call _save_model_staging after load."""
        import inspect
        from dvt.federation.engine import FederationEngine

        source = inspect.getsource(FederationEngine.execute)
        assert "_save_model_staging" in source, (
            "execute() should call _save_model_staging"
        )

    def test_self_view_registered_before_sql_translation(self):
        """Model self-view should be registered before _translate_to_spark."""
        import inspect
        from dvt.federation.engine import FederationEngine

        source = inspect.getsource(FederationEngine.execute)
        self_view_pos = source.find("_register_model_self_view")
        translate_pos = source.find("_translate_to_spark")
        assert self_view_pos > 0, "Should have _register_model_self_view call"
        assert translate_pos > 0, "Should have _translate_to_spark call"
        assert self_view_pos < translate_pos, (
            "Model self-view registration should happen BEFORE SQL translation"
        )

    def test_staging_saved_after_load(self):
        """Model staging save should happen after _write_to_target."""
        import inspect
        from dvt.federation.engine import FederationEngine

        source = inspect.getsource(FederationEngine.execute)
        write_pos = source.find("_write_to_target")
        save_pos = source.find("_save_model_staging")
        assert write_pos > 0, "Should have _write_to_target call"
        assert save_pos > 0, "Should have _save_model_staging call"
        assert save_pos > write_pos, (
            "Model staging save should happen AFTER writing to target"
        )

    def test_clear_staging_before_extraction(self):
        """Model staging clear should happen before source extraction."""
        import inspect
        from dvt.federation.engine import FederationEngine

        source = inspect.getsource(FederationEngine.execute)
        clear_pos = source.find("_clear_model_staging")
        extract_pos = source.find("extract_sources")
        assert clear_pos > 0, "Should have _clear_model_staging call"
        assert extract_pos > 0, "Should have extract_sources call"
        assert clear_pos < extract_pos, (
            "Model staging clear should happen BEFORE source extraction"
        )


# =============================================================================
# L. Staging Existence Check Tests (model-prefixed IDs)
# =============================================================================


class TestStagingExistsForModels:
    """Tests for StateManager.staging_exists() with model-prefixed IDs."""

    def test_delta_staging_detected(self):
        """staging_exists should return True for model Delta staging."""
        import tempfile
        from dvt.federation.state_manager import StateManager

        with tempfile.TemporaryDirectory() as tmpdir:
            sm = StateManager(Path(tmpdir))
            delta_path = Path(tmpdir) / "model.Coke_DB.incremental_test.delta"
            delta_path.mkdir()
            (delta_path / "_delta_log").mkdir()

            assert sm.staging_exists("model.Coke_DB.incremental_test") is True

    def test_no_staging_returns_false(self):
        """staging_exists should return False when no staging exists."""
        import tempfile
        from dvt.federation.state_manager import StateManager

        with tempfile.TemporaryDirectory() as tmpdir:
            sm = StateManager(Path(tmpdir))
            assert sm.staging_exists("model.Coke_DB.nonexistent") is False

    def test_empty_delta_dir_returns_false(self):
        """staging_exists should return False for empty .delta dir (no _delta_log)."""
        import tempfile
        from dvt.federation.state_manager import StateManager

        with tempfile.TemporaryDirectory() as tmpdir:
            sm = StateManager(Path(tmpdir))
            delta_path = Path(tmpdir) / "model.proj.test.delta"
            delta_path.mkdir()
            # No _delta_log inside
            assert sm.staging_exists("model.proj.test") is False

    def test_legacy_parquet_file_detected(self):
        """staging_exists should return True for legacy model Parquet file."""
        import tempfile
        from dvt.federation.state_manager import StateManager

        with tempfile.TemporaryDirectory() as tmpdir:
            sm = StateManager(Path(tmpdir))
            parquet_path = Path(tmpdir) / "model.proj.test.parquet"
            parquet_path.touch()
            assert sm.staging_exists("model.proj.test") is True

    def test_legacy_parquet_dir_detected(self):
        """staging_exists should return True for legacy model Parquet directory."""
        import tempfile
        from dvt.federation.state_manager import StateManager

        with tempfile.TemporaryDirectory() as tmpdir:
            sm = StateManager(Path(tmpdir))
            parquet_dir = Path(tmpdir) / "model.proj.test.parquet"
            parquet_dir.mkdir()
            (parquet_dir / "part-00000.parquet").touch()
            assert sm.staging_exists("model.proj.test") is True
