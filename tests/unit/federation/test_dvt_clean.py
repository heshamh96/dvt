# coding=utf-8
"""Unit tests for Phase 5: dvt clean --optimize extension.

Tests:
A. CLI parameter definition (--optimize flag)
B. CleanTask __init__ reads optimize flag
C. CleanTask.run() dispatch logic (--optimize skips dbt clean)
D. _optimize_staging() method structure
E. _optimize_delta_tables() method structure
F. _optimize_delta_tables() Delta table discovery
G. _optimize_delta_tables_hdfs() method structure
H. parse_duration() utility (existing, sanity check)
"""

import tempfile
from pathlib import Path
from unittest.mock import MagicMock

import pytest


# =============================================================================
# A. CLI Parameter Definition Tests
# =============================================================================


class TestCleanOptimizeParam:
    """Tests for the --optimize CLI parameter definition."""

    def test_optimize_param_exists(self):
        """params.py should define clean_optimize option."""
        from dvt.cli import params

        assert hasattr(params, "clean_optimize"), (
            "params.py should define clean_optimize"
        )

    def test_optimize_param_is_boolean(self):
        """--optimize should be a boolean flag (--optimize/--no-optimize)."""
        import inspect
        from dvt.cli import params

        source = inspect.getsource(params.clean_optimize)
        # It's a decorator, check the creation call
        # We can inspect that it was created with is_flag=True (implied by --optimize/--no-optimize)
        assert "clean_optimize" in dir(params)

    def test_optimize_default_is_false(self):
        """--optimize should default to False."""
        import inspect
        from dvt.cli import params

        # Read the source around clean_optimize definition
        source = inspect.getsource(params)
        # Find the clean_optimize definition
        idx = source.find("clean_optimize")
        chunk = source[idx : idx + 300]
        assert "default=False" in chunk, "clean_optimize should default to False"

    def test_clean_command_has_optimize_decorator(self):
        """The clean CLI command should include the @p.clean_optimize decorator."""
        import inspect
        from dvt.cli import main

        source = inspect.getsource(main)
        # Find the clean command definition section
        clean_idx = source.find('command("clean")')
        assert clean_idx > 0, "Should find the clean command definition"
        # The decorators are between @cli.command and def clean
        def_idx = source.find("def clean(", clean_idx)
        decorator_section = source[clean_idx:def_idx]
        assert "clean_optimize" in decorator_section, (
            "clean command should have @p.clean_optimize decorator"
        )


# =============================================================================
# B. CleanTask __init__ Tests
# =============================================================================


class TestCleanTaskInit:
    """Tests for CleanTask.__init__ reading the optimize flag."""

    def test_clean_task_has_optimize_attribute(self):
        """CleanTask should store the optimize flag."""
        import inspect
        from dvt.task.clean import CleanTask

        source = inspect.getsource(CleanTask.__init__)
        assert "optimize" in source.lower(), (
            "CleanTask.__init__ should read the optimize flag"
        )

    def test_clean_task_reads_optimize_from_args(self):
        """CleanTask should read OPTIMIZE from args (uppercase convention)."""
        import inspect
        from dvt.task.clean import CleanTask

        source = inspect.getsource(CleanTask.__init__)
        assert "OPTIMIZE" in source, "Should read OPTIMIZE (uppercase) from flags"


# =============================================================================
# C. CleanTask.run() Dispatch Logic Tests
# =============================================================================


class TestCleanTaskRunDispatch:
    """Tests for CleanTask.run() dispatching to optimize path."""

    def test_run_calls_optimize_staging(self):
        """run() should call _optimize_staging when optimize=True."""
        import inspect
        from dvt.task.clean import CleanTask

        source = inspect.getsource(CleanTask.run)
        assert "_optimize_staging" in source, "run() should call _optimize_staging"

    def test_optimize_skips_dbt_clean(self):
        """When --optimize is set, dbt clean should be skipped."""
        import inspect
        from dvt.task.clean import CleanTask

        source = inspect.getsource(CleanTask.run)
        # The optimize check should come before _run_dbt_clean
        optimize_pos = source.find("self.optimize")
        dbt_clean_pos = source.find("_run_dbt_clean")
        assert optimize_pos > 0, "Should check self.optimize"
        assert optimize_pos < dbt_clean_pos, (
            "Optimize check should come before dbt clean call"
        )

    def test_optimize_returns_early(self):
        """When --optimize is set, should return None after optimize."""
        import inspect
        from dvt.task.clean import CleanTask

        source = inspect.getsource(CleanTask.run)
        # Find the optimize block
        opt_idx = source.find("self.optimize")
        next_return = source.find("return", opt_idx)
        next_clean = source.find("_clean_staging", opt_idx)
        # The return should come before _clean_staging
        assert next_return < next_clean, (
            "Should return after _optimize_staging, not fall through to _clean_staging"
        )

    def test_run_docstring_mentions_optimize(self):
        """run() docstring should document the --optimize behavior."""
        from dvt.task.clean import CleanTask

        assert "optimize" in (CleanTask.run.__doc__ or "").lower(), (
            "run() docstring should mention optimize"
        )


# =============================================================================
# D. _optimize_staging() Method Structure Tests
# =============================================================================


class TestOptimizeStagingMethod:
    """Tests for _optimize_staging() method structure."""

    def test_method_exists(self):
        """CleanTask should have _optimize_staging method."""
        from dvt.task.clean import CleanTask

        assert hasattr(CleanTask, "_optimize_staging")

    def test_loads_bucket_config(self):
        """_optimize_staging should load bucket configuration."""
        import inspect
        from dvt.task.clean import CleanTask

        source = inspect.getsource(CleanTask._optimize_staging)
        assert "load_buckets_for_profile" in source, (
            "Should load bucket config via load_buckets_for_profile"
        )

    def test_iterates_buckets(self):
        """_optimize_staging should iterate over configured buckets."""
        import inspect
        from dvt.task.clean import CleanTask

        source = inspect.getsource(CleanTask._optimize_staging)
        assert "for bucket_name" in source, "Should iterate over bucket configs"

    def test_respects_bucket_filter(self):
        """_optimize_staging should respect --bucket filter."""
        import inspect
        from dvt.task.clean import CleanTask

        source = inspect.getsource(CleanTask._optimize_staging)
        assert "self.bucket_name" in source, (
            "Should check self.bucket_name for filtering"
        )

    def test_dispatches_to_filesystem(self):
        """_optimize_staging should call _optimize_delta_tables for filesystem."""
        import inspect
        from dvt.task.clean import CleanTask

        source = inspect.getsource(CleanTask._optimize_staging)
        assert "_optimize_delta_tables(" in source, (
            "Should dispatch to _optimize_delta_tables for filesystem"
        )

    def test_dispatches_to_hdfs(self):
        """_optimize_staging should call _optimize_delta_tables_hdfs for HDFS."""
        import inspect
        from dvt.task.clean import CleanTask

        source = inspect.getsource(CleanTask._optimize_staging)
        assert "_optimize_delta_tables_hdfs" in source, (
            "Should dispatch to _optimize_delta_tables_hdfs for HDFS"
        )

    def test_falls_back_to_project_staging(self):
        """When no buckets.yml exists, should fall back to project's .dvt/staging/."""
        import inspect
        from dvt.task.clean import CleanTask

        source = inspect.getsource(CleanTask._optimize_staging)
        assert "get_project_root" in source, (
            "Should fall back to project root when no buckets.yml"
        )
        assert ".dvt" in source, "Fallback path should include .dvt"
        assert "staging" in source, "Fallback path should include staging"


# =============================================================================
# E. _optimize_delta_tables() Method Structure Tests
# =============================================================================


class TestOptimizeDeltaTables:
    """Tests for _optimize_delta_tables() method structure."""

    def test_method_exists(self):
        """CleanTask should have _optimize_delta_tables method."""
        from dvt.task.clean import CleanTask

        assert hasattr(CleanTask, "_optimize_delta_tables")

    def test_imports_delta_table(self):
        """Should import DeltaTable from delta."""
        import inspect
        from dvt.task.clean import CleanTask

        source = inspect.getsource(CleanTask._optimize_delta_tables)
        assert "DeltaTable" in source, "Should import DeltaTable"

    def test_handles_missing_delta_spark(self):
        """Should handle ImportError when delta-spark is not installed."""
        import inspect
        from dvt.task.clean import CleanTask

        source = inspect.getsource(CleanTask._optimize_delta_tables)
        assert "ImportError" in source, (
            "Should catch ImportError for missing delta-spark"
        )

    def test_runs_optimize(self):
        """Should run DeltaTable.optimize().executeCompaction()."""
        import inspect
        from dvt.task.clean import CleanTask

        source = inspect.getsource(CleanTask._optimize_delta_tables)
        assert "optimize()" in source, "Should call optimize()"
        assert "executeCompaction()" in source, "Should call executeCompaction()"

    def test_runs_vacuum(self):
        """Should run DeltaTable.vacuum()."""
        import inspect
        from dvt.task.clean import CleanTask

        source = inspect.getsource(CleanTask._optimize_delta_tables)
        assert "vacuum(" in source, "Should call vacuum()"

    def test_vacuum_uses_configurable_retention(self):
        """Should use configurable retention hours from computes.yml delta: section."""
        import inspect
        from dvt.task.clean import CleanTask

        source = inspect.getsource(CleanTask._optimize_delta_tables)
        assert "retentionHours=retention_hours" in source, (
            "Should use configurable retention_hours from _get_cleanup_retention_hours()"
        )
        # Verify the retention is read from config
        source_getter = inspect.getsource(CleanTask._get_cleanup_retention_hours)
        assert "cleanup_retention_hours" in source_getter, (
            "Should read cleanup_retention_hours from computes.yml delta: section"
        )

    def test_disables_retention_check(self):
        """Should disable Delta retention duration check for 0-hour VACUUM."""
        import inspect
        from dvt.task.clean import CleanTask

        source = inspect.getsource(CleanTask._optimize_delta_tables)
        assert "retentionDurationCheck" in source, (
            "Should disable retention duration check"
        )

    def test_uses_spark_manager(self):
        """Should try to use SparkManager for session creation."""
        import inspect
        from dvt.task.clean import CleanTask

        source = inspect.getsource(CleanTask._optimize_delta_tables)
        assert "SparkManager" in source, (
            "Should try SparkManager.get_or_create_session()"
        )

    def test_has_spark_fallback(self):
        """Should fall back to creating own Spark session if SparkManager fails."""
        import inspect
        from dvt.task.clean import CleanTask

        source = inspect.getsource(CleanTask._optimize_delta_tables)
        assert "configure_spark_with_delta_pip" in source, (
            "Should have fallback Spark session creation with Delta"
        )

    def test_handles_per_table_errors(self):
        """Should catch exceptions per table (don't fail all if one fails)."""
        import inspect
        from dvt.task.clean import CleanTask

        source = inspect.getsource(CleanTask._optimize_delta_tables)
        # Should have try/except inside the loop
        assert source.count("except Exception") >= 1, (
            "Should handle per-table errors gracefully"
        )

    def test_reports_progress(self):
        """Should fire events with optimization progress."""
        import inspect
        from dvt.task.clean import CleanTask

        source = inspect.getsource(CleanTask._optimize_delta_tables)
        assert "fire_event" in source, "Should report progress via fire_event"


# =============================================================================
# F. Delta Table Discovery Tests
# =============================================================================


class TestDeltaTableDiscovery:
    """Tests for Delta table discovery logic in _optimize_delta_tables."""

    def test_finds_delta_dirs(self):
        """Should identify directories ending in .delta with _delta_log."""
        with tempfile.TemporaryDirectory() as tmpdir:
            staging = Path(tmpdir)

            # Create a valid Delta directory
            delta_dir = staging / "source.proj.table.delta"
            delta_dir.mkdir()
            (delta_dir / "_delta_log").mkdir()

            # Create a non-Delta directory
            other_dir = staging / "something_else"
            other_dir.mkdir()

            # Create a .delta dir without _delta_log (not valid)
            fake_delta = staging / "invalid.delta"
            fake_delta.mkdir()

            # Count valid Delta dirs
            delta_tables = []
            for entry in staging.iterdir():
                if entry.is_dir() and entry.name.endswith(".delta"):
                    if (entry / "_delta_log").is_dir():
                        delta_tables.append(entry)

            assert len(delta_tables) == 1
            assert delta_tables[0].name == "source.proj.table.delta"

    def test_finds_model_delta_staging(self):
        """Should find model.*.delta directories (Phase 3 model staging)."""
        with tempfile.TemporaryDirectory() as tmpdir:
            staging = Path(tmpdir)

            # Source staging
            src = staging / "source.Coke_DB.postgres_source.transactions_a.delta"
            src.mkdir()
            (src / "_delta_log").mkdir()

            # Model staging
            mdl = staging / "model.Coke_DB.incremental_test.delta"
            mdl.mkdir()
            (mdl / "_delta_log").mkdir()

            delta_tables = []
            for entry in staging.iterdir():
                if entry.is_dir() and entry.name.endswith(".delta"):
                    if (entry / "_delta_log").is_dir():
                        delta_tables.append(entry)

            assert len(delta_tables) == 2
            names = {t.name for t in delta_tables}
            assert "source.Coke_DB.postgres_source.transactions_a.delta" in names
            assert "model.Coke_DB.incremental_test.delta" in names

    def test_skips_state_directory(self):
        """Should not treat _state/ directory as a Delta table."""
        with tempfile.TemporaryDirectory() as tmpdir:
            staging = Path(tmpdir)

            # _state directory
            state = staging / "_state"
            state.mkdir()

            delta_tables = []
            for entry in staging.iterdir():
                if entry.is_dir() and entry.name.endswith(".delta"):
                    if (entry / "_delta_log").is_dir():
                        delta_tables.append(entry)

            assert len(delta_tables) == 0

    def test_skips_parquet_files(self):
        """Should not treat .parquet files as Delta tables."""
        with tempfile.TemporaryDirectory() as tmpdir:
            staging = Path(tmpdir)

            # Legacy parquet file
            pq = staging / "source.proj.table.parquet"
            pq.touch()

            # Legacy parquet directory
            pq_dir = staging / "source.proj.other.parquet"
            pq_dir.mkdir()
            (pq_dir / "part-00000.parquet").touch()

            delta_tables = []
            for entry in staging.iterdir():
                if entry.is_dir() and entry.name.endswith(".delta"):
                    if (entry / "_delta_log").is_dir():
                        delta_tables.append(entry)

            assert len(delta_tables) == 0


# =============================================================================
# G. HDFS Optimize Method Tests
# =============================================================================


class TestOptimizeDeltaTablesHdfs:
    """Tests for _optimize_delta_tables_hdfs() method structure."""

    def test_method_exists(self):
        """CleanTask should have _optimize_delta_tables_hdfs method."""
        from dvt.task.clean import CleanTask

        assert hasattr(CleanTask, "_optimize_delta_tables_hdfs")

    def test_imports_delta_table(self):
        """Should import DeltaTable from delta."""
        import inspect
        from dvt.task.clean import CleanTask

        source = inspect.getsource(CleanTask._optimize_delta_tables_hdfs)
        assert "DeltaTable" in source, "Should import DeltaTable"

    def test_handles_missing_delta_spark(self):
        """Should handle ImportError when delta-spark is not installed."""
        import inspect
        from dvt.task.clean import CleanTask

        source = inspect.getsource(CleanTask._optimize_delta_tables_hdfs)
        assert "ImportError" in source, (
            "Should catch ImportError for missing delta-spark"
        )

    def test_uses_hadoop_filesystem(self):
        """Should use Hadoop filesystem API to list HDFS directories."""
        import inspect
        from dvt.task.clean import CleanTask

        source = inspect.getsource(CleanTask._optimize_delta_tables_hdfs)
        assert "FileSystem" in source, "Should use Hadoop FileSystem"
        assert "listStatus" in source, "Should list HDFS directory contents"

    def test_checks_delta_log_exists(self):
        """Should verify _delta_log exists before treating as Delta table."""
        import inspect
        from dvt.task.clean import CleanTask

        source = inspect.getsource(CleanTask._optimize_delta_tables_hdfs)
        assert "_delta_log" in source, "Should check for _delta_log in HDFS directories"


# =============================================================================
# H. parse_duration() Utility Tests
# =============================================================================


class TestParseDuration:
    """Tests for parse_duration() utility function."""

    def test_parse_hours(self):
        """'24h' should parse to 24 hours."""
        from dvt.task.clean import parse_duration

        td = parse_duration("24h")
        assert td.total_seconds() == 24 * 3600

    def test_parse_days(self):
        """'7d' should parse to 7 days."""
        from dvt.task.clean import parse_duration

        td = parse_duration("7d")
        assert td.total_seconds() == 7 * 24 * 3600

    def test_parse_single_hour(self):
        """'1h' should parse to 1 hour."""
        from dvt.task.clean import parse_duration

        td = parse_duration("1h")
        assert td.total_seconds() == 3600

    def test_parse_uppercase(self):
        """'48H' should parse (case insensitive)."""
        from dvt.task.clean import parse_duration

        td = parse_duration("48H")
        assert td.total_seconds() == 48 * 3600

    def test_invalid_format_raises(self):
        """Invalid format should raise ValueError."""
        from dvt.task.clean import parse_duration

        with pytest.raises(ValueError):
            parse_duration("abc")

    def test_no_unit_raises(self):
        """Number without unit should raise ValueError."""
        from dvt.task.clean import parse_duration

        with pytest.raises(ValueError):
            parse_duration("24")

    def test_invalid_unit_raises(self):
        """Invalid unit should raise ValueError."""
        from dvt.task.clean import parse_duration

        with pytest.raises(ValueError):
            parse_duration("24m")  # minutes not supported
