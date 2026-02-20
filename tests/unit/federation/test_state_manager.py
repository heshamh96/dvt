# coding=utf-8
"""Unit tests for DVT federation state manager.

Tests the StateManager class that handles extraction state persistence,
row-level change detection, and federation run statistics.
"""

import json
import tempfile
from datetime import datetime
from pathlib import Path

import pytest

from dvt.federation.state_manager import (
    ExtractionStats,
    FederationRunStats,
    ModelFederationState,
    SourceState,
    StateManager,
    compute_row_hash,
    compute_schema_hash,
)


# =============================================================================
# Helper Functions Tests
# =============================================================================


class TestComputeRowHash:
    """Tests for compute_row_hash function."""

    def test_basic_hash(self):
        """Should compute consistent hash for simple row."""
        row = {"id": 1, "name": "test", "value": 100}
        columns = ["id", "name", "value"]

        hash1 = compute_row_hash(row, columns)
        hash2 = compute_row_hash(row, columns)

        assert hash1 == hash2
        assert len(hash1) == 32  # MD5 hex length

    def test_column_order_independent(self):
        """Hash should be same regardless of column list order."""
        row = {"id": 1, "name": "test", "value": 100}

        hash1 = compute_row_hash(row, ["id", "name", "value"])
        hash2 = compute_row_hash(row, ["value", "name", "id"])

        # Columns are sorted internally
        assert hash1 == hash2

    def test_different_values_different_hash(self):
        """Different values should produce different hashes."""
        row1 = {"id": 1, "name": "test"}
        row2 = {"id": 1, "name": "other"}
        columns = ["id", "name"]

        hash1 = compute_row_hash(row1, columns)
        hash2 = compute_row_hash(row2, columns)

        assert hash1 != hash2

    def test_missing_column_handled(self):
        """Should handle missing columns gracefully."""
        row = {"id": 1}
        columns = ["id", "name"]

        # Should not raise, missing column becomes empty string
        result = compute_row_hash(row, columns)
        assert len(result) == 32


class TestComputeSchemaHash:
    """Tests for compute_schema_hash function."""

    def test_basic_schema_hash(self):
        """Should compute consistent hash for column list."""
        columns = ["id", "name", "value"]

        hash1 = compute_schema_hash(columns)
        hash2 = compute_schema_hash(columns)

        assert hash1 == hash2
        assert len(hash1) == 32

    def test_column_order_independent(self):
        """Schema hash should be same regardless of column order."""
        columns1 = ["id", "name", "value"]
        columns2 = ["value", "name", "id"]

        hash1 = compute_schema_hash(columns1)
        hash2 = compute_schema_hash(columns2)

        assert hash1 == hash2

    def test_different_columns_different_hash(self):
        """Different columns should produce different hashes."""
        columns1 = ["id", "name"]
        columns2 = ["id", "email"]

        hash1 = compute_schema_hash(columns1)
        hash2 = compute_schema_hash(columns2)

        assert hash1 != hash2

    def test_with_types(self):
        """Should include column types in hash when provided."""
        columns = ["id", "name"]
        types1 = {"id": "INTEGER", "name": "VARCHAR"}
        types2 = {"id": "BIGINT", "name": "VARCHAR"}

        hash_no_types = compute_schema_hash(columns)
        hash_with_types1 = compute_schema_hash(columns, types1)
        hash_with_types2 = compute_schema_hash(columns, types2)

        assert hash_no_types != hash_with_types1
        assert hash_with_types1 != hash_with_types2


# =============================================================================
# Dataclass Tests
# =============================================================================


class TestSourceState:
    """Tests for SourceState dataclass."""

    def test_to_dict(self):
        """Should serialize to dictionary."""
        state = SourceState(
            source_name="postgres",
            table_name="orders",
            schema_hash="abc123",
            row_count=1000,
            last_extracted_at="2024-01-01T00:00:00",
            extraction_method="full",
            pk_columns=["id"],
            columns=["id", "name", "value"],
        )

        result = state.to_dict()

        assert result["source_name"] == "postgres"
        assert result["table_name"] == "orders"
        assert result["row_count"] == 1000
        assert result["columns"] == ["id", "name", "value"]

    def test_from_dict(self):
        """Should deserialize from dictionary."""
        data = {
            "source_name": "postgres",
            "table_name": "orders",
            "schema_hash": "abc123",
            "row_count": 1000,
            "last_extracted_at": "2024-01-01T00:00:00",
            "extraction_method": "full",
            "pk_columns": ["id"],
            "columns": ["id", "name", "value"],
        }

        state = SourceState.from_dict(data)

        assert state.source_name == "postgres"
        assert state.row_count == 1000


class TestExtractionStats:
    """Tests for ExtractionStats dataclass."""

    def test_to_dict(self):
        """Should serialize to dictionary."""
        stats = ExtractionStats(
            source_id="postgres__orders",
            extraction_method="incremental",
            row_count=1000,
            new_rows=50,
            changed_rows=10,
            deleted_rows=5,
            elapsed_seconds=2.5,
            started_at="2024-01-01T00:00:00",
            completed_at="2024-01-01T00:00:02",
        )

        result = stats.to_dict()

        assert result["source_id"] == "postgres__orders"
        assert result["new_rows"] == 50
        assert result["elapsed_seconds"] == 2.5


class TestModelFederationState:
    """Tests for ModelFederationState dataclass."""

    def test_to_dict(self):
        """Should serialize to dictionary."""
        state = ModelFederationState(
            model_unique_id="model.my_project.my_model",
            target="snowflake",
            compute="spark_local",
            bucket="s3://my-bucket",
            sources_used=["postgres__orders", "mysql__products"],
            execution_path="spark_federation",
            last_run_at="2024-01-01T00:00:00",
            last_run_status="success",
            elapsed_seconds=15.5,
        )

        result = state.to_dict()

        assert result["model_unique_id"] == "model.my_project.my_model"
        assert result["sources_used"] == ["postgres__orders", "mysql__products"]

    def test_from_dict(self):
        """Should deserialize from dictionary."""
        data = {
            "model_unique_id": "model.my_project.my_model",
            "target": "snowflake",
            "compute": "spark_local",
            "bucket": "s3://my-bucket",
            "sources_used": ["postgres__orders"],
            "execution_path": "spark_federation",
            "last_run_at": "2024-01-01T00:00:00",
            "last_run_status": "success",
            "elapsed_seconds": 15.5,
            "error": None,
        }

        state = ModelFederationState.from_dict(data)

        assert state.target == "snowflake"
        assert state.elapsed_seconds == 15.5


class TestFederationRunStats:
    """Tests for FederationRunStats dataclass."""

    def test_to_dict_with_nested_stats(self):
        """Should serialize including nested stats lists."""
        extraction_stat = ExtractionStats(
            source_id="postgres__orders",
            extraction_method="full",
            row_count=1000,
            new_rows=0,
            changed_rows=0,
            deleted_rows=0,
            elapsed_seconds=2.0,
            started_at="2024-01-01T00:00:00",
            completed_at="2024-01-01T00:00:02",
        )

        stats = FederationRunStats(
            run_id="run_123",
            started_at="2024-01-01T00:00:00",
            completed_at="2024-01-01T00:01:00",
            total_models=10,
            federation_models=3,
            pushdown_models=7,
            sources_extracted=5,
            total_rows_extracted=10000,
            errors=0,
            extraction_stats=[extraction_stat],
        )

        result = stats.to_dict()

        assert result["run_id"] == "run_123"
        assert result["federation_models"] == 3
        assert len(result["extraction_stats"]) == 1


# =============================================================================
# StateManager Tests
# =============================================================================


class TestStateManager:
    """Tests for StateManager class."""

    @pytest.fixture
    def temp_bucket(self):
        """Create a temporary bucket directory."""
        with tempfile.TemporaryDirectory() as tmpdir:
            yield Path(tmpdir)

    @pytest.fixture
    def state_manager(self, temp_bucket):
        """Create a StateManager with temp bucket."""
        return StateManager(temp_bucket)

    # -------------------------------------------------------------------------
    # Source State Tests
    # -------------------------------------------------------------------------

    def test_save_and_get_source_state(self, state_manager):
        """Should save and retrieve source state."""
        state = SourceState(
            source_name="postgres__orders",
            table_name="orders",
            schema_hash="abc123",
            row_count=1000,
            last_extracted_at="2024-01-01T00:00:00",
            extraction_method="full",
            pk_columns=["id"],
            columns=["id", "name"],
        )

        state_manager.save_source_state(state)
        retrieved = state_manager.get_source_state("postgres__orders")

        assert retrieved is not None
        assert retrieved.source_name == "postgres__orders"
        assert retrieved.row_count == 1000

    def test_get_nonexistent_source_state(self, state_manager):
        """Should return None for nonexistent source."""
        result = state_manager.get_source_state("nonexistent")
        assert result is None

    def test_clear_source_state(self, state_manager):
        """Should clear state for a source."""
        state = SourceState(
            source_name="postgres__orders",
            table_name="orders",
            schema_hash="abc123",
            row_count=1000,
            last_extracted_at="2024-01-01T00:00:00",
            extraction_method="full",
            pk_columns=["id"],
            columns=["id"],
        )

        state_manager.save_source_state(state)
        state_manager.clear_source_state("postgres__orders")

        result = state_manager.get_source_state("postgres__orders")
        assert result is None

    # -------------------------------------------------------------------------
    # Staging Tests
    # -------------------------------------------------------------------------

    def test_staging_exists_false(self, state_manager):
        """Should return False when staging doesn't exist."""
        result = state_manager.staging_exists("postgres__orders")
        assert result is False

    def test_staging_exists_true_legacy_parquet(self, state_manager, temp_bucket):
        """Should return True when legacy Parquet staging exists."""
        staging_file = temp_bucket / "postgres__orders.parquet"
        staging_file.touch()

        result = state_manager.staging_exists("postgres__orders")
        assert result is True

    def test_staging_exists_true_delta(self, state_manager, temp_bucket):
        """Should return True when Delta staging directory exists."""
        delta_dir = temp_bucket / "postgres__orders.delta"
        delta_log = delta_dir / "_delta_log"
        delta_log.mkdir(parents=True)

        result = state_manager.staging_exists("postgres__orders")
        assert result is True

    def test_staging_exists_false_empty_delta_dir(self, state_manager, temp_bucket):
        """Should return False when Delta directory exists but has no _delta_log."""
        delta_dir = temp_bucket / "postgres__orders.delta"
        delta_dir.mkdir()

        result = state_manager.staging_exists("postgres__orders")
        assert result is False

    def test_staging_exists_true_parquet_directory(self, state_manager, temp_bucket):
        """Should return True for legacy Parquet directory (JDBC extraction)."""
        parquet_dir = temp_bucket / "postgres__orders.parquet"
        parquet_dir.mkdir()
        # Simulate part files
        (parquet_dir / "part-00000.parquet").touch()

        result = state_manager.staging_exists("postgres__orders")
        assert result is True

    def test_get_staging_path_no_existing(self, state_manager, temp_bucket):
        """Should return Delta path when nothing exists (new extraction)."""
        path = state_manager.get_staging_path("postgres__orders")
        assert path == temp_bucket / "postgres__orders.delta"

    def test_get_staging_path_legacy_parquet(self, state_manager, temp_bucket):
        """Should return Parquet path when legacy Parquet exists."""
        staging_file = temp_bucket / "postgres__orders.parquet"
        staging_file.touch()

        path = state_manager.get_staging_path("postgres__orders")
        assert path == temp_bucket / "postgres__orders.parquet"

    def test_get_staging_path_delta_preferred(self, state_manager, temp_bucket):
        """Should return Delta path when Delta staging exists (even if Parquet also exists)."""
        # Create both legacy Parquet and Delta
        (temp_bucket / "postgres__orders.parquet").touch()
        delta_dir = temp_bucket / "postgres__orders.delta"
        (delta_dir / "_delta_log").mkdir(parents=True)

        path = state_manager.get_staging_path("postgres__orders")
        assert path == temp_bucket / "postgres__orders.delta"

    def test_clear_staging_legacy_parquet(self, state_manager, temp_bucket):
        """Should clear legacy Parquet staging file."""
        staging_file = temp_bucket / "postgres__orders.parquet"
        staging_file.touch()

        state_manager.clear_staging("postgres__orders")

        assert not staging_file.exists()

    def test_clear_staging_delta(self, state_manager, temp_bucket):
        """Should clear Delta staging directory."""
        delta_dir = temp_bucket / "postgres__orders.delta"
        delta_log = delta_dir / "_delta_log"
        delta_log.mkdir(parents=True)
        (delta_dir / "part-00000.parquet").touch()

        state_manager.clear_staging("postgres__orders")

        assert not delta_dir.exists()

    def test_clear_staging_both(self, state_manager, temp_bucket):
        """Should clear both Delta and legacy Parquet staging."""
        (temp_bucket / "postgres__orders.parquet").touch()
        delta_dir = temp_bucket / "postgres__orders.delta"
        (delta_dir / "_delta_log").mkdir(parents=True)

        state_manager.clear_staging("postgres__orders")

        assert not (temp_bucket / "postgres__orders.parquet").exists()
        assert not delta_dir.exists()

    # -------------------------------------------------------------------------
    # Should Extract Tests
    # -------------------------------------------------------------------------

    def test_should_extract_full_refresh(self, state_manager):
        """Should extract if full_refresh is True."""
        should, reason = state_manager.should_extract(
            "postgres__orders", "abc123", full_refresh=True
        )

        assert should is True
        assert reason == "full_refresh"

    def test_should_extract_no_staging(self, state_manager):
        """Should extract if no staging file exists."""
        should, reason = state_manager.should_extract(
            "postgres__orders", "abc123", full_refresh=False
        )

        assert should is True
        assert reason == "no_staging"

    def test_should_extract_schema_changed(self, state_manager, temp_bucket):
        """Should extract if schema changed."""
        # Create staging file
        staging_file = temp_bucket / "postgres__orders.parquet"
        staging_file.touch()

        # Create state with old schema hash
        state = SourceState(
            source_name="postgres__orders",
            table_name="orders",
            schema_hash="old_hash",
            row_count=1000,
            last_extracted_at="2024-01-01T00:00:00",
            extraction_method="full",
            pk_columns=["id"],
            columns=["id"],
        )
        state_manager.save_source_state(state)

        should, reason = state_manager.should_extract(
            "postgres__orders", "new_hash", full_refresh=False
        )

        assert should is True
        assert reason == "schema_changed"

    def test_should_not_extract_cached(self, state_manager, temp_bucket):
        """Should skip if staging exists and schema unchanged."""
        staging_file = temp_bucket / "postgres__orders.parquet"
        staging_file.touch()

        state = SourceState(
            source_name="postgres__orders",
            table_name="orders",
            schema_hash="abc123",
            row_count=1000,
            last_extracted_at="2024-01-01T00:00:00",
            extraction_method="full",
            pk_columns=["id"],
            columns=["id"],
        )
        state_manager.save_source_state(state)

        should, reason = state_manager.should_extract(
            "postgres__orders", "abc123", full_refresh=False
        )

        assert should is False
        assert reason == "skip"

    # -------------------------------------------------------------------------
    # Changed PKs Tests
    # -------------------------------------------------------------------------

    def test_get_changed_pks_all_new(self, state_manager):
        """Should report all as new if no stored hashes."""
        current = {"pk1": "hash1", "pk2": "hash2"}

        new, changed, deleted = state_manager.get_changed_pks("test_source", current)

        assert set(new) == {"pk1", "pk2"}
        assert changed == []
        assert deleted == []

    # -------------------------------------------------------------------------
    # Model State Tests
    # -------------------------------------------------------------------------

    def test_save_and_get_model_state(self, state_manager):
        """Should save and retrieve model federation state."""
        state = ModelFederationState(
            model_unique_id="model.my_project.my_model",
            target="snowflake",
            compute="spark_local",
            bucket="s3://bucket",
            sources_used=["postgres__orders"],
            execution_path="spark_federation",
            last_run_at="2024-01-01T00:00:00",
            last_run_status="success",
            elapsed_seconds=10.0,
        )

        state_manager.save_model_state(state)
        retrieved = state_manager.get_model_state("model.my_project.my_model")

        assert retrieved is not None
        assert retrieved.target == "snowflake"
        assert retrieved.elapsed_seconds == 10.0

    def test_get_nonexistent_model_state(self, state_manager):
        """Should return None for nonexistent model."""
        result = state_manager.get_model_state("model.nonexistent")
        assert result is None

    def test_clear_model_state(self, state_manager):
        """Should clear model state."""
        state = ModelFederationState(
            model_unique_id="model.my_project.my_model",
            target="snowflake",
            compute="spark_local",
            bucket="s3://bucket",
            sources_used=[],
            execution_path="spark_federation",
            last_run_at="2024-01-01T00:00:00",
            last_run_status="success",
            elapsed_seconds=10.0,
        )

        state_manager.save_model_state(state)
        state_manager.clear_model_state("model.my_project.my_model")

        result = state_manager.get_model_state("model.my_project.my_model")
        assert result is None

    # -------------------------------------------------------------------------
    # Run Stats Tests
    # -------------------------------------------------------------------------

    def test_save_and_get_run_stats(self, state_manager):
        """Should save and retrieve run statistics."""
        stats = FederationRunStats(
            run_id="run_123",
            started_at="2024-01-01T00:00:00",
            completed_at="2024-01-01T00:01:00",
            total_models=10,
            federation_models=3,
            pushdown_models=7,
            sources_extracted=5,
            total_rows_extracted=10000,
            errors=0,
        )

        state_manager.save_run_stats(stats)
        retrieved = state_manager.get_run_stats("run_123")

        assert retrieved is not None
        assert retrieved.run_id == "run_123"
        assert retrieved.federation_models == 3

    def test_get_latest_run_id(self, state_manager):
        """Should get most recent run ID."""
        for i in range(3):
            stats = FederationRunStats(
                run_id=f"run_{i}",
                started_at="2024-01-01T00:00:00",
                completed_at="2024-01-01T00:01:00",
                total_models=10,
                federation_models=3,
                pushdown_models=7,
                sources_extracted=5,
                total_rows_extracted=10000,
                errors=0,
            )
            state_manager.save_run_stats(stats)

        latest = state_manager.get_latest_run_id()

        # Most recently saved should be run_2
        assert latest == "run_2"

    def test_list_run_ids(self, state_manager):
        """Should list recent run IDs."""
        for i in range(5):
            stats = FederationRunStats(
                run_id=f"run_{i}",
                started_at="2024-01-01T00:00:00",
                completed_at="2024-01-01T00:01:00",
                total_models=10,
                federation_models=3,
                pushdown_models=7,
                sources_extracted=5,
                total_rows_extracted=10000,
                errors=0,
            )
            state_manager.save_run_stats(stats)

        run_ids = state_manager.list_run_ids(limit=3)

        assert len(run_ids) == 3
        # Most recent first
        assert run_ids[0] == "run_4"

    def test_clear_old_runs(self, state_manager):
        """Should clear old run stats, keeping recent ones."""
        for i in range(5):
            stats = FederationRunStats(
                run_id=f"run_{i}",
                started_at="2024-01-01T00:00:00",
                completed_at="2024-01-01T00:01:00",
                total_models=10,
                federation_models=3,
                pushdown_models=7,
                sources_extracted=5,
                total_rows_extracted=10000,
                errors=0,
            )
            state_manager.save_run_stats(stats)

        deleted = state_manager.clear_old_runs(keep_count=2)

        assert deleted == 3
        remaining = state_manager.list_run_ids()
        assert len(remaining) == 2

    # -------------------------------------------------------------------------
    # Helper Method Tests
    # -------------------------------------------------------------------------

    def test_record_extraction(self, state_manager):
        """Should create ExtractionStats record."""
        stats = state_manager.record_extraction(
            source_id="postgres__orders",
            method="incremental",
            row_count=1000,
            new_rows=50,
            changed_rows=10,
            elapsed=2.5,
        )

        assert stats.source_id == "postgres__orders"
        assert stats.extraction_method == "incremental"
        assert stats.new_rows == 50

    def test_get_sources_for_model(self, state_manager):
        """Should return sources used by a model."""
        state = ModelFederationState(
            model_unique_id="model.my_project.my_model",
            target="snowflake",
            compute="spark_local",
            bucket="s3://bucket",
            sources_used=["postgres__orders", "mysql__products"],
            execution_path="spark_federation",
            last_run_at="2024-01-01T00:00:00",
            last_run_status="success",
            elapsed_seconds=10.0,
        )
        state_manager.save_model_state(state)

        sources = state_manager.get_sources_for_model("model.my_project.my_model")

        assert sources == ["postgres__orders", "mysql__products"]

    def test_get_sources_for_nonexistent_model(self, state_manager):
        """Should return empty list for nonexistent model."""
        sources = state_manager.get_sources_for_model("model.nonexistent")
        assert sources == []

    def test_get_extraction_summary(self, state_manager):
        """Should return summary of all extracted sources."""
        state1 = SourceState(
            source_name="postgres__orders",
            table_name="orders",
            schema_hash="abc123",
            row_count=1000,
            last_extracted_at="2024-01-01T00:00:00",
            extraction_method="full",
            pk_columns=["id"],
            columns=["id"],
        )
        state2 = SourceState(
            source_name="mysql__products",
            table_name="products",
            schema_hash="def456",
            row_count=500,
            last_extracted_at="2024-01-02T00:00:00",
            extraction_method="incremental",
            pk_columns=["id"],
            columns=["id"],
        )

        state_manager.save_source_state(state1)
        state_manager.save_source_state(state2)

        summary = state_manager.get_extraction_summary()

        assert len(summary) == 2
        assert "postgres__orders" in summary
        assert summary["postgres__orders"]["row_count"] == 1000

    def test_clear_all_state(self, state_manager, temp_bucket):
        """Should clear all state files."""
        # Create some state
        state = SourceState(
            source_name="postgres__orders",
            table_name="orders",
            schema_hash="abc123",
            row_count=1000,
            last_extracted_at="2024-01-01T00:00:00",
            extraction_method="full",
            pk_columns=["id"],
            columns=["id"],
        )
        state_manager.save_source_state(state)

        # Clear all
        state_manager.clear_all_state()

        # State directory should exist but be empty
        state_path = temp_bucket / "_state"
        assert state_path.exists()
        assert list(state_path.iterdir()) == []

    # -------------------------------------------------------------------------
    # clear_all_source_staging — clears source.* but keeps model.*
    # -------------------------------------------------------------------------

    def test_clear_all_source_staging_removes_source_deltas(
        self, state_manager, temp_bucket
    ):
        """Source staging directories should be removed."""
        src = temp_bucket / "source.project.schema.table.delta"
        src.mkdir(parents=True)
        (src / "_delta_log").mkdir()
        (src / "part-00000.parquet").touch()

        state_manager.clear_all_source_staging()

        assert not src.exists()

    def test_clear_all_source_staging_preserves_model_deltas(
        self, state_manager, temp_bucket
    ):
        """Model staging directories must survive source clearing."""
        mdl = temp_bucket / "model.project.my_model.delta"
        mdl.mkdir(parents=True)
        (mdl / "_delta_log").mkdir()
        (mdl / "part-00000.parquet").touch()

        state_manager.clear_all_source_staging()

        assert mdl.exists()
        assert (mdl / "part-00000.parquet").exists()

    def test_clear_all_source_staging_removes_source_state_files(
        self, state_manager, temp_bucket
    ):
        """Source state JSON files should be removed."""
        state_dir = temp_bucket / "_state"
        state_dir.mkdir(parents=True, exist_ok=True)
        src_state = state_dir / "source.project.schema.table.state.json"
        src_state.write_text("{}")

        state_manager.clear_all_source_staging()

        assert not src_state.exists()

    def test_clear_all_source_staging_preserves_model_state_files(
        self, state_manager, temp_bucket
    ):
        """Model state files must survive source clearing."""
        state_dir = temp_bucket / "_state"
        state_dir.mkdir(parents=True, exist_ok=True)
        mdl_state = state_dir / "model.project.my_model.state.json"
        mdl_state.write_text("{}")

        state_manager.clear_all_source_staging()

        assert mdl_state.exists()

    def test_clear_all_source_staging_handles_multiple_sources(
        self, state_manager, temp_bucket
    ):
        """All source entries cleared, all model entries preserved."""
        # Create two sources and two models
        for name in [
            "source.proj.pg.orders.delta",
            "source.proj.sf.accounts.delta",
        ]:
            d = temp_bucket / name
            d.mkdir(parents=True)
            (d / "_delta_log").mkdir()

        for name in [
            "model.proj.my_model.delta",
            "model.proj.incr_model.delta",
        ]:
            d = temp_bucket / name
            d.mkdir(parents=True)
            (d / "_delta_log").mkdir()

        state_manager.clear_all_source_staging()

        remaining = [e.name for e in temp_bucket.iterdir() if e.name != "_state"]
        assert "source.proj.pg.orders.delta" not in remaining
        assert "source.proj.sf.accounts.delta" not in remaining
        assert "model.proj.my_model.delta" in remaining
        assert "model.proj.incr_model.delta" in remaining
