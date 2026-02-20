# coding=utf-8
"""Unit tests for Bug 4 fix: staging cache column collision.

Bug 4: When multiple models reference the same source with different column
subsets (via optimizer column pushdown), the first extraction writes only
its subset to Delta staging, but the state recorded ALL source columns.
The second model's union-of-columns check would incorrectly conclude that
all columns were already extracted and skip re-extraction, leading to
missing columns at query time.

Fix: Save the actually-extracted columns (from resolved_columns) to
SourceState, not the full source table schema from get_columns().
Also: case-insensitive comparison in the union-of-columns check, since
sqlglot may lowercase column names while state stores real DB casing.
"""

from unittest.mock import MagicMock, patch

import pytest

from dvt.federation.el_layer import ELLayer, SourceConfig, _resolve_column_names
from dvt.federation.extractors import ExtractionResult
from dvt.federation.state_manager import SourceState


# =============================================================================
# _resolve_column_names tests
# =============================================================================


class TestResolveColumnNames:
    """Tests for case-insensitive column name resolution."""

    def test_none_returns_none(self):
        """None optimizer columns (SELECT *) returns None."""
        assert _resolve_column_names(None, [{"name": "Col_A", "type": "text"}]) is None

    def test_empty_list_returns_none(self):
        """Empty optimizer columns returns None."""
        assert _resolve_column_names([], [{"name": "Col_A", "type": "text"}]) is None

    def test_exact_match(self):
        """Exact case match resolves correctly."""
        real = [{"name": "Col_A", "type": "text"}, {"name": "Col_B", "type": "int"}]
        assert _resolve_column_names(["Col_A", "Col_B"], real) == ["Col_A", "Col_B"]

    def test_case_insensitive_match(self):
        """Lowercased optimizer names resolve to real-cased DB names."""
        real = [
            {"name": "Country_Name", "type": "text"},
            {"name": "Geographical_Block", "type": "text"},
        ]
        result = _resolve_column_names(["country_name", "geographical_block"], real)
        assert result == ["Country_Name", "Geographical_Block"]

    def test_mixed_case_match(self):
        """Mix of exact and lowered names resolves correctly."""
        real = [
            {"name": "ID", "type": "int"},
            {"name": "Country_Name", "type": "text"},
            {"name": "region", "type": "text"},
        ]
        result = _resolve_column_names(["id", "Country_Name", "region"], real)
        assert result == ["ID", "Country_Name", "region"]

    def test_missing_column_skipped(self):
        """Column not in source schema is skipped (defensive)."""
        real = [{"name": "Col_A", "type": "text"}]
        result = _resolve_column_names(["col_a", "nonexistent"], real)
        assert result == ["Col_A"]

    def test_all_missing_returns_none(self):
        """If no columns match, returns None (falls back to SELECT *)."""
        real = [{"name": "Col_A", "type": "text"}]
        result = _resolve_column_names(["nonexistent"], real)
        assert result is None


# =============================================================================
# SourceState.columns tracks actually-extracted columns (Bug 4 core fix)
# =============================================================================


class TestStateColumnsTracking:
    """Verify that state saves actually-extracted columns, not full schema.

    This is the core fix for Bug 4: the state must reflect what's in Delta
    staging so the union-of-columns check works correctly.
    """

    def _make_el_layer(self, tmp_path):
        """Create an ELLayer with a temp bucket path."""
        return ELLayer(
            bucket_path=tmp_path / "staging",
            profile_name="test",
        )

    def _make_source(self, columns=None):
        """Create a SourceConfig with optional column pushdown."""
        return SourceConfig(
            source_name="sf__cbs_f_country",
            adapter_type="snowflake",
            schema="CBS",
            table="F_COUNTRY",
            connection=None,
            connection_config={"type": "snowflake", "account": "test"},
            columns=columns,
        )

    def _mock_extractor(self, all_columns):
        """Create a mock extractor that returns given column metadata."""
        extractor = MagicMock()
        extractor.get_columns.return_value = [
            {"name": c, "type": "VARCHAR"} for c in all_columns
        ]
        extractor.detect_primary_key.return_value = []
        extractor.extract.return_value = ExtractionResult(
            success=True,
            source_name="sf__cbs_f_country",
            row_count=100,
            extraction_method="jdbc",
        )
        extractor.extract_hashes.return_value = {}
        return extractor

    @patch("dvt.federation.el_layer.get_extractor")
    def test_state_records_extracted_columns_not_full_schema(
        self, mock_get_extractor, tmp_path
    ):
        """State.columns should be the pushed-down subset, not all source columns."""
        all_source_cols = [
            "Country_ID",
            "Country_Name",
            "Geographical_Block",
            "Population",
            "GDP",
        ]
        mock_get_extractor.return_value = self._mock_extractor(all_source_cols)

        el = self._make_el_layer(tmp_path)
        # Model A only needs 2 columns
        source = self._make_source(columns=["country_id", "country_name"])

        with patch.object(el, "_convert_to_delta") as mock_convert:
            mock_convert.return_value = ExtractionResult(
                success=True,
                source_name="sf__cbs_f_country",
                row_count=100,
                extraction_method="jdbc",
            )
            el._extract_source_locked(source, full_refresh=False)

        # Check saved state
        state = el.state_manager.get_source_state("sf__cbs_f_country")
        assert state is not None
        # State should have the resolved (real-cased) columns, NOT all 5
        assert set(state.columns) == {"Country_ID", "Country_Name"}

    @patch("dvt.federation.el_layer.get_extractor")
    def test_state_records_all_columns_for_select_star(
        self, mock_get_extractor, tmp_path
    ):
        """When no column pushdown (SELECT *), state should have all columns."""
        all_source_cols = ["Country_ID", "Country_Name", "Geographical_Block"]
        mock_get_extractor.return_value = self._mock_extractor(all_source_cols)

        el = self._make_el_layer(tmp_path)
        source = self._make_source(columns=None)  # SELECT *

        with patch.object(el, "_convert_to_delta") as mock_convert:
            mock_convert.return_value = ExtractionResult(
                success=True,
                source_name="sf__cbs_f_country",
                row_count=100,
                extraction_method="jdbc",
            )
            el._extract_source_locked(source, full_refresh=False)

        state = el.state_manager.get_source_state("sf__cbs_f_country")
        assert state is not None
        assert set(state.columns) == {
            "Country_ID",
            "Country_Name",
            "Geographical_Block",
        }


# =============================================================================
# Union-of-columns triggers re-extraction correctly (Bug 4 scenario)
# =============================================================================


class TestUnionOfColumnsReExtraction:
    """Test the union-of-columns check triggers re-extraction when needed.

    Scenario: Model A extracts [a, b] from source X. Model B needs [b, c].
    On Model B's extraction, the union-of-columns check should detect that
    column 'c' is missing and trigger re-extraction with [a, b, c].
    """

    def _make_el_layer(self, tmp_path):
        return ELLayer(bucket_path=tmp_path / "staging", profile_name="test")

    def _seed_state(self, el, columns):
        """Seed a SourceState to simulate a previous extraction."""
        from dvt.federation.state_manager import SourceState, compute_schema_hash

        # The state columns represent what's actually in Delta staging
        state = SourceState(
            source_name="sf__cbs_f_country",
            table_name="F_COUNTRY",
            schema_hash="abc123",
            row_count=100,
            last_extracted_at="2026-01-01T00:00:00",
            extraction_method="jdbc",
            pk_columns=[],
            columns=columns,
        )
        el.state_manager.save_source_state(state)
        # Create a fake Delta staging directory with _delta_log/ so
        # staging_exists() returns True (it checks for .delta/ + _delta_log/).
        delta_path = el.state_manager.bucket_path / "sf__cbs_f_country.delta"
        (delta_path / "_delta_log").mkdir(parents=True, exist_ok=True)

    def _mock_extractor(self, all_columns):
        extractor = MagicMock()
        extractor.get_columns.return_value = [
            {"name": c, "type": "VARCHAR"} for c in all_columns
        ]
        extractor.detect_primary_key.return_value = []
        extractor.extract.return_value = ExtractionResult(
            success=True,
            source_name="sf__cbs_f_country",
            row_count=100,
            extraction_method="jdbc",
        )
        extractor.extract_hashes.return_value = {}
        return extractor

    @patch("dvt.federation.el_layer.get_extractor")
    def test_subset_columns_skip_extraction(self, mock_get_extractor, tmp_path):
        """If requested columns are a subset of extracted, skip extraction."""
        el = self._make_el_layer(tmp_path)
        # Previous extraction had [Country_ID, Country_Name, Geographical_Block]
        self._seed_state(el, ["Country_ID", "Country_Name", "Geographical_Block"])

        all_cols = ["Country_ID", "Country_Name", "Geographical_Block", "Population"]
        mock_get_extractor.return_value = self._mock_extractor(all_cols)

        source = SourceConfig(
            source_name="sf__cbs_f_country",
            adapter_type="snowflake",
            schema="CBS",
            table="F_COUNTRY",
            connection=None,
            connection_config={"type": "snowflake"},
            columns=["country_id", "country_name"],  # Subset (lowercased by optimizer)
        )

        # Need to mock compute_schema_hash to return same hash as seeded state
        with patch(
            "dvt.federation.el_layer.compute_schema_hash", return_value="abc123"
        ):
            result = el._extract_source_locked(source, full_refresh=False)

        assert result.extraction_method == "skip"

    @patch("dvt.federation.el_layer.get_extractor")
    def test_new_columns_trigger_re_extraction(self, mock_get_extractor, tmp_path):
        """If requested columns include new ones, re-extract with union."""
        el = self._make_el_layer(tmp_path)
        # Previous extraction had only [Country_ID, Country_Name]
        self._seed_state(el, ["Country_ID", "Country_Name"])

        all_cols = ["Country_ID", "Country_Name", "Geographical_Block", "Population"]
        mock_get_extractor.return_value = self._mock_extractor(all_cols)

        source = SourceConfig(
            source_name="sf__cbs_f_country",
            adapter_type="snowflake",
            schema="CBS",
            table="F_COUNTRY",
            connection=None,
            connection_config={"type": "snowflake"},
            # Model B needs Geographical_Block which wasn't extracted before
            columns=["country_name", "geographical_block"],
        )

        with patch(
            "dvt.federation.el_layer.compute_schema_hash", return_value="abc123"
        ):
            with patch.object(el, "_convert_to_delta") as mock_convert:
                mock_convert.return_value = ExtractionResult(
                    success=True,
                    source_name="sf__cbs_f_country",
                    row_count=100,
                    extraction_method="jdbc",
                )
                result = el._extract_source_locked(source, full_refresh=False)

        # Should have extracted (not skipped)
        assert result.extraction_method == "jdbc"
        assert result.row_count == 100

        # Check state now has the union of columns
        state = el.state_manager.get_source_state("sf__cbs_f_country")
        assert state is not None
        # Should have all 3: Country_ID, Country_Name, Geographical_Block
        state_cols_lower = {c.lower() for c in state.columns}
        assert "country_id" in state_cols_lower
        assert "country_name" in state_cols_lower
        assert "geographical_block" in state_cols_lower

    @patch("dvt.federation.el_layer.get_extractor")
    def test_case_insensitive_subset_check(self, mock_get_extractor, tmp_path):
        """Case difference between optimizer and DB names should not trigger re-extract."""
        el = self._make_el_layer(tmp_path)
        # State has DB-cased names
        self._seed_state(el, ["Country_ID", "Country_Name"])

        all_cols = ["Country_ID", "Country_Name", "Geographical_Block"]
        mock_get_extractor.return_value = self._mock_extractor(all_cols)

        source = SourceConfig(
            source_name="sf__cbs_f_country",
            adapter_type="snowflake",
            schema="CBS",
            table="F_COUNTRY",
            connection=None,
            connection_config={"type": "snowflake"},
            # Optimizer lowercased the names — same columns, different case
            columns=["country_id", "country_name"],
        )

        with patch(
            "dvt.federation.el_layer.compute_schema_hash", return_value="abc123"
        ):
            result = el._extract_source_locked(source, full_refresh=False)

        # Should skip — same columns despite case difference
        assert result.extraction_method == "skip"


# =============================================================================
# End-to-end Bug 4 scenario (Model A then Model B)
# =============================================================================


class TestBug4EndToEnd:
    """Simulates the exact Bug 4 scenario from the UAT.

    Model A (snowflake_to_pg) needs [Country_ID, Country_Name] from source.
    Model B (snowflake_to_databricks) needs [Country_Name, Geographical_Block].

    Without the fix, after Model A extracts, the state would claim ALL columns
    exist (full schema), and Model B would skip extraction — but Delta staging
    only has [Country_ID, Country_Name], missing Geographical_Block.

    With the fix, the state correctly records only [Country_ID, Country_Name],
    and Model B's union-of-columns check triggers re-extraction with all 3.
    """

    def _mock_extractor(self, all_columns):
        extractor = MagicMock()
        extractor.get_columns.return_value = [
            {"name": c, "type": "VARCHAR"} for c in all_columns
        ]
        extractor.detect_primary_key.return_value = []
        extractor.extract.return_value = ExtractionResult(
            success=True,
            source_name="sf__cbs_f_country",
            row_count=100,
            extraction_method="jdbc",
        )
        extractor.extract_hashes.return_value = {}
        return extractor

    @patch("dvt.federation.el_layer.get_extractor")
    def test_model_a_then_model_b_triggers_re_extraction(
        self, mock_get_extractor, tmp_path
    ):
        """Full Bug 4 scenario: Model A extracts subset, Model B gets union."""
        all_source_cols = [
            "Country_ID",
            "Country_Name",
            "Geographical_Block",
            "Population",
            "GDP",
        ]
        mock_get_extractor.return_value = self._mock_extractor(all_source_cols)

        el = ELLayer(bucket_path=tmp_path / "staging", profile_name="test")

        # Helper: mock _convert_to_delta that also creates the Delta staging
        # directory so staging_exists() returns True for subsequent calls.
        def fake_convert(parquet_path, delta_path, source_name, extraction_result):
            # Create Delta directory structure so staging_exists() works
            (delta_path / "_delta_log").mkdir(parents=True, exist_ok=True)
            return ExtractionResult(
                success=True,
                source_name=source_name,
                row_count=100,
                extraction_method="jdbc",
            )

        # --- Model A: needs [Country_ID, Country_Name] ---
        source_a = SourceConfig(
            source_name="sf__cbs_f_country",
            adapter_type="snowflake",
            schema="CBS",
            table="F_COUNTRY",
            connection=None,
            connection_config={"type": "snowflake"},
            columns=["country_id", "country_name"],
        )

        with patch.object(el, "_convert_to_delta", side_effect=fake_convert):
            result_a = el._extract_source_locked(source_a, full_refresh=False)

        assert result_a.success
        assert result_a.extraction_method == "jdbc"

        # State should record ONLY the extracted columns
        state_after_a = el.state_manager.get_source_state("sf__cbs_f_country")
        assert state_after_a is not None
        state_cols_lower = {c.lower() for c in state_after_a.columns}
        assert state_cols_lower == {"country_id", "country_name"}
        # NOT the full schema (5 cols)!
        assert len(state_after_a.columns) == 2

        # --- Model B: needs [Country_Name, Geographical_Block] ---
        source_b = SourceConfig(
            source_name="sf__cbs_f_country",
            adapter_type="snowflake",
            schema="CBS",
            table="F_COUNTRY",
            connection=None,
            connection_config={"type": "snowflake"},
            columns=["country_name", "geographical_block"],
        )

        with patch.object(el, "_convert_to_delta", side_effect=fake_convert):
            result_b = el._extract_source_locked(source_b, full_refresh=False)

        # Model B should NOT skip — it needs Geographical_Block which wasn't extracted
        assert result_b.extraction_method == "jdbc"

        # State should now have the union of all 3 columns
        state_after_b = el.state_manager.get_source_state("sf__cbs_f_country")
        assert state_after_b is not None
        state_cols_lower = {c.lower() for c in state_after_b.columns}
        assert "country_id" in state_cols_lower
        assert "country_name" in state_cols_lower
        assert "geographical_block" in state_cols_lower
        assert len(state_after_b.columns) == 3
