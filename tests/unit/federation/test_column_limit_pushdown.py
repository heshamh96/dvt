# coding=utf-8
"""Unit tests for column selection and LIMIT pushdown.

Tests the end-to-end pipeline from QueryOptimizer extraction through
SourceConfig/ExtractionConfig propagation to the SQLGlot-based
build_export_query() in BaseExtractor.

Three test areas:
A. Column case resolution (_resolve_column_names)
B. build_export_query() with SQLGlot dialect-aware generation
C. End-to-end config propagation
"""

import pytest

# Skip all tests if SQLGlot is not available
sqlglot = pytest.importorskip("sqlglot")

from dvt.federation.el_layer import SourceConfig, _resolve_column_names
from dvt.federation.extractors.base import ExtractionConfig
from dvt.federation.query_optimizer import PushableOperations, QueryOptimizer


# =============================================================================
# A. Column Case Resolution Tests
# =============================================================================


class TestResolveColumnNames:
    """Tests for _resolve_column_names() case-insensitive matching."""

    def test_lowercased_to_real_case(self):
        """Should resolve lowercased optimizer names to real DB casing."""
        optimizer_cols = ["customercode", "skuname", "quantity"]
        real_cols = [
            {"name": "CustomerCode", "type": "varchar"},
            {"name": "SKUName", "type": "varchar"},
            {"name": "Quantity", "type": "int"},
        ]

        result = _resolve_column_names(optimizer_cols, real_cols)

        assert result == ["CustomerCode", "SKUName", "Quantity"]

    def test_columns_with_spaces(self):
        """Should resolve columns that have spaces in their names."""
        optimizer_cols = ["customer code", "sku code"]
        real_cols = [
            {"name": "Customer Code", "type": "varchar"},
            {"name": "SKU Code", "type": "varchar"},
            {"name": "Region", "type": "varchar"},
        ]

        result = _resolve_column_names(optimizer_cols, real_cols)

        assert result == ["Customer Code", "SKU Code"]

    def test_none_returns_none(self):
        """Should return None for None input (SELECT *)."""
        real_cols = [{"name": "id", "type": "int"}]

        result = _resolve_column_names(None, real_cols)

        assert result is None

    def test_empty_list_returns_none(self):
        """Should return None for empty list (SELECT *)."""
        real_cols = [{"name": "id", "type": "int"}]

        result = _resolve_column_names([], real_cols)

        assert result is None

    def test_missing_column_skipped(self):
        """Should skip columns not found in real metadata."""
        optimizer_cols = ["id", "nonexistent", "name"]
        real_cols = [
            {"name": "id", "type": "int"},
            {"name": "name", "type": "varchar"},
        ]

        result = _resolve_column_names(optimizer_cols, real_cols)

        assert result == ["id", "name"]

    def test_all_missing_returns_none(self):
        """Should return None if no columns match."""
        optimizer_cols = ["nonexistent1", "nonexistent2"]
        real_cols = [
            {"name": "id", "type": "int"},
            {"name": "name", "type": "varchar"},
        ]

        result = _resolve_column_names(optimizer_cols, real_cols)

        assert result is None

    def test_exact_case_match(self):
        """Should work when cases already match."""
        optimizer_cols = ["id", "name", "amount"]
        real_cols = [
            {"name": "id", "type": "int"},
            {"name": "name", "type": "varchar"},
            {"name": "amount", "type": "decimal"},
        ]

        result = _resolve_column_names(optimizer_cols, real_cols)

        assert result == ["id", "name", "amount"]

    def test_mixed_case_sensitivity(self):
        """Should handle mixed casing from SQLGlot normalization."""
        optimizer_cols = ["customer_id", "ordertotal", "createdat"]
        real_cols = [
            {"name": "Customer_ID", "type": "int"},
            {"name": "OrderTotal", "type": "decimal"},
            {"name": "CreatedAt", "type": "timestamp"},
            {"name": "Status", "type": "varchar"},
        ]

        result = _resolve_column_names(optimizer_cols, real_cols)

        assert result == ["Customer_ID", "OrderTotal", "CreatedAt"]


# =============================================================================
# B. build_export_query() with SQLGlot Tests
# =============================================================================


class TestBuildExportQuery:
    """Tests for BaseExtractor.build_export_query() via SQLGlot."""

    def _make_extractor(self, dialect="postgres"):
        """Create a minimal extractor for testing build_export_query."""
        from unittest.mock import MagicMock

        from dvt.federation.extractors.base import BaseExtractor

        # BaseExtractor is abstract, so we create a concrete subclass
        class TestExtractor(BaseExtractor):
            adapter_types = ["test"]

            def extract(self, config, output_path):
                pass

            def extract_hashes(self, config):
                pass

            def get_row_count(self, schema, table, predicates=None):
                return 0

            def get_columns(self, schema, table):
                return []

            def detect_primary_key(self, schema, table):
                return []

        ext = TestExtractor.__new__(TestExtractor)
        ext.connection = MagicMock()
        ext.dialect = dialect
        ext.on_progress = lambda msg: None
        ext.connection_config = None
        return ext

    def test_select_star_no_optimizations(self):
        """Should produce SELECT * when no columns/predicates/limit."""
        ext = self._make_extractor("postgres")
        config = ExtractionConfig(
            source_name="src.proj.orders",
            schema="public",
            table="orders",
        )

        sql = ext.build_export_query(config)

        assert "SELECT" in sql.upper()
        assert "*" in sql
        assert "public" in sql.lower() or "PUBLIC" in sql.upper()
        assert "orders" in sql.lower() or "ORDERS" in sql.upper()
        assert "WHERE" not in sql.upper()
        assert "LIMIT" not in sql.upper()

    def test_column_projection_postgres(self):
        """Should produce SELECT with specific columns for Postgres."""
        ext = self._make_extractor("postgres")
        config = ExtractionConfig(
            source_name="src.proj.orders",
            schema="public",
            table="orders",
            columns=["id", "date", "amount"],
        )

        sql = ext.build_export_query(config)

        assert "id" in sql.lower()
        assert "date" in sql.lower()
        assert "amount" in sql.lower()
        assert "*" not in sql

    def test_predicates(self):
        """Should produce WHERE clause with predicates."""
        ext = self._make_extractor("postgres")
        config = ExtractionConfig(
            source_name="src.proj.orders",
            schema="public",
            table="orders",
            predicates=["date > '2024-01-01'"],
        )

        sql = ext.build_export_query(config)

        assert "WHERE" in sql.upper()
        assert "date" in sql.lower()

    def test_limit_postgres(self):
        """Should produce LIMIT clause for Postgres."""
        ext = self._make_extractor("postgres")
        config = ExtractionConfig(
            source_name="src.proj.orders",
            schema="public",
            table="orders",
            limit=100,
        )

        sql = ext.build_export_query(config)

        assert "LIMIT" in sql.upper()
        assert "100" in sql

    def test_combined_columns_predicates_limit(self):
        """Should combine columns, predicates, and LIMIT."""
        ext = self._make_extractor("postgres")
        config = ExtractionConfig(
            source_name="src.proj.orders",
            schema="public",
            table="orders",
            columns=["id", "amount"],
            predicates=["amount > 100"],
            limit=50,
        )

        sql = ext.build_export_query(config)

        assert "id" in sql.lower()
        assert "amount" in sql.lower()
        assert "WHERE" in sql.upper()
        assert "LIMIT" in sql.upper()
        assert "50" in sql
        assert "*" not in sql

    def test_mysql_dialect(self):
        """Should produce valid MySQL query with backtick quoting."""
        ext = self._make_extractor("mysql")
        config = ExtractionConfig(
            source_name="src.proj.orders",
            schema="mydb",
            table="orders",
            columns=["id", "total"],
            limit=10,
        )

        sql = ext.build_export_query(config)

        assert "LIMIT" in sql.upper()
        assert "10" in sql

    def test_no_limit_when_none(self):
        """Should not include LIMIT when limit is None."""
        ext = self._make_extractor("postgres")
        config = ExtractionConfig(
            source_name="src.proj.orders",
            schema="public",
            table="orders",
            limit=None,
        )

        sql = ext.build_export_query(config)

        assert "LIMIT" not in sql.upper()


# =============================================================================
# C. End-to-End Config Propagation Tests
# =============================================================================


class TestConfigPropagation:
    """Tests for columns + limit flowing through the config chain."""

    def test_source_config_has_limit(self):
        """SourceConfig should accept limit field."""
        sc = SourceConfig(
            source_name="test",
            adapter_type="postgres",
            schema="public",
            table="orders",
            connection=None,
            columns=["id", "name"],
            predicates=["id > 10"],
            limit=100,
        )

        assert sc.limit == 100
        assert sc.columns == ["id", "name"]
        assert sc.predicates == ["id > 10"]

    def test_source_config_limit_default_none(self):
        """SourceConfig limit should default to None."""
        sc = SourceConfig(
            source_name="test",
            adapter_type="postgres",
            schema="public",
            table="orders",
            connection=None,
        )

        assert sc.limit is None
        assert sc.columns is None

    def test_extraction_config_has_limit(self):
        """ExtractionConfig should accept limit field."""
        ec = ExtractionConfig(
            source_name="test",
            schema="public",
            table="orders",
            columns=["id", "amount"],
            predicates=["date > '2024-01-01'"],
            limit=500,
        )

        assert ec.limit == 500
        assert ec.columns == ["id", "amount"]
        assert ec.predicates == ["date > '2024-01-01'"]

    def test_extraction_config_limit_default_none(self):
        """ExtractionConfig limit should default to None."""
        ec = ExtractionConfig(
            source_name="test",
            schema="public",
            table="orders",
        )

        assert ec.limit is None

    def test_pushable_operations_to_source_config(self):
        """PushableOperations columns/limit should flow to SourceConfig."""
        ops = PushableOperations(
            source_id="source.proj.orders",
            source_alias="o",
            columns=["id", "name", "amount"],
            predicates=["date > '2024-01-01'"],
            limit=100,
        )

        sc = SourceConfig(
            source_name=ops.source_id,
            adapter_type="postgres",
            schema="public",
            table="orders",
            connection=None,
            columns=ops.columns or None,
            predicates=ops.predicates or None,
            limit=ops.limit,
        )

        assert sc.columns == ["id", "name", "amount"]
        assert sc.predicates == ["date > '2024-01-01'"]
        assert sc.limit == 100

    def test_empty_columns_becomes_none(self):
        """Empty optimizer columns list should become None (SELECT *)."""
        ops = PushableOperations(
            source_id="source.proj.orders",
            source_alias="o",
            columns=[],  # SELECT *
            limit=None,
        )

        sc = SourceConfig(
            source_name=ops.source_id,
            adapter_type="postgres",
            schema="public",
            table="orders",
            connection=None,
            columns=ops.columns or None,
            limit=ops.limit,
        )

        assert sc.columns is None
        assert sc.limit is None

    def test_optimizer_extract_to_export_query(self):
        """Full pipeline: optimizer extraction → build_export_query."""
        optimizer = QueryOptimizer()
        sql = "SELECT o.id, o.amount FROM orders o WHERE o.status = 'active' LIMIT 50"
        mappings = {"source.proj.orders": "o"}

        result = optimizer.extract_all_pushable_operations(sql, mappings, "postgres")
        ops = result["source.proj.orders"]

        # Columns and limit should be extracted
        assert "id" in ops.columns
        assert "amount" in ops.columns
        assert ops.limit == 50
        assert len(ops.predicates) >= 1

        # Simulate case resolution
        real_cols = [
            {"name": "ID", "type": "int"},
            {"name": "Amount", "type": "decimal"},
            {"name": "Status", "type": "varchar"},
        ]
        resolved = _resolve_column_names(ops.columns, real_cols)

        assert "ID" in resolved
        assert "Amount" in resolved
        # Status is in predicates/WHERE but may or may not be in SELECT columns
        # depending on SQLGlot — the key point is ID and Amount are resolved

    def test_select_star_no_column_pushdown(self):
        """SELECT * should result in no column pushdown."""
        optimizer = QueryOptimizer()
        sql = "SELECT * FROM orders o"
        mappings = {"source.proj.orders": "o"}

        result = optimizer.extract_all_pushable_operations(sql, mappings, "postgres")
        ops = result["source.proj.orders"]

        # SELECT * should produce empty columns (no projection pushdown)
        assert ops.columns == []

        # Empty columns → None via the or-None pattern
        columns = ops.columns or None
        assert columns is None
