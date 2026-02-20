# coding=utf-8
"""Unit tests for --target CLI override: schema/database resolution and
Databricks JDBC column mapping bypass.

Tests verify:
1. _resolve_target_table_name() uses override target's schema/database
2. _resolve_target_table_name() respects model's custom schema config
3. Databricks 3-part table names (catalog.schema.table)
4. No-op when target is not overridden
5. FederationLoader._load_jdbc() bypasses adapter DDL for Databricks + special cols
"""

from dataclasses import dataclass, field
from typing import Any, Dict, Optional, Set
from unittest.mock import MagicMock, Mock, patch

import pytest

from dvt.federation.loaders.base import FederationLoader, LoadConfig
from dvt.federation.resolver import ExecutionPath, ResolvedExecution


# =============================================================================
# Mock helpers
# =============================================================================


@dataclass
class MockModelConfig:
    """Minimal model config mock."""

    target: Optional[str] = None
    schema: Optional[str] = None


@dataclass
class MockModel:
    """Minimal model mock for engine tests."""

    name: str = "test_model"
    schema: str = "public"
    database: Optional[str] = None
    config: Optional[MockModelConfig] = None


def _make_resolution(target: str = "pg_dev") -> ResolvedExecution:
    """Create a minimal ResolvedExecution."""
    return ResolvedExecution(
        model_id="model.test.test_model",
        target=target,
        execution_path=ExecutionPath.SPARK_FEDERATION,
        upstream_targets=set(),
    )


def _make_engine(default_target: str = "pg_dev", profiles: dict = None):
    """Create a FederationEngine with mocked config and profiles."""
    from dvt.federation.engine import FederationEngine

    mock_config = MagicMock()
    mock_config.target_name = default_target
    mock_config.profile_name = "test_profile"
    mock_config.profiles_dir = "/fake"
    mock_manifest = MagicMock()

    engine = FederationEngine(mock_config, mock_manifest)

    # Pre-load profiles so _get_connection_config doesn't try to read files
    if profiles:
        engine._profiles = profiles
    else:
        engine._profiles = {
            "test_profile": {
                "outputs": {
                    "pg_dev": {
                        "type": "postgres",
                        "schema": "public",
                        "database": "postgres",
                        "host": "localhost",
                        "port": 5432,
                    },
                    "dbx_dev": {
                        "type": "databricks",
                        "schema": "dvt_test",
                        "catalog": "demo",
                        "host": "dbc-test.cloud.databricks.com",
                        "http_path": "/sql/test",
                    },
                    "sf_dev": {
                        "type": "snowflake",
                        "schema": "STG",
                        "database": "EXIM_EDWH_DEV",
                    },
                }
            }
        }

    return engine


# =============================================================================
# _resolve_target_table_name tests
# =============================================================================


class TestResolveTargetTableName:
    """Tests for FederationEngine._resolve_target_table_name()."""

    def test_no_override_uses_model_schema(self):
        """When target matches model's target, use model.schema as-is."""
        engine = _make_engine(default_target="pg_dev")
        model = MockModel(
            name="my_model",
            schema="public",
            config=MockModelConfig(target="pg_dev"),
        )
        resolution = _make_resolution(target="pg_dev")
        target_config = {"type": "postgres", "schema": "public"}

        result = engine._resolve_target_table_name(model, resolution, target_config)
        assert result == "public.my_model"

    def test_override_uses_target_schema(self):
        """When --target overrides, use the override target's schema."""
        engine = _make_engine(default_target="pg_dev")
        model = MockModel(
            name="pushdown_pg_only",
            schema="public",  # parse-time schema from pg_dev
            config=MockModelConfig(target="pg_dev"),
        )
        resolution = _make_resolution(target="dbx_dev")
        target_config = {"type": "databricks", "schema": "dvt_test", "catalog": "demo"}

        result = engine._resolve_target_table_name(model, resolution, target_config)
        # Databricks: catalog.schema.table
        assert result == "demo.dvt_test.pushdown_pg_only"

    def test_override_respects_custom_schema(self):
        """When model has config(schema='custom'), keep it even with --target."""
        engine = _make_engine(default_target="pg_dev")
        model = MockModel(
            name="pushdown_databricks_only",
            schema="dvt_test",  # from config(schema='dvt_test')
            config=MockModelConfig(target="dbx_dev", schema="dvt_test"),
        )
        resolution = _make_resolution(target="pg_dev")
        target_config = {"type": "postgres", "schema": "public"}

        result = engine._resolve_target_table_name(model, resolution, target_config)
        # Model has explicit schema config -> keep it
        assert result == "dvt_test.pushdown_databricks_only"

    def test_override_pg_to_databricks_3part(self):
        """PG model to DBX target should produce catalog.schema.table."""
        engine = _make_engine(default_target="pg_dev")
        model = MockModel(
            name="test_model",
            schema="public",
            config=MockModelConfig(target="pg_dev"),
        )
        resolution = _make_resolution(target="dbx_dev")
        target_config = {"type": "databricks", "schema": "dvt_test", "catalog": "demo"}

        result = engine._resolve_target_table_name(model, resolution, target_config)
        assert result == "demo.dvt_test.test_model"

    def test_override_databricks_to_pg_2part(self):
        """DBX model to PG target should produce schema.table (no catalog)."""
        engine = _make_engine(default_target="pg_dev")
        model = MockModel(
            name="test_model",
            schema="dvt_test",
            config=MockModelConfig(target="dbx_dev", schema="dvt_test"),
        )
        resolution = _make_resolution(target="pg_dev")
        target_config = {"type": "postgres", "schema": "public"}

        result = engine._resolve_target_table_name(model, resolution, target_config)
        # Model has custom schema -> keep dvt_test
        assert result == "dvt_test.test_model"

    def test_override_databricks_to_pg_no_custom_schema(self):
        """DBX model without custom schema to PG -> use PG's schema."""
        engine = _make_engine(default_target="dbx_dev")
        model = MockModel(
            name="test_model",
            schema="default",  # parse-time from dbx_dev default
            config=MockModelConfig(target=None),  # no explicit target config
        )
        resolution = _make_resolution(target="pg_dev")
        target_config = {"type": "postgres", "schema": "public"}

        result = engine._resolve_target_table_name(model, resolution, target_config)
        assert result == "public.test_model"

    def test_override_to_snowflake(self):
        """Override to Snowflake should use Snowflake's schema."""
        engine = _make_engine(default_target="pg_dev")
        model = MockModel(
            name="test_model",
            schema="public",
            config=MockModelConfig(target="pg_dev"),
        )
        resolution = _make_resolution(target="sf_dev")
        target_config = {
            "type": "snowflake",
            "schema": "STG",
            "database": "EXIM_EDWH_DEV",
        }

        result = engine._resolve_target_table_name(model, resolution, target_config)
        # Snowflake: not databricks/spark, so 2-part
        assert result == "STG.test_model"

    def test_no_override_default_target(self):
        """Model with no explicit target, resolution matches default -> no override."""
        engine = _make_engine(default_target="pg_dev")
        model = MockModel(
            name="test_model",
            schema="public",
            config=MockModelConfig(target=None),  # uses default
        )
        resolution = _make_resolution(target="pg_dev")
        target_config = {"type": "postgres", "schema": "public"}

        result = engine._resolve_target_table_name(model, resolution, target_config)
        assert result == "public.test_model"

    def test_databricks_no_catalog_uses_2part(self):
        """Databricks without catalog in config falls back to 2-part name."""
        engine = _make_engine(default_target="pg_dev")
        model = MockModel(
            name="test_model",
            schema="public",
            config=MockModelConfig(target="pg_dev"),
        )
        resolution = _make_resolution(target="dbx_dev")
        # No catalog in config
        target_config = {"type": "databricks", "schema": "dvt_test"}

        result = engine._resolve_target_table_name(model, resolution, target_config)
        # No catalog -> 2-part even for databricks
        assert result == "dvt_test.test_model"


# =============================================================================
# Loader: Databricks + special columns uses adapter SQL Connector
# =============================================================================


class TestDatabricksSpecialColumnsPath:
    """Tests that _load_jdbc dispatches to _load_via_adapter for Databricks
    with special column names, and that _load_via_adapter uses adapter DDL
    + batched INSERT statements (not Spark JDBC)."""

    def _make_df_mock(self, column_names, rows=None):
        """Create a mock DataFrame with the given column names and optional rows."""
        from unittest.mock import PropertyMock

        fields = []
        for name in column_names:
            f = MagicMock()
            f.name = name
            fields.append(f)

        schema = MagicMock()
        schema.fields = fields

        df = MagicMock()
        type(df).schema = PropertyMock(return_value=schema)
        df.count.return_value = len(rows) if rows else 0
        df.repartition.return_value = df
        df.collect.return_value = rows or []
        return df

    def _make_loader_config(self, adapter_type="databricks"):
        """Create LoadConfig for loader tests."""
        return LoadConfig(
            table_name="dvt_test.test_table",
            mode="overwrite",
            truncate=True,
            full_refresh=False,
            connection_config={
                "type": adapter_type,
                "host": "test.cloud.databricks.com",
                "http_path": "/sql/test",
                "token": "test_token",
                "schema": "dvt_test",
            },
            jdbc_config={"num_partitions": 1, "batch_size": 100},
        )

    @patch("dvt.federation.loaders.base.FederationLoader._load_via_adapter")
    def test_special_cols_dispatches_to_adapter(self, mock_adapter_load):
        """Databricks + special column names should dispatch to _load_via_adapter."""
        loader = FederationLoader()
        df = self._make_df_mock(["Customer Code", "Total Amount"])
        config = self._make_loader_config("databricks")
        mock_adapter = MagicMock()
        mock_adapter.type.return_value = "databricks"
        mock_adapter_load.return_value = MagicMock()

        with patch("dvt.federation.spark_manager.SparkManager.get_instance"):
            with patch("dvt.federation.auth.get_auth_handler") as mock_auth:
                mock_auth_instance = MagicMock()
                mock_auth_instance.validate.return_value = (True, None)
                mock_auth_instance.get_jdbc_properties.return_value = {}
                mock_auth.return_value = mock_auth_instance

                loader._load_jdbc(df, config, adapter=mock_adapter)

        # Should dispatch to _load_via_adapter
        mock_adapter_load.assert_called_once_with(df, config, mock_adapter)

    @patch("dvt.federation.loaders.base.FederationLoader._execute_ddl")
    @patch("dvt.federation.loaders.base.FederationLoader._create_table_with_adapter")
    def test_simple_cols_uses_jdbc_path(self, mock_create, mock_ddl):
        """Databricks + simple column names should use normal JDBC path."""
        loader = FederationLoader()
        df = self._make_df_mock(["customer_code", "total_amount"])
        config = self._make_loader_config("databricks")
        mock_adapter = MagicMock()

        with patch("dvt.federation.spark_manager.SparkManager.get_instance"):
            with patch("dvt.federation.auth.get_auth_handler") as mock_auth:
                mock_auth_instance = MagicMock()
                mock_auth_instance.validate.return_value = (True, None)
                mock_auth_instance.get_jdbc_properties.return_value = {
                    "user": "token",
                    "password": "test",
                }
                mock_auth.return_value = mock_auth_instance

                try:
                    loader._load_jdbc(df, config, adapter=mock_adapter)
                except Exception:
                    pass

        # Adapter DDL SHOULD have been called (normal JDBC path)
        mock_ddl.assert_called_once()
        mock_create.assert_called_once()

    @patch("dvt.federation.loaders.base.FederationLoader._execute_ddl")
    @patch("dvt.federation.loaders.base.FederationLoader._create_table_with_adapter")
    def test_postgres_special_cols_uses_jdbc_path(self, mock_create, mock_ddl):
        """Postgres + special columns should still use JDBC path (not adapter path)."""
        loader = FederationLoader()
        df = self._make_df_mock(["Customer Code", "Total Amount"])
        config = self._make_loader_config("postgres")
        config.connection_config = {
            "type": "postgres",
            "host": "localhost",
            "schema": "public",
        }
        mock_adapter = MagicMock()

        with patch("dvt.federation.spark_manager.SparkManager.get_instance"):
            with patch("dvt.federation.auth.get_auth_handler") as mock_auth:
                mock_auth_instance = MagicMock()
                mock_auth_instance.validate.return_value = (True, None)
                mock_auth_instance.get_jdbc_properties.return_value = {
                    "user": "test",
                    "password": "test",
                }
                mock_auth.return_value = mock_auth_instance

                try:
                    loader._load_jdbc(df, config, adapter=mock_adapter)
                except Exception:
                    pass

        # Postgres: ALWAYS uses JDBC path regardless of column names
        mock_ddl.assert_called_once()
        mock_create.assert_called_once()

    @patch("dvt.federation.loaders.base.FederationLoader._execute_ddl")
    @patch("dvt.federation.loaders.base.FederationLoader._create_table_with_adapter")
    @patch("dvt.federation.loaders.base.FederationLoader._commit")
    @patch("dvt.federation.adapter_manager.get_quoted_table_name")
    def test_load_via_adapter_uses_ddl_and_insert(
        self, mock_quote, mock_commit, mock_create, mock_ddl
    ):
        """_load_via_adapter creates table via DDL then INSERTs via adapter."""
        from unittest.mock import call

        mock_quote.return_value = "`dvt_test`.`test_table`"

        loader = FederationLoader()
        # Two rows of data
        row1 = MagicMock()
        row1.__iter__ = Mock(return_value=iter(["CUST001", 100]))
        row2 = MagicMock()
        row2.__iter__ = Mock(return_value=iter(["CUST002", 200]))

        df = self._make_df_mock(["Customer Code", "quantity"], rows=[row1, row2])
        config = self._make_loader_config("databricks")
        mock_adapter = MagicMock()
        mock_adapter.type.return_value = "databricks"

        result = loader._load_via_adapter(df, config, mock_adapter)

        assert result.success is True
        assert result.row_count == 2
        assert result.load_method == "adapter"

        # DDL and CREATE TABLE should have been called
        mock_ddl.assert_called_once()
        mock_create.assert_called_once()

        # adapter.execute should have been called with INSERT INTO ... VALUES
        insert_calls = [
            c for c in mock_adapter.execute.call_args_list if "INSERT INTO" in str(c)
        ]
        assert len(insert_calls) == 1  # One batch for 2 rows
        insert_sql = insert_calls[0][0][0]
        assert "INSERT INTO `dvt_test`.`test_table`" in insert_sql
        assert "`Customer Code`" in insert_sql
        assert "`quantity`" in insert_sql
        assert "CUST001" in insert_sql
        assert "CUST002" in insert_sql

    @patch("dvt.federation.loaders.base.FederationLoader._execute_ddl")
    @patch("dvt.federation.loaders.base.FederationLoader._create_table_with_adapter")
    @patch("dvt.federation.loaders.base.FederationLoader._commit")
    @patch("dvt.federation.adapter_manager.get_quoted_table_name")
    def test_load_via_adapter_escapes_values(
        self, mock_quote, mock_commit, mock_create, mock_ddl
    ):
        """_load_via_adapter properly escapes SQL values."""
        mock_quote.return_value = "`dvt_test`.`test_table`"

        loader = FederationLoader()
        row = MagicMock()
        row.__iter__ = Mock(return_value=iter(["O'Brien", None, 42, True]))

        df = self._make_df_mock(
            ["Customer Name", "Notes", "age", "active"],
            rows=[row],
        )
        config = self._make_loader_config("databricks")
        mock_adapter = MagicMock()
        mock_adapter.type.return_value = "databricks"

        result = loader._load_via_adapter(df, config, mock_adapter)
        assert result.success is True

        insert_sql = mock_adapter.execute.call_args[0][0]
        # Single quotes escaped by doubling
        assert "O''Brien" in insert_sql
        # NULL literal
        assert "NULL" in insert_sql
        # Numeric literal (no quotes)
        assert "42" in insert_sql
        # Boolean literal
        assert "TRUE" in insert_sql

    @patch("dvt.federation.loaders.base.FederationLoader._execute_ddl")
    @patch("dvt.federation.loaders.base.FederationLoader._create_table_with_adapter")
    @patch("dvt.federation.loaders.base.FederationLoader._commit")
    @patch("dvt.federation.adapter_manager.get_quoted_table_name")
    def test_load_via_adapter_empty_df(
        self, mock_quote, mock_commit, mock_create, mock_ddl
    ):
        """_load_via_adapter handles empty DataFrames gracefully."""
        mock_quote.return_value = "`dvt_test`.`test_table`"

        loader = FederationLoader()
        df = self._make_df_mock(["Customer Code"], rows=[])
        config = self._make_loader_config("databricks")
        mock_adapter = MagicMock()
        mock_adapter.type.return_value = "databricks"

        result = loader._load_via_adapter(df, config, mock_adapter)
        assert result.success is True
        assert result.row_count == 0
        # adapter.execute should NOT have been called (no INSERTs)
        mock_adapter.execute.assert_not_called()

    @patch("dvt.federation.loaders.base.FederationLoader._load_via_adapter")
    def test_no_adapter_skips_special_cols_check(self, mock_adapter_load):
        """When no adapter provided, skip special cols check — use pure JDBC."""
        loader = FederationLoader()
        df = self._make_df_mock(["Customer Code", "Total Amount"])
        config = self._make_loader_config("databricks")

        with patch("dvt.federation.spark_manager.SparkManager.get_instance"):
            with patch("dvt.federation.auth.get_auth_handler") as mock_auth:
                mock_auth_instance = MagicMock()
                mock_auth_instance.validate.return_value = (True, None)
                mock_auth_instance.get_jdbc_properties.return_value = {}
                mock_auth.return_value = mock_auth_instance

                # No adapter -> should NOT dispatch to _load_via_adapter
                loader._load_jdbc(df, config, adapter=None)

        # _load_via_adapter should NOT have been called
        mock_adapter_load.assert_not_called()
