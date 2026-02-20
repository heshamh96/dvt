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
# Loader: Databricks + special columns bypasses adapter DDL
# =============================================================================


class TestDatabricksSpecialColumnsPath:
    """Tests that _load_jdbc bypasses adapter DDL for Databricks with special cols."""

    def _make_df_mock(self, column_names):
        """Create a mock DataFrame with the given column names."""
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
        df.count.return_value = 10
        df.repartition.return_value = df
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

    @patch("dvt.federation.loaders.base.FederationLoader._execute_ddl")
    @patch("dvt.federation.loaders.base.FederationLoader._create_table_with_adapter")
    def test_special_cols_skips_adapter_ddl(self, mock_create, mock_ddl):
        """Databricks + special column names should skip adapter DDL."""
        loader = FederationLoader()
        df = self._make_df_mock(["Customer Code", "Total Amount"])
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

                # The JDBC write will fail but we're testing the DDL bypass
                try:
                    loader._load_jdbc(df, config, adapter=mock_adapter)
                except Exception:
                    pass

        # Adapter DDL should NOT have been called
        mock_ddl.assert_not_called()
        mock_create.assert_not_called()

    @patch("dvt.federation.loaders.base.FederationLoader._execute_ddl")
    @patch("dvt.federation.loaders.base.FederationLoader._create_table_with_adapter")
    def test_simple_cols_uses_adapter_ddl(self, mock_create, mock_ddl):
        """Databricks + simple column names should use normal adapter DDL path."""
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

        # Adapter DDL SHOULD have been called
        mock_ddl.assert_called_once()
        mock_create.assert_called_once()

    @patch("dvt.federation.loaders.base.FederationLoader._execute_ddl")
    @patch("dvt.federation.loaders.base.FederationLoader._create_table_with_adapter")
    def test_postgres_special_cols_uses_adapter_ddl(self, mock_create, mock_ddl):
        """Postgres + special columns should still use adapter DDL (not Databricks)."""
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

        # Postgres: ALWAYS uses adapter DDL regardless of column names
        mock_ddl.assert_called_once()
        mock_create.assert_called_once()
