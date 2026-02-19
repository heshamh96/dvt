# coding=utf-8
"""Unit tests for DDL contract on FederationLoader.

DDL Contract:
- dvt run (default):         TRUNCATE + INSERT (preserves table structure)
- dvt run --full-refresh:    DROP + CREATE + INSERT (rebuilds structure)

Tests verify that FederationLoader._execute_ddl() respects this contract
for all adapters (single JDBC + adapter loader pattern).
"""

from unittest.mock import MagicMock, Mock, patch

import pytest

from dvt.federation.loaders.base import FederationLoader, LoadConfig


# =============================================================================
# Helper
# =============================================================================


def _make_config(full_refresh=False, truncate=True, mode="overwrite"):
    """Create a LoadConfig for DDL tests."""
    return LoadConfig(
        table_name="public.test_table",
        mode=mode,
        truncate=truncate,
        full_refresh=full_refresh,
        connection_config={
            "type": "postgres",
            "host": "localhost",
            "port": 5432,
            "user": "test",
            "password": "test",
            "database": "testdb",
        },
    )


# =============================================================================
# FederationLoader._execute_ddl DDL Contract
# =============================================================================


class TestFederationLoaderDDL:
    """DDL contract tests for FederationLoader._execute_ddl() via adapter."""

    def _run_execute_ddl(self, config):
        """Run _execute_ddl and return SQL executed via adapter."""
        loader = FederationLoader()
        mock_adapter = MagicMock()
        executed_sql = []

        def capture_execute(sql, auto_begin=False):
            executed_sql.append(sql)

        mock_adapter.execute.side_effect = capture_execute
        mock_adapter.connection_named.return_value.__enter__ = Mock(return_value=None)
        mock_adapter.connection_named.return_value.__exit__ = Mock(return_value=False)

        with patch(
            "dvt.federation.adapter_manager.get_quoted_table_name",
            return_value='"public"."test_table"',
        ):
            loader._execute_ddl(mock_adapter, config)

        return executed_sql

    def test_default_run_truncates(self):
        """dvt run (default): should TRUNCATE via adapter."""
        config = _make_config(full_refresh=False, truncate=True)
        sql_stmts = self._run_execute_ddl(config)

        assert len(sql_stmts) == 1
        assert "TRUNCATE TABLE" in sql_stmts[0]

    def test_full_refresh_drops(self):
        """dvt run --full-refresh: should DROP via adapter."""
        config = _make_config(full_refresh=True, truncate=True)
        sql_stmts = self._run_execute_ddl(config)

        assert any("DROP TABLE" in s for s in sql_stmts)

    def test_no_truncate_no_full_refresh_no_ddl(self):
        """truncate=False and full_refresh=False: no DDL."""
        config = _make_config(full_refresh=False, truncate=False)
        sql_stmts = self._run_execute_ddl(config)

        assert len(sql_stmts) == 0

    def test_full_refresh_does_not_truncate(self):
        """full_refresh=True should DROP, not TRUNCATE."""
        config = _make_config(full_refresh=True, truncate=True)
        sql_stmts = self._run_execute_ddl(config)

        sql_joined = " ".join(sql_stmts)
        assert "DROP TABLE IF EXISTS" in sql_joined
        assert "TRUNCATE" not in sql_joined


# =============================================================================
# LoadConfig defaults
# =============================================================================


class TestLoadConfigDefaults:
    """LoadConfig defaults should enforce TRUNCATE behavior."""

    def test_load_config_defaults(self):
        """LoadConfig defaults should enforce TRUNCATE behavior."""
        config = LoadConfig(table_name="test")

        assert config.mode == "overwrite"
        assert config.truncate is True
        assert config.full_refresh is False

    def test_load_config_no_bucket_or_streaming_fields(self):
        """LoadConfig should not have bucket_config or streaming_batch_size."""
        config = LoadConfig(table_name="test")
        assert not hasattr(config, "bucket_config")
        assert not hasattr(config, "streaming_batch_size")

    def test_load_config_has_phase4_fields(self):
        """LoadConfig should have incremental_strategy and unique_key for Phase 4."""
        config = LoadConfig(table_name="test")
        assert config.incremental_strategy is None
        assert config.unique_key is None


# =============================================================================
# FederationLoader basic behavior
# =============================================================================


class TestFederationLoaderBasic:
    """Test FederationLoader instantiation and basic behavior."""

    def test_instantiation(self):
        """FederationLoader should be instantiable without args."""
        loader = FederationLoader()
        assert loader is not None

    def test_progress_callback(self):
        """FederationLoader should use progress callback."""
        messages = []
        loader = FederationLoader(on_progress=messages.append)
        loader._log("test message")
        assert messages == ["test message"]

    def test_backward_compat_alias(self):
        """BaseLoader should be an alias for FederationLoader."""
        from dvt.federation.loaders.base import BaseLoader

        assert BaseLoader is FederationLoader

    def test_get_loader_returns_federation_loader(self):
        """get_loader() should always return FederationLoader."""
        from dvt.federation.loaders import get_loader

        loader = get_loader("postgres")
        assert isinstance(loader, FederationLoader)

        loader2 = get_loader("snowflake")
        assert isinstance(loader2, FederationLoader)

        loader3 = get_loader("unknown_adapter")
        assert isinstance(loader3, FederationLoader)
