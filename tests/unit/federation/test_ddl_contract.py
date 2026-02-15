# coding=utf-8
"""Unit tests for DDL contract across all loader methods.

DDL Contract:
- dvt run (default):         TRUNCATE + INSERT (preserves table structure)
- dvt run --full-refresh:    DROP + CREATE + INSERT (rebuilds structure)

Tests verify that every loader method respects this contract:
1. PostgresLoader._load_pipe()         - dedicated psycopg2 connection
2. PostgresLoader._load_copy_streaming() - dedicated psycopg2 connection
3. PostgresLoader._load_copy()          - dedicated psycopg2 connection
4. GenericLoader._load_pipe()           - adapter-based DDL
5. BaseLoader._execute_ddl()            - adapter-based DDL (shared)
"""

import sys
from unittest.mock import MagicMock, Mock, call, patch

import pytest

from dvt.federation.loaders.base import LoadConfig


# =============================================================================
# Helper: extract SQL statements from mock psycopg2 cursor
# =============================================================================


def _get_executed_sql(mock_cursor):
    """Extract all SQL strings passed to cursor.execute()."""
    return [c.args[0] for c in mock_cursor.execute.call_args_list]


def _make_pg_config(full_refresh=False, truncate=True, mode="overwrite"):
    """Create a LoadConfig for postgres DDL tests."""
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


def _mock_psycopg2_connect(mock_cursor, mock_conn):
    """Create a patcher for psycopg2.connect that returns our mock connection."""
    mock_conn.cursor.return_value = mock_cursor
    mock_conn.autocommit = False
    return patch("psycopg2.connect", return_value=mock_conn)


def _mock_pg_helpers():
    """Create patchers for PostgresAuthHandler and build_create_table_sql."""
    create_sql = 'CREATE TABLE "public"."test_table" ("id" INTEGER, "name" TEXT)'

    mock_auth = patch(
        "dvt.federation.auth.postgres.PostgresAuthHandler.get_native_connection_kwargs",
        return_value={"host": "localhost", "dbname": "testdb"},
    )

    mock_build = patch(
        "dvt.utils.identifiers.build_create_table_sql",
        return_value=create_sql,
    )

    return mock_auth, mock_build


# =============================================================================
# PostgresLoader._load_pipe DDL Contract
# =============================================================================


class TestPostgresLoadPipeDDL:
    """DDL contract tests for PostgresLoader._load_pipe()."""

    def _run_pipe_ddl(self, config):
        """Run _load_pipe and return SQL executed on the DDL cursor."""
        from dvt.federation.loaders.postgres import PostgresLoader

        loader = PostgresLoader()

        mock_cursor = MagicMock()
        mock_conn = MagicMock()

        mock_df = MagicMock()
        mock_df.columns = ["id", "name"]

        mock_auth_p, mock_build_p = _mock_pg_helpers()

        with (
            _mock_psycopg2_connect(mock_cursor, mock_conn),
            mock_auth_p,
            mock_build_p,
            patch.object(loader, "_load_via_pipe", return_value=500),
            patch.object(loader, "_log"),
            patch("shutil.rmtree"),
        ):
            loader._load_pipe(mock_df, config)

        return _get_executed_sql(mock_cursor)

    def test_default_run_truncates(self):
        """dvt run (default): should TRUNCATE, not DROP."""
        config = _make_pg_config(full_refresh=False, truncate=True)
        sql_stmts = self._run_pipe_ddl(config)

        sql_joined = " ".join(sql_stmts)
        assert "CREATE TABLE IF NOT EXISTS" in sql_joined
        assert "TRUNCATE TABLE" in sql_joined
        assert "DROP TABLE" not in sql_joined

    def test_full_refresh_drops(self):
        """dvt run --full-refresh: should DROP + CREATE."""
        config = _make_pg_config(full_refresh=True, truncate=True)
        sql_stmts = self._run_pipe_ddl(config)

        sql_joined = " ".join(sql_stmts)
        assert "DROP TABLE IF EXISTS" in sql_joined
        assert "CREATE TABLE" in sql_joined
        assert "TRUNCATE" not in sql_joined

    def test_overwrite_no_truncate_drops(self):
        """mode=overwrite + truncate=False: should DROP + CREATE."""
        config = _make_pg_config(full_refresh=False, truncate=False, mode="overwrite")
        sql_stmts = self._run_pipe_ddl(config)

        sql_joined = " ".join(sql_stmts)
        assert "DROP TABLE IF EXISTS" in sql_joined
        assert "CREATE TABLE" in sql_joined
        assert "TRUNCATE" not in sql_joined

    def test_append_mode_no_ddl(self):
        """mode=append: should NOT execute any DDL."""
        config = _make_pg_config(full_refresh=False, truncate=True, mode="append")
        sql_stmts = self._run_pipe_ddl(config)

        # Filter out non-DDL statements (only check for DROP/TRUNCATE/CREATE)
        ddl_stmts = [
            s
            for s in sql_stmts
            if any(kw in s.upper() for kw in ["DROP", "TRUNCATE", "CREATE"])
        ]
        assert len(ddl_stmts) == 0


# =============================================================================
# PostgresLoader._load_copy_streaming DDL Contract
# =============================================================================


class TestPostgresLoadCopyStreamingDDL:
    """DDL contract tests for PostgresLoader._load_copy_streaming()."""

    def _run_streaming_ddl(self, config):
        """Run _load_copy_streaming and return SQL executed on cursor."""
        from dvt.federation.loaders.postgres import PostgresLoader, _SparkRowPipe

        loader = PostgresLoader()

        mock_cursor = MagicMock()
        mock_conn = MagicMock()

        mock_df = MagicMock()
        mock_df.columns = ["id", "name"]
        mock_df.rdd.getNumPartitions.return_value = 4

        mock_pipe = MagicMock()
        mock_pipe.rows_written = 100

        mock_auth_p, mock_build_p = _mock_pg_helpers()

        with (
            _mock_psycopg2_connect(mock_cursor, mock_conn),
            mock_auth_p,
            mock_build_p,
            patch(
                "dvt.federation.loaders.postgres._SparkRowPipe",
                return_value=mock_pipe,
            ),
            patch.object(loader, "_log"),
        ):
            loader._load_copy_streaming(mock_df, config)

        return _get_executed_sql(mock_cursor)

    def test_default_run_truncates(self):
        """dvt run (default): should TRUNCATE, not DROP."""
        config = _make_pg_config(full_refresh=False, truncate=True)
        sql_stmts = self._run_streaming_ddl(config)

        sql_joined = " ".join(sql_stmts)
        assert "CREATE TABLE IF NOT EXISTS" in sql_joined
        assert "TRUNCATE TABLE" in sql_joined
        assert "DROP TABLE" not in sql_joined

    def test_full_refresh_drops(self):
        """dvt run --full-refresh: should DROP + CREATE."""
        config = _make_pg_config(full_refresh=True, truncate=True)
        sql_stmts = self._run_streaming_ddl(config)

        sql_joined = " ".join(sql_stmts)
        assert "DROP TABLE IF EXISTS" in sql_joined
        assert "CREATE TABLE" in sql_joined
        assert "TRUNCATE" not in sql_joined

    def test_overwrite_no_truncate_drops(self):
        """mode=overwrite + truncate=False: should DROP + CREATE."""
        config = _make_pg_config(full_refresh=False, truncate=False, mode="overwrite")
        sql_stmts = self._run_streaming_ddl(config)

        sql_joined = " ".join(sql_stmts)
        assert "DROP TABLE IF EXISTS" in sql_joined
        assert "CREATE TABLE" in sql_joined
        assert "TRUNCATE" not in sql_joined


# =============================================================================
# PostgresLoader._load_copy DDL Contract
# =============================================================================


class TestPostgresLoadCopyDDL:
    """DDL contract tests for PostgresLoader._load_copy()."""

    def _run_copy_ddl(self, config):
        """Run _load_copy and return SQL executed on cursor."""
        from dvt.federation.loaders.postgres import PostgresLoader

        loader = PostgresLoader()

        mock_cursor = MagicMock()
        mock_conn = MagicMock()

        mock_df = MagicMock()
        mock_df.columns = ["id", "name"]
        mock_df.collect.return_value = [(1, "alice"), (2, "bob")]

        mock_auth_p, mock_build_p = _mock_pg_helpers()

        with (
            _mock_psycopg2_connect(mock_cursor, mock_conn),
            mock_auth_p,
            mock_build_p,
            patch.object(loader, "_log"),
        ):
            loader._load_copy(mock_df, config)

        return _get_executed_sql(mock_cursor)

    def test_default_run_truncates(self):
        """dvt run (default): should TRUNCATE, not DROP."""
        config = _make_pg_config(full_refresh=False, truncate=True)
        sql_stmts = self._run_copy_ddl(config)

        sql_joined = " ".join(sql_stmts)
        assert "CREATE TABLE IF NOT EXISTS" in sql_joined
        assert "TRUNCATE TABLE" in sql_joined
        assert "DROP TABLE" not in sql_joined

    def test_full_refresh_drops(self):
        """dvt run --full-refresh: should DROP + CREATE."""
        config = _make_pg_config(full_refresh=True, truncate=True)
        sql_stmts = self._run_copy_ddl(config)

        sql_joined = " ".join(sql_stmts)
        assert "DROP TABLE IF EXISTS" in sql_joined
        assert "CREATE TABLE" in sql_joined
        assert "TRUNCATE" not in sql_joined

    def test_overwrite_no_truncate_drops(self):
        """mode=overwrite + truncate=False: should DROP + CREATE."""
        config = _make_pg_config(full_refresh=False, truncate=False, mode="overwrite")
        sql_stmts = self._run_copy_ddl(config)

        sql_joined = " ".join(sql_stmts)
        assert "DROP TABLE IF EXISTS" in sql_joined
        assert "CREATE TABLE" in sql_joined
        assert "TRUNCATE" not in sql_joined


# =============================================================================
# GenericLoader._load_pipe DDL Contract (adapter-based)
# =============================================================================


class TestGenericLoadPipeDDL:
    """DDL contract tests for GenericLoader._load_pipe()."""

    def test_default_run_calls_execute_ddl_with_truncate(self):
        """dvt run (default): _execute_ddl should receive config with truncate=True."""
        from dvt.federation.loaders.generic import GenericLoader

        loader = GenericLoader()
        config = _make_pg_config(full_refresh=False, truncate=True)
        config.connection_config["type"] = "mysql"

        mock_df = MagicMock()
        mock_adapter = MagicMock()

        with (
            patch.object(loader, "_execute_ddl") as mock_ddl,
            patch.object(loader, "_create_table_with_adapter"),
            patch.object(loader, "_load_via_pipe", return_value=100),
            patch.object(loader, "_log"),
            patch("shutil.rmtree"),
        ):
            loader._load_pipe(mock_df, config, adapter=mock_adapter, tool_name="mysql")

        mock_ddl.assert_called_once_with(mock_adapter, config)
        assert config.truncate is True
        assert config.full_refresh is False

    def test_full_refresh_calls_execute_ddl_with_full_refresh(self):
        """dvt run --full-refresh: _execute_ddl should see full_refresh=True."""
        from dvt.federation.loaders.generic import GenericLoader

        loader = GenericLoader()
        config = _make_pg_config(full_refresh=True, truncate=True)
        config.connection_config["type"] = "mysql"

        mock_df = MagicMock()
        mock_adapter = MagicMock()

        with (
            patch.object(loader, "_execute_ddl") as mock_ddl,
            patch.object(loader, "_create_table_with_adapter"),
            patch.object(loader, "_load_via_pipe", return_value=100),
            patch.object(loader, "_log"),
            patch("shutil.rmtree"),
        ):
            loader._load_pipe(mock_df, config, adapter=mock_adapter, tool_name="mysql")

        mock_ddl.assert_called_once_with(mock_adapter, config)
        assert config.full_refresh is True

    def test_no_adapter_skips_ddl(self):
        """Without adapter, DDL should be skipped entirely."""
        from dvt.federation.loaders.generic import GenericLoader

        loader = GenericLoader()
        config = _make_pg_config(full_refresh=False, truncate=True)
        config.connection_config["type"] = "mysql"

        mock_df = MagicMock()

        with (
            patch.object(loader, "_execute_ddl") as mock_ddl,
            patch.object(loader, "_create_table_with_adapter") as mock_create,
            patch.object(loader, "_load_via_pipe", return_value=100),
            patch.object(loader, "_log"),
            patch("shutil.rmtree"),
        ):
            loader._load_pipe(mock_df, config, adapter=None, tool_name="mysql")

        mock_ddl.assert_not_called()
        mock_create.assert_not_called()


# =============================================================================
# BaseLoader._execute_ddl DDL Contract (adapter-based, shared)
# =============================================================================


class TestBaseExecuteDDL:
    """DDL contract tests for BaseLoader._execute_ddl() via adapter."""

    def _make_loader(self):
        from dvt.federation.loaders.base import BaseLoader

        class TestLoader(BaseLoader):
            adapter_types = ["test"]

            def load(self, df, config, adapter=None):
                pass

        return TestLoader()

    def _run_execute_ddl(self, config):
        """Run _execute_ddl and return SQL executed via adapter."""
        loader = self._make_loader()
        mock_adapter = MagicMock()
        executed_sql = []

        def capture_execute(sql):
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

    def test_default_run_truncates_via_adapter(self):
        """dvt run (default): should TRUNCATE via adapter."""
        config = _make_pg_config(full_refresh=False, truncate=True)
        sql_stmts = self._run_execute_ddl(config)

        assert len(sql_stmts) == 1
        assert "TRUNCATE TABLE" in sql_stmts[0]

    def test_full_refresh_drops_via_adapter(self):
        """dvt run --full-refresh: should DROP via adapter."""
        config = _make_pg_config(full_refresh=True, truncate=True)
        sql_stmts = self._run_execute_ddl(config)

        assert any("DROP TABLE" in s for s in sql_stmts)

    def test_no_truncate_no_full_refresh_no_ddl(self):
        """truncate=False and full_refresh=False: no DDL."""
        config = _make_pg_config(full_refresh=False, truncate=False)
        sql_stmts = self._run_execute_ddl(config)

        assert len(sql_stmts) == 0


# =============================================================================
# Cross-method consistency tests
# =============================================================================


class TestDDLContractConsistency:
    """Verify all PG loader methods produce consistent DDL behavior."""

    def test_all_methods_use_same_condition_for_drop(self):
        """All PG methods should DROP under the same conditions."""
        import inspect
        from dvt.federation.loaders.postgres import PostgresLoader

        pipe_src = inspect.getsource(PostgresLoader._load_pipe)
        streaming_src = inspect.getsource(PostgresLoader._load_copy_streaming)
        copy_src = inspect.getsource(PostgresLoader._load_copy)

        for method_name, src in [
            ("_load_pipe", pipe_src),
            ("_load_copy_streaming", streaming_src),
            ("_load_copy", copy_src),
        ]:
            assert "config.full_refresh" in src, (
                f"{method_name} doesn't check full_refresh"
            )
            assert "config.truncate" in src, f"{method_name} doesn't check truncate"
            assert "TRUNCATE TABLE" in src, f"{method_name} doesn't have TRUNCATE path"
            assert "DROP TABLE IF EXISTS" in src, (
                f"{method_name} doesn't have DROP path"
            )
            assert "CREATE TABLE IF NOT EXISTS" in src, (
                f"{method_name} doesn't have CREATE IF NOT EXISTS path"
            )

    def test_load_config_defaults(self):
        """LoadConfig defaults should enforce TRUNCATE behavior."""
        config = LoadConfig(table_name="test")

        assert config.mode == "overwrite"
        assert config.truncate is True
        assert config.full_refresh is False
