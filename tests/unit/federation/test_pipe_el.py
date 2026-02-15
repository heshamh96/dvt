# coding=utf-8
"""Unit tests for pipe-based EL pipeline (Plan B).

Tests:
1. CLI tool detection: registry, detect_cli_tools, format_cli_tool_report
2. Pipe extraction: _extract_via_pipe shared method + per-DB command builders
3. Pipe loading: _load_via_pipe shared method + per-DB command builders
4. Fallback chains: pipe -> existing methods -> JDBC
5. sync.py Step 9 integration
"""

import os
import subprocess
from pathlib import Path
from unittest.mock import MagicMock, Mock, call, patch

import pytest


# =============================================================================
# CLI Tool Detection Tests
# =============================================================================


class TestCLIToolInfo:
    """Tests for CLI tool registry data structures."""

    def test_registry_has_all_tools(self):
        """Registry should contain all 5 expected tools."""
        from dvt.task.cli_tools import CLI_TOOL_REGISTRY

        tool_names = [t.tool_name for t in CLI_TOOL_REGISTRY]
        assert "psql" in tool_names
        assert "mysql" in tool_names
        assert "bcp" in tool_names
        assert "clickhouse-client" in tool_names
        assert "sqlplus" in tool_names
        assert len(CLI_TOOL_REGISTRY) == 5

    def test_psql_compatible_adapters(self):
        """psql should cover postgres and related adapters."""
        from dvt.task.cli_tools import CLI_TOOL_REGISTRY

        psql_tool = next(t for t in CLI_TOOL_REGISTRY if t.tool_name == "psql")
        assert "postgres" in psql_tool.compatible_adapters
        assert "greenplum" in psql_tool.compatible_adapters
        assert "alloydb" in psql_tool.compatible_adapters
        assert "timescaledb" in psql_tool.compatible_adapters

    def test_mysql_compatible_adapters(self):
        """mysql should cover mysql and related adapters."""
        from dvt.task.cli_tools import CLI_TOOL_REGISTRY

        mysql_tool = next(t for t in CLI_TOOL_REGISTRY if t.tool_name == "mysql")
        assert "mysql" in mysql_tool.compatible_adapters
        assert "mariadb" in mysql_tool.compatible_adapters
        assert "tidb" in mysql_tool.compatible_adapters

    def test_each_tool_has_install_instructions(self):
        """Every tool should have at least one install instruction."""
        from dvt.task.cli_tools import CLI_TOOL_REGISTRY

        for tool in CLI_TOOL_REGISTRY:
            assert len(tool.install_instructions) > 0, (
                f"{tool.tool_name} has no install instructions"
            )

    def test_each_tool_has_docs_url(self):
        """Every tool should have a docs URL."""
        from dvt.task.cli_tools import CLI_TOOL_REGISTRY

        for tool in CLI_TOOL_REGISTRY:
            assert tool.docs_url.startswith("http"), (
                f"{tool.tool_name} has invalid docs_url: {tool.docs_url}"
            )


class TestDetectCLITools:
    """Tests for detect_cli_tools function."""

    @patch("dvt.task.cli_tools.shutil.which")
    @patch("dvt.task.cli_tools._get_tool_version")
    def test_detects_found_tool(self, mock_version, mock_which):
        """Should detect a tool that is on PATH."""
        from dvt.task.cli_tools import detect_cli_tools

        mock_which.return_value = "/usr/bin/psql"
        mock_version.return_value = "psql (PostgreSQL) 16.1"

        results = detect_cli_tools(["postgres"])

        assert len(results) == 1
        assert results[0].found is True
        assert results[0].path == "/usr/bin/psql"
        assert results[0].version == "psql (PostgreSQL) 16.1"
        assert results[0].tool.tool_name == "psql"

    @patch("dvt.task.cli_tools.shutil.which")
    def test_detects_missing_tool(self, mock_which):
        """Should report missing tool when not on PATH."""
        from dvt.task.cli_tools import detect_cli_tools

        mock_which.return_value = None

        results = detect_cli_tools(["postgres"])

        assert len(results) == 1
        assert results[0].found is False
        assert results[0].path is None

    @patch("dvt.task.cli_tools.shutil.which")
    def test_skips_cloud_adapters(self, mock_which):
        """Should return empty for cloud-only adapters."""
        from dvt.task.cli_tools import detect_cli_tools

        results = detect_cli_tools(["snowflake", "bigquery", "redshift"])

        assert len(results) == 0
        mock_which.assert_not_called()

    @patch("dvt.task.cli_tools.shutil.which")
    @patch("dvt.task.cli_tools._get_tool_version")
    def test_no_duplicate_checks(self, mock_version, mock_which):
        """Should check psql only once even if multiple postgres-compatible adapters."""
        from dvt.task.cli_tools import detect_cli_tools

        mock_which.return_value = "/usr/bin/psql"
        mock_version.return_value = "psql 16"

        results = detect_cli_tools(["postgres", "greenplum", "alloydb"])

        assert len(results) == 1  # Only one check for psql

    @patch("dvt.task.cli_tools.shutil.which")
    @patch("dvt.task.cli_tools._get_tool_version")
    def test_multiple_adapters_multiple_tools(self, mock_version, mock_which):
        """Should detect multiple tools for multiple adapter types."""
        from dvt.task.cli_tools import detect_cli_tools

        def which_side_effect(name):
            return {
                "psql": "/usr/bin/psql",
                "mysql": None,
            }.get(name)

        mock_which.side_effect = which_side_effect
        mock_version.return_value = "psql 16"

        results = detect_cli_tools(["postgres", "mysql"])

        assert len(results) == 2
        psql_result = next(r for r in results if r.tool.tool_name == "psql")
        mysql_result = next(r for r in results if r.tool.tool_name == "mysql")
        assert psql_result.found is True
        assert mysql_result.found is False

    @patch("dvt.task.cli_tools.shutil.which")
    def test_empty_adapter_list(self, mock_which):
        """Should return empty for empty adapter list."""
        from dvt.task.cli_tools import detect_cli_tools

        results = detect_cli_tools([])
        assert len(results) == 0


class TestFormatCLIToolReport:
    """Tests for format_cli_tool_report function."""

    def test_empty_results(self):
        """Should show 'no adapters' message for empty results."""
        from dvt.task.cli_tools import format_cli_tool_report

        report = format_cli_tool_report([])
        assert "No on-prem database adapters" in report

    def test_found_tool_in_report(self):
        """Should show 'found' for detected tools."""
        from dvt.task.cli_tools import (
            CLIToolInfo,
            CLIToolStatus,
            format_cli_tool_report,
        )

        tool = CLIToolInfo(
            tool_name="psql",
            adapter_type="postgres",
            compatible_adapters=["postgres"],
            version_command=["psql", "--version"],
            install_instructions={"macOS": "brew install libpq"},
            docs_url="https://example.com",
        )
        results = [
            CLIToolStatus(
                tool=tool, found=True, path="/usr/bin/psql", version="psql 16"
            )
        ]

        report = format_cli_tool_report(results)
        assert "found" in report
        assert "psql 16" in report

    def test_missing_tool_shows_install_instructions(self):
        """Should show install instructions for missing tools."""
        from dvt.task.cli_tools import (
            CLIToolInfo,
            CLIToolStatus,
            format_cli_tool_report,
        )

        tool = CLIToolInfo(
            tool_name="mysql",
            adapter_type="mysql",
            compatible_adapters=["mysql"],
            version_command=["mysql", "--version"],
            install_instructions={"macOS": "brew install mysql-client"},
            docs_url="https://example.com",
        )
        results = [CLIToolStatus(tool=tool, found=False)]

        report = format_cli_tool_report(results)
        assert "not found" in report
        assert "Missing CLI tools" in report
        assert "brew install mysql-client" in report

    def test_report_has_footer(self):
        """Report should always end with the JDBC fallback note."""
        from dvt.task.cli_tools import (
            CLIToolInfo,
            CLIToolStatus,
            format_cli_tool_report,
        )

        tool = CLIToolInfo(
            tool_name="psql",
            adapter_type="postgres",
            compatible_adapters=["postgres"],
            version_command=["psql", "--version"],
            install_instructions={},
            docs_url="https://example.com",
        )
        results = [CLIToolStatus(tool=tool, found=True)]

        report = format_cli_tool_report(results)
        assert "Spark JDBC fallback" in report
        assert "PyArrow" in report


class TestPlatformDetection:
    """Tests for _detect_platform helper."""

    @patch("dvt.task.cli_tools.platform.system")
    def test_darwin_returns_macos(self, mock_system):
        """Should return 'macOS' for Darwin."""
        from dvt.task.cli_tools import _detect_platform

        mock_system.return_value = "Darwin"
        assert _detect_platform() == "macOS"

    @patch("dvt.task.cli_tools.platform.system")
    def test_linux_with_ubuntu(self, mock_system):
        """Should return 'Debian/Ubuntu' for Ubuntu."""
        from dvt.task.cli_tools import _detect_platform

        mock_system.return_value = "Linux"
        os_release = "NAME=Ubuntu\nVERSION_ID=22.04\nID=ubuntu\n"

        with patch("builtins.open", create=True) as mock_open:
            mock_open.return_value.__enter__ = lambda s: s
            mock_open.return_value.__exit__ = Mock(return_value=False)
            mock_open.return_value.read = Mock(return_value=os_release)
            assert _detect_platform() == "Debian/Ubuntu"


# =============================================================================
# Pipe Extraction Tests
# =============================================================================


class TestBaseExtractorPipe:
    """Tests for shared _extract_via_pipe in BaseExtractor."""

    def _make_extractor(self, cli_tool=None, cli_found=True):
        """Create a minimal extractor with pipe support for testing."""
        from dvt.federation.extractors.base import BaseExtractor, ExtractionConfig

        class TestExtractor(BaseExtractor):
            adapter_types = ["test"]

            def __init__(self):
                super().__init__(connection=MagicMock(), dialect="test")
                if cli_tool:
                    self.cli_tool = cli_tool

            def extract(self, config, output_path):
                pass

            def extract_hashes(self, config):
                return {}

            def get_row_count(self, schema, table, predicates=None):
                return 0

            def get_columns(self, schema, table):
                return []

            def detect_primary_key(self, schema, table):
                return []

        return TestExtractor()

    def test_has_cli_tool_when_present(self):
        """_has_cli_tool should return True when tool is on PATH."""
        ext = self._make_extractor(cli_tool="psql")
        with patch(
            "dvt.federation.extractors.base.shutil.which", return_value="/usr/bin/psql"
        ):
            assert ext._has_cli_tool() is True

    def test_has_cli_tool_when_missing(self):
        """_has_cli_tool should return False when tool is not on PATH."""
        ext = self._make_extractor(cli_tool="psql")
        with patch("dvt.federation.extractors.base.shutil.which", return_value=None):
            assert ext._has_cli_tool() is False

    def test_has_cli_tool_when_no_tool_name(self):
        """_has_cli_tool should return False when cli_tool is None."""
        ext = self._make_extractor(cli_tool=None)
        assert ext._has_cli_tool() is False

    def test_build_extraction_command_not_implemented(self):
        """Default _build_extraction_command should raise NotImplementedError."""
        from dvt.federation.extractors.base import ExtractionConfig

        ext = self._make_extractor()
        config = ExtractionConfig(source_name="test", schema="public", table="t1")
        with pytest.raises(NotImplementedError):
            ext._build_extraction_command(config)


class TestPostgresExtractorPipe:
    """Tests for PostgresExtractor pipe extraction."""

    def _make_extractor(self):
        from dvt.federation.extractors.postgres import PostgresExtractor

        return PostgresExtractor(
            connection=MagicMock(),
            dialect="postgres",
            connection_config={
                "host": "db.example.com",
                "port": 5432,
                "user": "dvt_user",
                "password": "secret123",
                "database": "mydb",
            },
        )

    def test_cli_tool_is_psql(self):
        """PostgresExtractor.cli_tool should be 'psql'."""
        ext = self._make_extractor()
        assert ext.cli_tool == "psql"

    def test_build_extraction_command(self):
        """Should build correct psql COPY command."""
        from dvt.federation.extractors.base import ExtractionConfig

        ext = self._make_extractor()
        config = ExtractionConfig(
            source_name="pg__orders",
            schema="public",
            table="orders",
            connection_config={
                "host": "db.example.com",
                "port": 5432,
                "user": "dvt_user",
                "password": "secret123",
                "database": "mydb",
            },
        )
        cmd = ext._build_extraction_command(config)

        assert cmd[0] == "psql"
        assert "-h" in cmd
        assert "db.example.com" in cmd
        assert "-U" in cmd
        assert "dvt_user" in cmd
        assert "-d" in cmd
        assert "mydb" in cmd
        assert "--no-psqlrc" in cmd
        assert "--quiet" in cmd
        # Should contain COPY ... TO STDOUT
        copy_arg = cmd[cmd.index("-c") + 1]
        assert "COPY" in copy_arg
        assert "TO STDOUT" in copy_arg
        assert "FORMAT csv" in copy_arg

    def test_build_extraction_env_sets_pgpassword(self):
        """Should set PGPASSWORD in environment."""
        from dvt.federation.extractors.base import ExtractionConfig

        ext = self._make_extractor()
        config = ExtractionConfig(
            source_name="pg__orders",
            schema="public",
            table="orders",
            connection_config={
                "host": "localhost",
                "password": "secret123",
            },
        )
        env = ext._build_extraction_env(config)
        assert env["PGPASSWORD"] == "secret123"

    def test_extract_tries_pipe_first(self):
        """extract() should try pipe when psql is available."""
        from dvt.federation.extractors.base import ExtractionConfig, ExtractionResult

        ext = self._make_extractor()
        config = ExtractionConfig(source_name="test", schema="public", table="t1")
        output_path = Path("/tmp/test.parquet")

        pipe_result = ExtractionResult(
            success=True, source_name="test", row_count=100, extraction_method="pipe"
        )

        with (
            patch.object(ext, "_has_cli_tool", return_value=True),
            patch.object(
                ext, "_extract_via_pipe", return_value=pipe_result
            ) as mock_pipe,
        ):
            result = ext.extract(config, output_path)

            mock_pipe.assert_called_once_with(config, output_path)
            assert result.extraction_method == "pipe"

    def test_extract_falls_back_on_pipe_failure(self):
        """extract() should fall back to streaming COPY if pipe fails."""
        from dvt.federation.extractors.base import ExtractionConfig, ExtractionResult

        ext = self._make_extractor()
        config = ExtractionConfig(source_name="test", schema="public", table="t1")
        output_path = Path("/tmp/test.parquet")

        streaming_result = ExtractionResult(
            success=True,
            source_name="test",
            row_count=100,
            extraction_method="copy_streaming",
        )

        with (
            patch.object(ext, "_has_cli_tool", return_value=True),
            patch.object(
                ext, "_extract_via_pipe", side_effect=RuntimeError("pipe failed")
            ),
            patch.object(
                ext, "_extract_copy_streaming", return_value=streaming_result
            ) as mock_streaming,
        ):
            result = ext.extract(config, output_path)

            mock_streaming.assert_called_once()
            assert result.extraction_method == "copy_streaming"


class TestMySQLExtractorPipe:
    """Tests for MySQLExtractor pipe extraction."""

    def _make_extractor(self):
        from dvt.federation.extractors.mysql import MySQLExtractor

        return MySQLExtractor(
            connection=MagicMock(),
            dialect="mysql",
            connection_config={
                "host": "mysql.example.com",
                "port": 3306,
                "user": "root",
                "password": "mysecret",
                "database": "mydb",
            },
        )

    def test_cli_tool_is_mysql(self):
        """MySQLExtractor.cli_tool should be 'mysql'."""
        ext = self._make_extractor()
        assert ext.cli_tool == "mysql"

    def test_build_extraction_command(self):
        """Should build correct mysql --batch command."""
        from dvt.federation.extractors.base import ExtractionConfig

        ext = self._make_extractor()
        config = ExtractionConfig(
            source_name="mysql__orders",
            schema="mydb",
            table="orders",
            connection_config={
                "host": "mysql.example.com",
                "port": 3306,
                "user": "root",
                "password": "mysecret",
                "database": "mydb",
            },
        )
        cmd = ext._build_extraction_command(config)

        assert cmd[0] == "mysql"
        assert "--batch" in cmd
        assert "--raw" in cmd
        assert "-h" in cmd
        assert "mysql.example.com" in cmd

    def test_build_extraction_env_sets_mysql_pwd(self):
        """Should set MYSQL_PWD in environment."""
        from dvt.federation.extractors.base import ExtractionConfig

        ext = self._make_extractor()
        config = ExtractionConfig(
            source_name="mysql__orders",
            schema="mydb",
            table="orders",
            connection_config={"password": "mysecret"},
        )
        env = ext._build_extraction_env(config)
        assert env["MYSQL_PWD"] == "mysecret"

    def test_csv_parse_options_tab_delimited(self):
        """MySQL extractor should configure tab-delimited parsing."""
        ext = self._make_extractor()
        with patch.dict("sys.modules", {}):
            opts = ext._get_csv_parse_options()
            if opts is not None:
                assert opts.delimiter == "\t"

    def test_extract_tries_pipe_first(self):
        """extract() should try pipe when mysql CLI is available."""
        from dvt.federation.extractors.base import ExtractionConfig, ExtractionResult

        ext = self._make_extractor()
        config = ExtractionConfig(source_name="test", schema="mydb", table="t1")
        output_path = Path("/tmp/test.parquet")

        pipe_result = ExtractionResult(
            success=True, source_name="test", row_count=100, extraction_method="pipe"
        )

        with (
            patch.object(ext, "_has_cli_tool", return_value=True),
            patch.object(
                ext, "_extract_via_pipe", return_value=pipe_result
            ) as mock_pipe,
        ):
            result = ext.extract(config, output_path)
            mock_pipe.assert_called_once()
            assert result.extraction_method == "pipe"

    def test_extract_falls_back_to_jdbc(self):
        """extract() should fall back to JDBC if pipe fails."""
        from dvt.federation.extractors.base import ExtractionConfig, ExtractionResult

        ext = self._make_extractor()
        config = ExtractionConfig(source_name="test", schema="mydb", table="t1")
        output_path = Path("/tmp/test.parquet")

        jdbc_result = ExtractionResult(
            success=True, source_name="test", row_count=100, extraction_method="jdbc"
        )

        with (
            patch.object(ext, "_has_cli_tool", return_value=True),
            patch.object(ext, "_extract_via_pipe", side_effect=RuntimeError("fail")),
            patch.object(ext, "_extract_jdbc", return_value=jdbc_result) as mock_jdbc,
        ):
            result = ext.extract(config, output_path)
            mock_jdbc.assert_called_once()
            assert result.extraction_method == "jdbc"


class TestSQLServerExtractorPipe:
    """Tests for SQLServerExtractor pipe extraction."""

    def _make_extractor(self):
        from dvt.federation.extractors.sqlserver import SQLServerExtractor

        return SQLServerExtractor(
            connection=MagicMock(),
            dialect="sqlserver",
            connection_config={
                "host": "sql.example.com",
                "port": 1433,
                "user": "sa",
                "password": "SqlPass!",
                "database": "mydb",
            },
        )

    def test_cli_tool_is_bcp(self):
        """SQLServerExtractor.cli_tool should be 'bcp'."""
        ext = self._make_extractor()
        assert ext.cli_tool == "bcp"

    def test_build_extraction_command(self):
        """Should build correct bcp queryout command."""
        from dvt.federation.extractors.base import ExtractionConfig

        ext = self._make_extractor()
        config = ExtractionConfig(
            source_name="sql__orders",
            schema="dbo",
            table="orders",
            connection_config={
                "host": "sql.example.com",
                "port": 1433,
                "user": "sa",
                "password": "SqlPass!",
                "database": "mydb",
            },
        )
        cmd = ext._build_extraction_command(config)

        assert cmd[0] == "bcp"
        assert "queryout" in cmd
        assert "/dev/stdout" in cmd
        assert "-S" in cmd
        assert "sql.example.com,1433" in cmd

    def test_extract_tries_pipe_first(self):
        """extract() should try pipe when bcp is available."""
        from dvt.federation.extractors.base import ExtractionConfig, ExtractionResult

        ext = self._make_extractor()
        config = ExtractionConfig(source_name="test", schema="dbo", table="t1")
        output_path = Path("/tmp/test.parquet")

        pipe_result = ExtractionResult(
            success=True, source_name="test", row_count=50, extraction_method="pipe"
        )

        with (
            patch.object(ext, "_has_cli_tool", return_value=True),
            patch.object(ext, "_extract_via_pipe", return_value=pipe_result),
        ):
            result = ext.extract(config, output_path)
            assert result.extraction_method == "pipe"


class TestClickHouseExtractorPipe:
    """Tests for ClickHouseExtractor pipe extraction."""

    def _make_extractor(self):
        from dvt.federation.extractors.clickhouse import ClickHouseExtractor

        return ClickHouseExtractor(
            connection=MagicMock(),
            dialect="clickhouse",
            connection_config={
                "host": "ch.example.com",
                "port": 9000,
                "user": "default",
                "password": "chpass",
                "database": "default",
            },
        )

    def test_cli_tool_is_clickhouse_client(self):
        """ClickHouseExtractor.cli_tool should be 'clickhouse-client'."""
        ext = self._make_extractor()
        assert ext.cli_tool == "clickhouse-client"

    def test_build_extraction_command(self):
        """Should build correct clickhouse-client command with CSVWithNames.

        Password is passed via CLICKHOUSE_PASSWORD env var (not --password flag)
        to avoid exposing secrets in process listings.
        """
        from dvt.federation.extractors.base import ExtractionConfig

        ext = self._make_extractor()
        config = ExtractionConfig(
            source_name="ch__events",
            schema="default",
            table="events",
            connection_config={
                "host": "ch.example.com",
                "port": 9000,
                "user": "default",
                "password": "chpass",
                "database": "default",
            },
        )
        cmd = ext._build_extraction_command(config)

        assert cmd[0] == "clickhouse-client"
        assert "--host" in cmd
        assert "ch.example.com" in cmd
        # Password must NOT appear in command args (security)
        assert "--password" not in cmd
        # Password goes via env var instead
        env = ext._build_extraction_env(config)
        assert env.get("CLICKHOUSE_PASSWORD") == "chpass"
        # Query should include FORMAT CSVWithNames
        query_idx = cmd.index("--query") + 1
        assert "FORMAT CSVWithNames" in cmd[query_idx]

    def test_build_extraction_command_no_password(self):
        """Should omit --password flag when password is empty."""
        from dvt.federation.extractors.base import ExtractionConfig

        ext = self._make_extractor()
        config = ExtractionConfig(
            source_name="ch__events",
            schema="default",
            table="events",
            connection_config={
                "host": "localhost",
                "port": 9000,
                "user": "default",
                "password": "",
                "database": "default",
            },
        )
        cmd = ext._build_extraction_command(config)
        assert "--password" not in cmd

    def test_extract_tries_pipe_first(self):
        """extract() should try pipe when clickhouse-client is available."""
        from dvt.federation.extractors.base import ExtractionConfig, ExtractionResult

        ext = self._make_extractor()
        config = ExtractionConfig(source_name="test", schema="default", table="t1")
        output_path = Path("/tmp/test.parquet")

        pipe_result = ExtractionResult(
            success=True, source_name="test", row_count=200, extraction_method="pipe"
        )

        with (
            patch.object(ext, "_has_cli_tool", return_value=True),
            patch.object(ext, "_extract_via_pipe", return_value=pipe_result),
        ):
            result = ext.extract(config, output_path)
            assert result.extraction_method == "pipe"


# =============================================================================
# Pipe Loading Tests
# =============================================================================


class TestBaseLoaderPipe:
    """Tests for shared _load_via_pipe in BaseLoader."""

    def _make_loader(self, cli_tool=None):
        """Create a minimal loader with pipe support."""
        from dvt.federation.loaders.base import BaseLoader, LoadConfig, LoadResult

        class TestLoader(BaseLoader):
            adapter_types = ["test"]

            def load(self, df, config, adapter=None):
                pass

        loader = TestLoader()
        if cli_tool:
            loader.cli_tool = cli_tool
        return loader

    def test_has_cli_tool_when_present(self):
        """_has_cli_tool should return True when tool is on PATH."""
        loader = self._make_loader(cli_tool="psql")
        with patch(
            "dvt.federation.loaders.base.shutil.which", return_value="/usr/bin/psql"
        ):
            assert loader._has_cli_tool() is True

    def test_has_cli_tool_when_missing(self):
        """_has_cli_tool should return False when tool is not on PATH."""
        loader = self._make_loader(cli_tool="psql")
        with patch("dvt.federation.loaders.base.shutil.which", return_value=None):
            assert loader._has_cli_tool() is False

    def test_has_cli_tool_when_no_tool_name(self):
        """_has_cli_tool should return False when cli_tool is None."""
        loader = self._make_loader()
        assert loader._has_cli_tool() is False

    def test_build_load_command_not_implemented(self):
        """Default _build_load_command should raise NotImplementedError."""
        from dvt.federation.loaders.base import LoadConfig

        loader = self._make_loader()
        config = LoadConfig(table_name="test.t1")
        with pytest.raises(NotImplementedError):
            loader._build_load_command(config)

    def test_default_csv_delimiter(self):
        """Default CSV delimiter should be comma."""
        loader = self._make_loader()
        assert loader._get_csv_write_delimiter() == ","


class TestPostgresLoaderPipe:
    """Tests for PostgresLoader pipe loading."""

    def _make_loader(self):
        from dvt.federation.loaders.postgres import PostgresLoader

        return PostgresLoader()

    def test_cli_tool_is_psql(self):
        """PostgresLoader.cli_tool should be 'psql'."""
        loader = self._make_loader()
        assert loader.cli_tool == "psql"

    def test_build_load_command(self):
        """Should build correct psql COPY FROM STDIN command."""
        from dvt.federation.loaders.base import LoadConfig

        loader = self._make_loader()
        config = LoadConfig(
            table_name="public.orders",
            connection_config={
                "host": "db.example.com",
                "port": 5432,
                "user": "dvt_user",
                "password": "secret",
                "database": "mydb",
            },
        )
        cmd = loader._build_load_command(config)

        assert cmd[0] == "psql"
        assert "-h" in cmd
        assert "db.example.com" in cmd
        assert "-d" in cmd
        assert "mydb" in cmd
        copy_arg = cmd[cmd.index("-c") + 1]
        assert "COPY" in copy_arg
        assert "FROM STDIN" in copy_arg
        assert "FORMAT csv" in copy_arg

    def test_build_load_env_sets_pgpassword(self):
        """Should set PGPASSWORD in environment."""
        from dvt.federation.loaders.base import LoadConfig

        loader = self._make_loader()
        config = LoadConfig(
            table_name="public.orders",
            connection_config={"password": "secret123"},
        )
        env = loader._build_load_env(config)
        assert env["PGPASSWORD"] == "secret123"

    def test_load_tries_pipe_first(self):
        """load() should try pipe when psql is available."""
        from dvt.federation.loaders.base import LoadConfig, LoadResult

        loader = self._make_loader()
        config = LoadConfig(table_name="public.orders")
        df = MagicMock()

        pipe_result = LoadResult(
            success=True, table_name="public.orders", row_count=100, load_method="pipe"
        )

        with (
            patch.object(loader, "_has_cli_tool", return_value=True),
            patch.object(loader, "_load_pipe", return_value=pipe_result) as mock_pipe,
        ):
            result = loader.load(df, config)
            mock_pipe.assert_called_once()
            assert result.load_method == "pipe"

    def test_load_falls_back_on_pipe_failure(self):
        """load() should fall back to streaming COPY if pipe fails."""
        from dvt.federation.loaders.base import LoadConfig, LoadResult

        loader = self._make_loader()
        config = LoadConfig(table_name="public.orders")
        df = MagicMock()

        streaming_result = LoadResult(
            success=True,
            table_name="public.orders",
            row_count=100,
            load_method="copy_streaming",
        )

        with (
            patch.object(loader, "_has_cli_tool", return_value=True),
            patch.object(loader, "_load_pipe", side_effect=RuntimeError("pipe failed")),
            patch.object(
                loader, "_load_copy_streaming", return_value=streaming_result
            ) as mock_s,
        ):
            result = loader.load(df, config)
            mock_s.assert_called_once()
            assert result.load_method == "copy_streaming"

    def test_load_skips_pipe_when_no_tool(self):
        """load() should skip pipe when psql is not available."""
        from dvt.federation.loaders.base import LoadConfig, LoadResult

        loader = self._make_loader()
        config = LoadConfig(table_name="public.orders")
        df = MagicMock()

        streaming_result = LoadResult(
            success=True,
            table_name="public.orders",
            row_count=100,
            load_method="copy_streaming",
        )

        with (
            patch.object(loader, "_has_cli_tool", return_value=False),
            patch.object(
                loader, "_load_copy_streaming", return_value=streaming_result
            ) as mock_s,
        ):
            result = loader.load(df, config)
            mock_s.assert_called_once()


class TestGenericLoaderPipe:
    """Tests for GenericLoader pipe loading."""

    def _make_loader(self):
        from dvt.federation.loaders.generic import GenericLoader

        return GenericLoader()

    def test_detect_cli_tool_mysql(self):
        """Should detect mysql CLI for mysql adapter."""
        loader = self._make_loader()
        from dvt.federation.loaders.base import LoadConfig

        config = LoadConfig(
            table_name="mydb.orders",
            connection_config={"type": "mysql"},
        )

        with patch(
            "dvt.federation.loaders.generic.shutil.which", return_value="/usr/bin/mysql"
        ):
            tool = loader._detect_cli_tool(config)
            assert tool == "mysql"

    def test_detect_cli_tool_missing(self):
        """Should return None when CLI tool is not on PATH."""
        loader = self._make_loader()
        from dvt.federation.loaders.base import LoadConfig

        config = LoadConfig(
            table_name="mydb.orders",
            connection_config={"type": "mysql"},
        )

        with patch("dvt.federation.loaders.generic.shutil.which", return_value=None):
            tool = loader._detect_cli_tool(config)
            assert tool is None

    def test_detect_cli_tool_unknown_adapter(self):
        """Should return None for unknown adapter types."""
        loader = self._make_loader()
        from dvt.federation.loaders.base import LoadConfig

        config = LoadConfig(
            table_name="mydb.orders",
            connection_config={"type": "snowflake"},
        )
        tool = loader._detect_cli_tool(config)
        assert tool is None

    def test_build_mysql_load_command(self):
        """Should build correct mysql LOAD DATA command."""
        loader = self._make_loader()
        from dvt.federation.loaders.base import LoadConfig

        config = LoadConfig(
            table_name="mydb.orders",
            connection_config={
                "type": "mysql",
                "host": "mysql.example.com",
                "port": 3306,
                "user": "root",
                "password": "pass",
                "database": "mydb",
            },
        )
        cmd = loader._build_mysql_load_command(config)
        assert cmd[0] == "mysql"
        assert "--local-infile=1" in cmd
        assert "LOAD DATA LOCAL INFILE" in cmd[-1]

    def test_build_sqlserver_load_command(self):
        """Should build correct bcp IN command."""
        loader = self._make_loader()
        from dvt.federation.loaders.base import LoadConfig

        config = LoadConfig(
            table_name="dbo.orders",
            connection_config={
                "type": "sqlserver",
                "host": "sql.example.com",
                "port": 1433,
                "user": "sa",
                "password": "SqlPass!",
                "database": "mydb",
            },
        )
        cmd = loader._build_sqlserver_load_command(config)
        assert cmd[0] == "bcp"
        assert "dbo.orders" in cmd
        assert "in" in cmd
        assert "/dev/stdin" in cmd

    def test_build_clickhouse_load_command(self):
        """Should build correct clickhouse-client INSERT command."""
        loader = self._make_loader()
        from dvt.federation.loaders.base import LoadConfig

        config = LoadConfig(
            table_name="events",
            connection_config={
                "type": "clickhouse",
                "host": "ch.example.com",
                "port": 9000,
                "user": "default",
                "password": "chpass",
                "database": "default",
            },
        )
        cmd = loader._build_clickhouse_load_command(config)
        assert cmd[0] == "clickhouse-client"
        assert "INSERT INTO events FORMAT CSVWithNames" in cmd[cmd.index("--query") + 1]

    def test_build_load_env_sets_mysql_pwd(self):
        """Should set MYSQL_PWD for MySQL adapters."""
        loader = self._make_loader()
        from dvt.federation.loaders.base import LoadConfig

        config = LoadConfig(
            table_name="mydb.orders",
            connection_config={
                "type": "mysql",
                "password": "mysecret",
            },
        )
        env = loader._build_load_env(config)
        assert env["MYSQL_PWD"] == "mysecret"

    def test_load_tries_pipe_for_mysql(self):
        """load() should try pipe when mysql CLI is available."""
        from dvt.federation.loaders.base import LoadConfig, LoadResult

        loader = self._make_loader()
        config = LoadConfig(
            table_name="mydb.orders",
            connection_config={"type": "mysql"},
        )
        df = MagicMock()

        pipe_result = LoadResult(
            success=True, table_name="mydb.orders", row_count=100, load_method="pipe"
        )

        with (
            patch.object(loader, "_detect_cli_tool", return_value="mysql"),
            patch.object(loader, "_load_pipe", return_value=pipe_result) as mock_pipe,
        ):
            result = loader.load(df, config)
            mock_pipe.assert_called_once()
            assert result.load_method == "pipe"

    def test_load_falls_back_to_jdbc(self):
        """load() should fall back to JDBC when no CLI tool available."""
        from dvt.federation.loaders.base import LoadConfig, LoadResult

        loader = self._make_loader()
        config = LoadConfig(
            table_name="mydb.orders",
            connection_config={"type": "snowflake"},
        )
        df = MagicMock()

        jdbc_result = LoadResult(
            success=True, table_name="mydb.orders", row_count=100, load_method="jdbc"
        )

        with patch.object(loader, "_load_jdbc", return_value=jdbc_result) as mock_jdbc:
            result = loader.load(df, config)
            mock_jdbc.assert_called_once()
            assert result.load_method == "jdbc"

    def test_load_falls_back_on_pipe_failure(self):
        """load() should fall back to JDBC if pipe load fails."""
        from dvt.federation.loaders.base import LoadConfig, LoadResult

        loader = self._make_loader()
        config = LoadConfig(
            table_name="mydb.orders",
            connection_config={"type": "mysql"},
        )
        df = MagicMock()

        jdbc_result = LoadResult(
            success=True, table_name="mydb.orders", row_count=100, load_method="jdbc"
        )

        with (
            patch.object(loader, "_detect_cli_tool", return_value="mysql"),
            patch.object(loader, "_load_pipe", side_effect=RuntimeError("pipe failed")),
            patch.object(loader, "_load_jdbc", return_value=jdbc_result) as mock_jdbc,
        ):
            result = loader.load(df, config)
            mock_jdbc.assert_called_once()
            assert result.load_method == "jdbc"


# =============================================================================
# ExtractionResult / LoadResult Method Field Tests
# =============================================================================


class TestResultMethodFields:
    """Tests that pipe methods properly set extraction_method/load_method."""

    def test_extraction_result_has_pipe_method(self):
        """ExtractionResult should accept 'pipe' as extraction_method."""
        from dvt.federation.extractors.base import ExtractionResult

        result = ExtractionResult(
            success=True,
            source_name="test",
            row_count=100,
            extraction_method="pipe",
        )
        assert result.extraction_method == "pipe"

    def test_load_result_has_pipe_method(self):
        """LoadResult should accept 'pipe' as load_method."""
        from dvt.federation.loaders.base import LoadResult

        result = LoadResult(
            success=True,
            table_name="test.t1",
            row_count=100,
            load_method="pipe",
        )
        assert result.load_method == "pipe"
