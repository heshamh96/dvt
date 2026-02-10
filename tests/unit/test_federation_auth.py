# coding=utf-8
"""Unit tests for DVT federation authentication handlers.

Tests the authentication handler system that enables DVT federation
(Spark JDBC, native SQL) to support all authentication methods for
all 28 supported database adapters.
"""

import json
import os
import tempfile
from typing import Dict, Any
from unittest import mock

import pytest

from dvt.federation.auth import (
    AUTH_HANDLERS,
    get_auth_handler,
    get_supported_adapters,
    is_adapter_supported,
)
from dvt.federation.auth.base import BaseAuthHandler
from dvt.federation.auth.snowflake import SnowflakeAuthHandler
from dvt.federation.auth.databricks import DatabricksAuthHandler
from dvt.federation.auth.bigquery import BigQueryAuthHandler
from dvt.federation.auth.redshift import RedshiftAuthHandler
from dvt.federation.auth.postgres import PostgresAuthHandler
from dvt.federation.auth.mysql import MySQLAuthHandler
from dvt.federation.auth.sqlserver import SQLServerAuthHandler
from dvt.federation.auth.oracle import OracleAuthHandler
from dvt.federation.auth.trino import TrinoAuthHandler
from dvt.federation.auth.clickhouse import ClickHouseAuthHandler
from dvt.federation.auth.athena import AthenaAuthHandler
from dvt.federation.auth.exasol import ExasolAuthHandler
from dvt.federation.auth.vertica import VerticaAuthHandler
from dvt.federation.auth.duckdb import DuckDBAuthHandler
from dvt.federation.auth.firebolt import FireboltAuthHandler
from dvt.federation.auth.db2 import DB2AuthHandler
from dvt.federation.auth.hive import HiveAuthHandler
from dvt.federation.auth.impala import ImpalaAuthHandler
from dvt.federation.auth.generic import GenericAuthHandler


# =============================================================================
# Factory and Registry Tests
# =============================================================================


class TestAuthHandlerFactory:
    """Tests for get_auth_handler factory function."""

    def test_get_snowflake_handler(self):
        """Should return SnowflakeAuthHandler for snowflake."""
        handler = get_auth_handler("snowflake")
        assert isinstance(handler, SnowflakeAuthHandler)

    def test_get_postgres_handler(self):
        """Should return PostgresAuthHandler for postgres."""
        handler = get_auth_handler("postgres")
        assert isinstance(handler, PostgresAuthHandler)

    def test_get_bigquery_handler(self):
        """Should return BigQueryAuthHandler for bigquery."""
        handler = get_auth_handler("bigquery")
        assert isinstance(handler, BigQueryAuthHandler)

    def test_get_unknown_adapter_returns_generic(self):
        """Unknown adapters should return GenericAuthHandler."""
        handler = get_auth_handler("unknown_database")
        assert isinstance(handler, GenericAuthHandler)

    def test_case_insensitive_lookup(self):
        """Adapter type lookup should be case-insensitive."""
        handler_lower = get_auth_handler("snowflake")
        handler_upper = get_auth_handler("SNOWFLAKE")
        handler_mixed = get_auth_handler("Snowflake")

        assert isinstance(handler_lower, SnowflakeAuthHandler)
        assert isinstance(handler_upper, SnowflakeAuthHandler)
        assert isinstance(handler_mixed, SnowflakeAuthHandler)

    def test_postgres_compatible_adapters_use_postgres_handler(self):
        """PostgreSQL-compatible databases should use PostgresAuthHandler."""
        for adapter in [
            "greenplum",
            "materialize",
            "risingwave",
            "cratedb",
            "alloydb",
            "timescaledb",
        ]:
            handler = get_auth_handler(adapter)
            assert isinstance(handler, PostgresAuthHandler), (
                f"{adapter} should use PostgresAuthHandler"
            )

    def test_mysql_compatible_adapters_use_mysql_handler(self):
        """MySQL-compatible databases should use MySQLAuthHandler."""
        for adapter in ["mysql", "tidb", "singlestore"]:
            handler = get_auth_handler(adapter)
            assert isinstance(handler, MySQLAuthHandler), (
                f"{adapter} should use MySQLAuthHandler"
            )

    def test_microsoft_adapters_use_sqlserver_handler(self):
        """Microsoft databases should use SQLServerAuthHandler."""
        for adapter in ["sqlserver", "synapse", "fabric"]:
            handler = get_auth_handler(adapter)
            assert isinstance(handler, SQLServerAuthHandler), (
                f"{adapter} should use SQLServerAuthHandler"
            )


class TestAdapterRegistry:
    """Tests for AUTH_HANDLERS registry and helper functions."""

    def test_is_adapter_supported_returns_true_for_known(self):
        """is_adapter_supported should return True for known adapters."""
        assert is_adapter_supported("snowflake") is True
        assert is_adapter_supported("postgres") is True
        assert is_adapter_supported("bigquery") is True

    def test_is_adapter_supported_returns_false_for_unknown(self):
        """is_adapter_supported should return False for unknown adapters."""
        assert is_adapter_supported("unknown_database") is False
        assert is_adapter_supported("nosql_db") is False

    def test_get_supported_adapters_returns_list(self):
        """get_supported_adapters should return list of adapter names."""
        adapters = get_supported_adapters()
        assert isinstance(adapters, list)
        assert len(adapters) > 20  # We have 28+ adapters registered
        assert "snowflake" in adapters
        assert "postgres" in adapters
        assert "bigquery" in adapters

    def test_all_registered_handlers_are_valid(self):
        """All registered handlers should be BaseAuthHandler subclasses."""
        for adapter, handler_class in AUTH_HANDLERS.items():
            assert issubclass(handler_class, BaseAuthHandler), (
                f"{adapter} handler should be BaseAuthHandler subclass"
            )


# =============================================================================
# Snowflake Auth Handler Tests
# =============================================================================


class TestSnowflakeAuthHandler:
    """Tests for SnowflakeAuthHandler."""

    def setup_method(self):
        """Set up test fixtures."""
        self.handler = SnowflakeAuthHandler()

    def test_detect_password_auth(self):
        """Should detect password authentication."""
        creds = {
            "type": "snowflake",
            "user": "test_user",
            "password": "test_pass",
            "account": "test_account",
        }
        assert self.handler.detect_auth_method(creds) == "password"

    def test_detect_keypair_auth_from_path(self):
        """Should detect keypair auth from private_key_path."""
        creds = {
            "type": "snowflake",
            "user": "test_user",
            "private_key_path": "/path/to/key.p8",
            "account": "test_account",
        }
        assert self.handler.detect_auth_method(creds) == "keypair"

    def test_detect_keypair_auth_from_inline_key(self):
        """Should detect keypair auth from inline private_key."""
        creds = {
            "type": "snowflake",
            "user": "test_user",
            "private_key": "-----BEGIN PRIVATE KEY-----...",
            "account": "test_account",
        }
        assert self.handler.detect_auth_method(creds) == "keypair"

    def test_detect_oauth_auth(self):
        """Should detect OAuth authentication."""
        creds = {
            "type": "snowflake",
            "authenticator": "oauth",
            "token": "oauth_token_value",
            "account": "test_account",
        }
        assert self.handler.detect_auth_method(creds) == "oauth"

    def test_detect_externalbrowser_auth(self):
        """Should detect externalbrowser (SSO) authentication."""
        creds = {
            "type": "snowflake",
            "authenticator": "externalbrowser",
            "user": "test_user",
            "account": "test_account",
        }
        assert self.handler.detect_auth_method(creds) == "externalbrowser"

    def test_externalbrowser_is_interactive(self):
        """Externalbrowser should be detected as interactive."""
        creds = {
            "type": "snowflake",
            "authenticator": "externalbrowser",
            "user": "test_user",
            "account": "test_account",
        }
        assert self.handler.is_interactive(creds) is True

    def test_password_is_not_interactive(self):
        """Password auth should not be interactive."""
        creds = {
            "type": "snowflake",
            "user": "test_user",
            "password": "test_pass",
            "account": "test_account",
        }
        assert self.handler.is_interactive(creds) is False

    def test_validate_blocks_externalbrowser(self):
        """Validate should return error for externalbrowser auth."""
        creds = {
            "type": "snowflake",
            "authenticator": "externalbrowser",
            "user": "test_user",
            "account": "test_account",
        }
        is_valid, error = self.handler.validate(creds)
        assert is_valid is False
        assert "externalbrowser" in error.lower()
        assert "browser" in error.lower() or "interactive" in error.lower()

    def test_validate_allows_password(self):
        """Validate should pass for password auth."""
        creds = {
            "type": "snowflake",
            "user": "test_user",
            "password": "test_pass",
            "account": "test_account",
        }
        is_valid, error = self.handler.validate(creds)
        assert is_valid is True
        assert error is None

    def test_get_jdbc_properties_password(self):
        """Should build correct JDBC properties for password auth."""
        creds = {
            "type": "snowflake",
            "user": "test_user",
            "password": "test_pass",
            "account": "test_account",
            "warehouse": "my_wh",
            "role": "my_role",
        }
        props = self.handler.get_jdbc_properties(creds)

        assert props["user"] == "test_user"
        assert props["password"] == "test_pass"
        assert props["warehouse"] == "my_wh"
        assert props["role"] == "my_role"

    def test_get_jdbc_properties_keypair(self):
        """Should build correct JDBC properties for keypair auth."""
        creds = {
            "type": "snowflake",
            "user": "test_user",
            "private_key_path": "/path/to/key.p8",
            "private_key_passphrase": "key_password",
            "account": "test_account",
        }
        props = self.handler.get_jdbc_properties(creds)

        assert props["user"] == "test_user"
        assert props["private_key_file"] == "/path/to/key.p8"
        assert props["private_key_file_pwd"] == "key_password"

    def test_get_native_connection_kwargs_password(self):
        """Should build correct native kwargs for password auth."""
        creds = {
            "type": "snowflake",
            "user": "test_user",
            "password": "test_pass",
            "account": "test_account",
            "database": "my_db",
            "schema": "my_schema",
            "warehouse": "my_wh",
        }
        kwargs = self.handler.get_native_connection_kwargs(creds)

        assert kwargs["user"] == "test_user"
        assert kwargs["password"] == "test_pass"
        assert kwargs["account"] == "test_account"
        assert kwargs["database"] == "my_db"
        assert kwargs["schema"] == "my_schema"
        assert kwargs["warehouse"] == "my_wh"


# =============================================================================
# Databricks Auth Handler Tests
# =============================================================================


class TestDatabricksAuthHandler:
    """Tests for DatabricksAuthHandler."""

    def setup_method(self):
        """Set up test fixtures."""
        self.handler = DatabricksAuthHandler()

    def test_detect_token_auth(self):
        """Should detect token authentication."""
        creds = {
            "type": "databricks",
            "token": "dapi_token_value",
            "host": "https://abc.cloud.databricks.com",
        }
        assert self.handler.detect_auth_method(creds) == "token"

    def test_detect_oauth_m2m_auth(self):
        """Should detect OAuth M2M (machine-to-machine) authentication."""
        creds = {
            "type": "databricks",
            "auth_type": "oauth",
            "client_id": "my_client_id",
            "client_secret": "my_client_secret",
            "host": "https://abc.cloud.databricks.com",
        }
        assert self.handler.detect_auth_method(creds) == "oauth_m2m"

    def test_detect_oauth_u2m_auth(self):
        """Should detect OAuth U2M (user-to-machine) authentication."""
        creds = {
            "type": "databricks",
            "auth_type": "oauth",
            "host": "https://abc.cloud.databricks.com",
        }
        assert self.handler.detect_auth_method(creds) == "oauth_u2m"

    def test_oauth_u2m_is_interactive(self):
        """OAuth U2M should be detected as interactive."""
        creds = {
            "type": "databricks",
            "auth_type": "oauth",
            "host": "https://abc.cloud.databricks.com",
        }
        assert self.handler.is_interactive(creds) is True

    def test_token_is_not_interactive(self):
        """Token auth should not be interactive."""
        creds = {
            "type": "databricks",
            "token": "dapi_token_value",
            "host": "https://abc.cloud.databricks.com",
        }
        assert self.handler.is_interactive(creds) is False

    def test_validate_blocks_oauth_u2m(self):
        """Validate should return error for OAuth U2M."""
        creds = {
            "type": "databricks",
            "auth_type": "oauth",
            "host": "https://abc.cloud.databricks.com",
        }
        is_valid, error = self.handler.validate(creds)
        assert is_valid is False
        assert "oauth_u2m" in error.lower() or "browser" in error.lower()

    def test_get_jdbc_properties_token(self):
        """Should build correct JDBC properties for token auth."""
        creds = {
            "type": "databricks",
            "token": "dapi_token_value",
            "host": "https://abc.cloud.databricks.com",
            "http_path": "/sql/1.0/warehouses/xxx",
        }
        props = self.handler.get_jdbc_properties(creds)

        # Databricks JDBC uses user="token", password=<access_token>
        assert props["user"] == "token"
        assert props["password"] == "dapi_token_value"


# =============================================================================
# BigQuery Auth Handler Tests
# =============================================================================


class TestBigQueryAuthHandler:
    """Tests for BigQueryAuthHandler."""

    def setup_method(self):
        """Set up test fixtures."""
        self.handler = BigQueryAuthHandler()

    def test_detect_oauth_auth(self):
        """Should detect OAuth authentication (default)."""
        creds = {
            "type": "bigquery",
            "project": "my-project",
        }
        assert self.handler.detect_auth_method(creds) == "oauth"

    def test_detect_service_account_auth(self):
        """Should detect service account file auth."""
        creds = {
            "type": "bigquery",
            "project": "my-project",
            "keyfile": "/path/to/keyfile.json",
        }
        assert self.handler.detect_auth_method(creds) == "service_account"

    def test_detect_service_account_json_auth(self):
        """Should detect inline service account JSON auth."""
        creds = {
            "type": "bigquery",
            "project": "my-project",
            "keyfile_json": {"client_email": "sa@proj.iam.gserviceaccount.com"},
        }
        assert self.handler.detect_auth_method(creds) == "service_account_json"

    def test_detect_service_account_from_method(self):
        """Should detect service account from method field."""
        creds = {
            "type": "bigquery",
            "project": "my-project",
            "method": "service-account",
            "keyfile": "/path/to/keyfile.json",
        }
        assert self.handler.detect_auth_method(creds) == "service_account"

    def test_no_interactive_auth_methods(self):
        """BigQuery should have no interactive auth methods."""
        assert len(self.handler.INTERACTIVE_AUTH_METHODS) == 0

    def test_oauth_is_not_interactive(self):
        """OAuth (gcloud) should not be interactive."""
        creds = {
            "type": "bigquery",
            "project": "my-project",
        }
        assert self.handler.is_interactive(creds) is False

    def test_get_spark_connector_options_oauth(self):
        """Should build correct Spark connector options for OAuth."""
        creds = {
            "type": "bigquery",
            "project": "my-project",
            "dataset": "my_dataset",
        }
        options = self.handler.get_spark_connector_options(creds)

        assert options["project"] == "my-project"
        assert options["parentProject"] == "my-project"
        assert options["dataset"] == "my_dataset"

    def test_get_spark_connector_options_service_account(self):
        """Should build correct Spark connector options for service account."""
        creds = {
            "type": "bigquery",
            "project": "my-project",
            "keyfile": "/path/to/keyfile.json",
        }
        options = self.handler.get_spark_connector_options(creds)

        assert options["credentialsFile"] == "/path/to/keyfile.json"

    def test_supports_native_connector(self):
        """BigQuery should support native Spark connector."""
        assert self.handler.supports_native_connector() is True

    def test_use_native_connector(self):
        """BigQuery should prefer native connector."""
        creds = {"type": "bigquery", "project": "my-project"}
        assert self.handler.use_native_connector(creds) is True


# =============================================================================
# PostgreSQL Auth Handler Tests
# =============================================================================


class TestPostgresAuthHandler:
    """Tests for PostgresAuthHandler."""

    def setup_method(self):
        """Set up test fixtures."""
        self.handler = PostgresAuthHandler()

    def test_detect_password_auth(self):
        """Should detect password authentication."""
        creds = {
            "type": "postgres",
            "user": "test_user",
            "password": "test_pass",
            "host": "localhost",
        }
        assert self.handler.detect_auth_method(creds) == "password"

    def test_detect_ssl_cert_auth(self):
        """Should detect SSL certificate authentication."""
        creds = {
            "type": "postgres",
            "user": "test_user",
            "password": "test_pass",
            "host": "localhost",
            "sslcert": "/path/to/cert.pem",
            "sslkey": "/path/to/key.pem",
        }
        assert self.handler.detect_auth_method(creds) == "ssl_cert"

    def test_no_interactive_auth_methods(self):
        """PostgreSQL should have no interactive auth methods."""
        assert len(self.handler.INTERACTIVE_AUTH_METHODS) == 0

    def test_get_jdbc_properties_password(self):
        """Should build correct JDBC properties for password auth."""
        creds = {
            "type": "postgres",
            "user": "test_user",
            "password": "test_pass",
            "host": "localhost",
            "sslmode": "require",
        }
        props = self.handler.get_jdbc_properties(creds)

        assert props["user"] == "test_user"
        assert props["password"] == "test_pass"
        assert props["sslmode"] == "require"
        assert props["ssl"] == "true"

    def test_get_jdbc_properties_with_ssl_certs(self):
        """Should include SSL certificate paths in JDBC properties."""
        creds = {
            "type": "postgres",
            "user": "test_user",
            "password": "test_pass",
            "host": "localhost",
            "sslmode": "verify-full",
            "sslcert": "/path/to/cert.pem",
            "sslkey": "/path/to/key.pem",
            "sslrootcert": "/path/to/ca.pem",
        }
        props = self.handler.get_jdbc_properties(creds)

        assert props["sslcert"] == "/path/to/cert.pem"
        assert props["sslkey"] == "/path/to/key.pem"
        assert props["sslrootcert"] == "/path/to/ca.pem"

    def test_get_native_connection_kwargs(self):
        """Should build correct native kwargs for psycopg2."""
        creds = {
            "type": "postgres",
            "user": "test_user",
            "password": "test_pass",
            "host": "db.example.com",
            "port": 5432,
            "database": "my_database",
        }
        kwargs = self.handler.get_native_connection_kwargs(creds)

        assert kwargs["user"] == "test_user"
        assert kwargs["password"] == "test_pass"
        assert kwargs["host"] == "db.example.com"
        assert kwargs["port"] == 5432
        assert kwargs["dbname"] == "my_database"


# =============================================================================
# Redshift Auth Handler Tests
# =============================================================================


class TestRedshiftAuthHandler:
    """Tests for RedshiftAuthHandler."""

    def setup_method(self):
        """Set up test fixtures."""
        self.handler = RedshiftAuthHandler()

    def test_detect_password_auth(self):
        """Should detect password authentication."""
        creds = {
            "type": "redshift",
            "user": "test_user",
            "password": "test_pass",
            "host": "cluster.region.redshift.amazonaws.com",
        }
        assert self.handler.detect_auth_method(creds) == "password"

    def test_detect_iam_auth(self):
        """Should detect IAM authentication."""
        creds = {
            "type": "redshift",
            "method": "iam",
            "cluster_id": "my-cluster",
            "iam_profile": "my-profile",
            "host": "cluster.region.redshift.amazonaws.com",
        }
        assert self.handler.detect_auth_method(creds) == "iam"


# =============================================================================
# SQL Server Auth Handler Tests
# =============================================================================


class TestSQLServerAuthHandler:
    """Tests for SQLServerAuthHandler."""

    def setup_method(self):
        """Set up test fixtures."""
        self.handler = SQLServerAuthHandler()

    def test_detect_password_auth(self):
        """Should detect password authentication."""
        creds = {
            "type": "sqlserver",
            "user": "test_user",
            "password": "test_pass",
            "host": "localhost",
        }
        assert self.handler.detect_auth_method(creds) == "password"

    def test_detect_azure_ad_auth(self):
        """Should detect Azure AD authentication."""
        creds = {
            "type": "sqlserver",
            "authentication": "ActiveDirectoryPassword",
            "user": "user@domain.com",
            "password": "test_pass",
            "host": "server.database.windows.net",
        }
        assert self.handler.detect_auth_method(creds) == "azure_ad"

    def test_detect_windows_auth(self):
        """Should detect Windows (integrated) authentication."""
        creds = {
            "type": "sqlserver",
            "authentication": "windows",
            "host": "localhost",
        }
        assert self.handler.detect_auth_method(creds) == "windows"

    def test_windows_auth_is_interactive(self):
        """Windows auth should be detected as interactive."""
        creds = {
            "type": "sqlserver",
            "windows_login": True,
            "host": "localhost",
        }
        # Windows auth uses SSPI which requires system-level auth
        # Depending on implementation, it may or may not be interactive


# =============================================================================
# Trino Auth Handler Tests
# =============================================================================


class TestTrinoAuthHandler:
    """Tests for TrinoAuthHandler."""

    def setup_method(self):
        """Set up test fixtures."""
        self.handler = TrinoAuthHandler()

    def test_detect_none_auth(self):
        """Should detect no authentication."""
        creds = {
            "type": "trino",
            "host": "localhost",
            "user": "test_user",
        }
        assert self.handler.detect_auth_method(creds) == "none"

    def test_detect_password_auth(self):
        """Should detect password (LDAP) authentication."""
        creds = {
            "type": "trino",
            "host": "localhost",
            "user": "test_user",
            "password": "test_pass",
        }
        assert self.handler.detect_auth_method(creds) == "password"

    def test_detect_jwt_auth(self):
        """Should detect JWT authentication."""
        creds = {
            "type": "trino",
            "host": "localhost",
            "jwt_token": "jwt_token_value",
        }
        assert self.handler.detect_auth_method(creds) == "jwt"

    def test_no_interactive_auth_methods(self):
        """Trino should have no interactive auth methods."""
        assert len(self.handler.INTERACTIVE_AUTH_METHODS) == 0


# =============================================================================
# DuckDB Auth Handler Tests
# =============================================================================


class TestDuckDBAuthHandler:
    """Tests for DuckDBAuthHandler."""

    def setup_method(self):
        """Set up test fixtures."""
        self.handler = DuckDBAuthHandler()

    def test_detect_none_auth(self):
        """Should detect no authentication (local file)."""
        creds = {
            "type": "duckdb",
            "path": "/path/to/database.duckdb",
        }
        assert self.handler.detect_auth_method(creds) == "none"

    def test_no_interactive_auth_methods(self):
        """DuckDB should have no interactive auth methods."""
        assert len(self.handler.INTERACTIVE_AUTH_METHODS) == 0

    def test_always_valid(self):
        """DuckDB should always be valid (local file)."""
        creds = {"type": "duckdb", "path": "/path/to/database.duckdb"}
        is_valid, error = self.handler.validate(creds)
        assert is_valid is True
        assert error is None


# =============================================================================
# Athena Auth Handler Tests
# =============================================================================


class TestAthenaAuthHandler:
    """Tests for AthenaAuthHandler."""

    def setup_method(self):
        """Set up test fixtures."""
        self.handler = AthenaAuthHandler()

    def test_detect_aws_profile_auth(self):
        """Should detect AWS profile authentication."""
        creds = {
            "type": "athena",
            "aws_profile_name": "my-profile",
            "region_name": "us-east-1",
        }
        assert self.handler.detect_auth_method(creds) == "aws_profile"

    def test_detect_access_key_auth(self):
        """Should detect access key authentication."""
        creds = {
            "type": "athena",
            "aws_access_key_id": "AKIAIOSFODNN7EXAMPLE",
            "aws_secret_access_key": "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
            "region_name": "us-east-1",
        }
        assert self.handler.detect_auth_method(creds) == "access_key"

    def test_no_interactive_auth_methods(self):
        """Athena should have no interactive auth methods."""
        assert len(self.handler.INTERACTIVE_AUTH_METHODS) == 0


# =============================================================================
# Base Auth Handler Tests
# =============================================================================


class TestBaseAuthHandler:
    """Tests for BaseAuthHandler temp file management."""

    def test_create_temp_credentials_file(self):
        """Should create temp file with content and register for cleanup."""
        # Clear any existing temp files
        BaseAuthHandler._temp_files.clear()

        content = '{"key": "value"}'
        filepath = BaseAuthHandler.create_temp_credentials_file(content)

        try:
            # File should exist
            assert os.path.exists(filepath)

            # Content should match
            with open(filepath, "r") as f:
                assert f.read() == content

            # File should be registered for cleanup
            assert filepath in BaseAuthHandler._temp_files
        finally:
            # Cleanup
            if os.path.exists(filepath):
                os.remove(filepath)
            BaseAuthHandler._temp_files.clear()

    def test_cleanup_temp_files(self):
        """Should remove all registered temp files on cleanup."""
        # Create some temp files
        files = []
        for i in range(3):
            filepath = BaseAuthHandler.create_temp_credentials_file(f"content_{i}")
            files.append(filepath)

        # All files should exist
        for filepath in files:
            assert os.path.exists(filepath)

        # Run cleanup
        BaseAuthHandler.cleanup_temp_files()

        # All files should be removed
        for filepath in files:
            assert not os.path.exists(filepath)

        # Registry should be empty
        assert len(BaseAuthHandler._temp_files) == 0

    def test_get_interactive_error_message(self):
        """Should generate helpful error message with alternatives."""
        handler = SnowflakeAuthHandler()
        creds = {
            "type": "snowflake",
            "authenticator": "externalbrowser",
            "user": "test_user",
            "account": "test_account",
        }
        error_msg = handler.get_interactive_error_message(creds)

        # Should mention the auth method
        assert "externalbrowser" in error_msg

        # Should mention alternatives
        assert "password" in error_msg or "keypair" in error_msg


# =============================================================================
# Generic Auth Handler Tests
# =============================================================================


class TestGenericAuthHandler:
    """Tests for GenericAuthHandler (fallback)."""

    def setup_method(self):
        """Set up test fixtures."""
        self.handler = GenericAuthHandler()

    def test_detect_password_auth(self):
        """Should detect password authentication."""
        creds = {
            "type": "unknown_db",
            "user": "test_user",
            "password": "test_pass",
        }
        assert self.handler.detect_auth_method(creds) == "password"

    def test_get_jdbc_properties_basic(self):
        """Should build basic JDBC properties."""
        creds = {
            "type": "unknown_db",
            "user": "test_user",
            "password": "test_pass",
        }
        props = self.handler.get_jdbc_properties(creds)

        assert props["user"] == "test_user"
        assert props["password"] == "test_pass"

    def test_no_interactive_auth_methods(self):
        """GenericAuthHandler should have no interactive methods."""
        assert len(self.handler.INTERACTIVE_AUTH_METHODS) == 0


# =============================================================================
# Handler Auth Method Coverage Tests
# =============================================================================


class TestAllHandlersHaveRequiredMethods:
    """Ensure all handlers implement required abstract methods."""

    @pytest.mark.parametrize("adapter_type", get_supported_adapters())
    def test_handler_has_detect_auth_method(self, adapter_type: str):
        """All handlers should implement detect_auth_method."""
        handler = get_auth_handler(adapter_type)
        creds = {"type": adapter_type, "user": "test", "password": "test"}
        # Should not raise
        result = handler.detect_auth_method(creds)
        assert isinstance(result, str)

    @pytest.mark.parametrize("adapter_type", get_supported_adapters())
    def test_handler_has_get_jdbc_properties(self, adapter_type: str):
        """All handlers should implement get_jdbc_properties."""
        handler = get_auth_handler(adapter_type)
        creds = {"type": adapter_type, "user": "test", "password": "test"}
        # Should not raise
        result = handler.get_jdbc_properties(creds)
        assert isinstance(result, dict)

    @pytest.mark.parametrize("adapter_type", get_supported_adapters())
    def test_handler_has_get_native_connection_kwargs(self, adapter_type: str):
        """All handlers should implement get_native_connection_kwargs."""
        handler = get_auth_handler(adapter_type)
        creds = {"type": adapter_type, "user": "test", "password": "test"}
        # Should not raise
        result = handler.get_native_connection_kwargs(creds)
        assert isinstance(result, dict)

    @pytest.mark.parametrize("adapter_type", get_supported_adapters())
    def test_handler_has_validate(self, adapter_type: str):
        """All handlers should implement validate (from base class)."""
        handler = get_auth_handler(adapter_type)
        creds = {"type": adapter_type, "user": "test", "password": "test"}
        # Should not raise
        is_valid, error = handler.validate(creds)
        assert isinstance(is_valid, bool)
        assert error is None or isinstance(error, str)
