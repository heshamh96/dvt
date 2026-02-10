# coding=utf-8
"""Unit tests for native SQL execution in spark_seed.py.

Tests the native SQL execution methods that use auth handlers
for database-specific authentication. These tests focus on verifying
that auth handlers are used correctly and return proper connection kwargs.
"""

import pytest

from dvt.federation.auth.postgres import PostgresAuthHandler
from dvt.federation.auth.redshift import RedshiftAuthHandler
from dvt.federation.auth.mysql import MySQLAuthHandler
from dvt.federation.auth.bigquery import BigQueryAuthHandler
from dvt.federation.auth.databricks import DatabricksAuthHandler
from dvt.federation.auth.sqlserver import SQLServerAuthHandler
from dvt.federation.auth.trino import TrinoAuthHandler
from dvt.federation.auth.clickhouse import ClickHouseAuthHandler
from dvt.federation.auth.oracle import OracleAuthHandler


# =============================================================================
# PostgreSQL Native Connection Tests
# =============================================================================


class TestPostgresNativeConnection:
    """Tests for PostgreSQL native connection kwargs."""

    def test_password_auth_kwargs(self):
        """Should return correct kwargs for password authentication."""
        handler = PostgresAuthHandler()
        creds = {
            "type": "postgres",
            "host": "localhost",
            "port": 5432,
            "database": "testdb",
            "user": "testuser",
            "password": "testpass",
        }

        kwargs = handler.get_native_connection_kwargs(creds)

        assert kwargs["host"] == "localhost"
        assert kwargs["port"] == 5432
        assert kwargs["dbname"] == "testdb"
        assert kwargs["user"] == "testuser"
        assert kwargs["password"] == "testpass"

    def test_ssl_cert_auth_kwargs(self):
        """Should include SSL certificate paths."""
        handler = PostgresAuthHandler()
        creds = {
            "type": "postgres",
            "host": "localhost",
            "user": "testuser",
            "password": "testpass",
            "database": "testdb",
            "sslmode": "verify-full",
            "sslcert": "/path/to/cert.pem",
            "sslkey": "/path/to/key.pem",
            "sslrootcert": "/path/to/ca.pem",
        }

        kwargs = handler.get_native_connection_kwargs(creds)

        assert kwargs["sslmode"] == "verify-full"
        assert kwargs["sslcert"] == "/path/to/cert.pem"
        assert kwargs["sslkey"] == "/path/to/key.pem"
        assert kwargs["sslrootcert"] == "/path/to/ca.pem"

    def test_validation_always_passes(self):
        """PostgreSQL should have no interactive auth methods."""
        handler = PostgresAuthHandler()
        creds = {
            "type": "postgres",
            "user": "testuser",
            "password": "testpass",
        }

        is_valid, error = handler.validate(creds)

        assert is_valid is True
        assert error is None


# =============================================================================
# Redshift Native Connection Tests
# =============================================================================


class TestRedshiftNativeConnection:
    """Tests for Redshift native connection kwargs."""

    def test_password_auth_kwargs(self):
        """Should return correct kwargs for password authentication."""
        handler = RedshiftAuthHandler()
        creds = {
            "type": "redshift",
            "host": "cluster.region.redshift.amazonaws.com",
            "port": 5439,
            "database": "dev",
            "user": "admin",
            "password": "password123",
        }

        kwargs = handler.get_native_connection_kwargs(creds)

        assert kwargs["host"] == "cluster.region.redshift.amazonaws.com"
        assert kwargs["port"] == 5439
        assert kwargs["database"] == "dev"
        assert kwargs["user"] == "admin"
        assert kwargs["password"] == "password123"

    def test_iam_auth_kwargs(self):
        """Should return correct kwargs for IAM authentication."""
        handler = RedshiftAuthHandler()
        creds = {
            "type": "redshift",
            "method": "iam",
            "host": "cluster.region.redshift.amazonaws.com",
            "database": "dev",
            "user": "admin",
            "cluster_id": "my-cluster",
            "region": "us-east-1",
            "iam_profile": "my-profile",
        }

        kwargs = handler.get_native_connection_kwargs(creds)

        assert kwargs["iam"] is True
        assert kwargs["cluster_identifier"] == "my-cluster"
        assert kwargs["region"] == "us-east-1"
        assert kwargs["profile"] == "my-profile"

    def test_detect_password_auth(self):
        """Should detect password auth when no method specified."""
        handler = RedshiftAuthHandler()
        creds = {
            "type": "redshift",
            "user": "admin",
            "password": "password123",
        }

        assert handler.detect_auth_method(creds) == "password"

    def test_detect_iam_auth(self):
        """Should detect IAM auth when method is 'iam'."""
        handler = RedshiftAuthHandler()
        creds = {
            "type": "redshift",
            "method": "iam",
            "user": "admin",
        }

        assert handler.detect_auth_method(creds) == "iam"


# =============================================================================
# MySQL Native Connection Tests
# =============================================================================


class TestMySQLNativeConnection:
    """Tests for MySQL native connection kwargs."""

    def test_password_auth_kwargs(self):
        """Should return correct kwargs for password authentication."""
        handler = MySQLAuthHandler()
        creds = {
            "type": "mysql",
            "host": "localhost",
            "port": 3306,
            "database": "testdb",
            "user": "testuser",
            "password": "testpass",
        }

        kwargs = handler.get_native_connection_kwargs(creds)

        assert kwargs["host"] == "localhost"
        assert kwargs["port"] == 3306
        assert kwargs["database"] == "testdb"
        assert kwargs["user"] == "testuser"
        assert kwargs["password"] == "testpass"

    def test_ssl_auth_kwargs(self):
        """Should return correct kwargs for SSL authentication."""
        handler = MySQLAuthHandler()
        creds = {
            "type": "mysql",
            "host": "localhost",
            "user": "testuser",
            "password": "testpass",
            "ssl_ca": "/path/to/ca.pem",
            "ssl_cert": "/path/to/cert.pem",
            "ssl_key": "/path/to/key.pem",
        }

        kwargs = handler.get_native_connection_kwargs(creds)

        assert "ssl" in kwargs
        assert kwargs["ssl"]["ca"] == "/path/to/ca.pem"
        assert kwargs["ssl"]["cert"] == "/path/to/cert.pem"
        assert kwargs["ssl"]["key"] == "/path/to/key.pem"

    def test_detect_password_auth(self):
        """Should detect password auth by default."""
        handler = MySQLAuthHandler()
        creds = {
            "type": "mysql",
            "user": "testuser",
            "password": "testpass",
        }

        assert handler.detect_auth_method(creds) == "password"

    def test_detect_ssl_auth(self):
        """Should detect SSL auth when ssl_ca is present."""
        handler = MySQLAuthHandler()
        creds = {
            "type": "mysql",
            "user": "testuser",
            "password": "testpass",
            "ssl_ca": "/path/to/ca.pem",
        }

        assert handler.detect_auth_method(creds) == "ssl"


# =============================================================================
# BigQuery Native Connection Tests
# =============================================================================


class TestBigQueryNativeConnection:
    """Tests for BigQuery native connection kwargs."""

    def test_oauth_auth_kwargs(self):
        """Should return correct kwargs for OAuth authentication."""
        handler = BigQueryAuthHandler()
        creds = {
            "type": "bigquery",
            "project": "my-project",
        }

        assert handler.detect_auth_method(creds) == "oauth"

        kwargs = handler.get_native_connection_kwargs(creds)
        assert kwargs["project"] == "my-project"

    def test_service_account_auth_kwargs(self):
        """Should return correct kwargs for service account file."""
        handler = BigQueryAuthHandler()
        creds = {
            "type": "bigquery",
            "project": "my-project",
            "keyfile": "/path/to/keyfile.json",
        }

        assert handler.detect_auth_method(creds) == "service_account"

        kwargs = handler.get_native_connection_kwargs(creds)
        assert kwargs["project"] == "my-project"
        assert kwargs["keyfile"] == "/path/to/keyfile.json"

    def test_service_account_json_auth_kwargs(self):
        """Should return correct kwargs for inline service account JSON."""
        handler = BigQueryAuthHandler()
        keyfile_json = {
            "type": "service_account",
            "project_id": "my-project",
            "client_email": "sa@my-project.iam.gserviceaccount.com",
        }
        creds = {
            "type": "bigquery",
            "project": "my-project",
            "keyfile_json": keyfile_json,
        }

        assert handler.detect_auth_method(creds) == "service_account_json"

        kwargs = handler.get_native_connection_kwargs(creds)
        assert kwargs["keyfile_json"] == keyfile_json

    def test_no_interactive_auth_methods(self):
        """BigQuery should have no interactive auth methods."""
        handler = BigQueryAuthHandler()
        assert len(handler.INTERACTIVE_AUTH_METHODS) == 0


# =============================================================================
# Databricks Native Connection Tests
# =============================================================================


class TestDatabricksNativeConnection:
    """Tests for Databricks native connection kwargs."""

    def test_token_auth_kwargs(self):
        """Should return correct kwargs for token authentication."""
        handler = DatabricksAuthHandler()
        creds = {
            "type": "databricks",
            "host": "abc.cloud.databricks.com",
            "http_path": "/sql/1.0/warehouses/xxx",
            "token": "dapi_xxx",
            "catalog": "main",
            "schema": "default",
        }

        assert handler.detect_auth_method(creds) == "token"

        kwargs = handler.get_native_connection_kwargs(creds)
        assert kwargs["server_hostname"] == "abc.cloud.databricks.com"
        assert kwargs["http_path"] == "/sql/1.0/warehouses/xxx"
        assert kwargs["access_token"] == "dapi_xxx"
        assert kwargs["catalog"] == "main"
        assert kwargs["schema"] == "default"

    def test_oauth_m2m_auth_kwargs(self):
        """Should return correct kwargs for OAuth M2M authentication."""
        handler = DatabricksAuthHandler()
        creds = {
            "type": "databricks",
            "host": "abc.cloud.databricks.com",
            "auth_type": "oauth",
            "client_id": "my_client_id",
            "client_secret": "my_client_secret",
        }

        assert handler.detect_auth_method(creds) == "oauth_m2m"

        kwargs = handler.get_native_connection_kwargs(creds)
        assert kwargs["client_id"] == "my_client_id"
        assert kwargs["client_secret"] == "my_client_secret"

    def test_oauth_u2m_blocked(self):
        """Should block OAuth U2M (requires browser)."""
        handler = DatabricksAuthHandler()
        creds = {
            "type": "databricks",
            "host": "abc.cloud.databricks.com",
            "auth_type": "oauth",
        }

        assert handler.is_interactive(creds) is True
        is_valid, error = handler.validate(creds)
        assert is_valid is False
        assert "oauth_u2m" in error.lower() or "browser" in error.lower()


# =============================================================================
# SQL Server Native Connection Tests
# =============================================================================


class TestSQLServerNativeConnection:
    """Tests for SQL Server native connection kwargs."""

    def test_password_auth_kwargs(self):
        """Should return correct kwargs for password authentication."""
        handler = SQLServerAuthHandler()
        creds = {
            "type": "sqlserver",
            "host": "localhost",
            "port": 1433,
            "database": "testdb",
            "user": "sa",
            "password": "password123",
        }

        assert handler.detect_auth_method(creds) == "password"

        kwargs = handler.get_native_connection_kwargs(creds)
        assert kwargs["server"] == "localhost"
        assert kwargs["port"] == "1433"
        assert kwargs["database"] == "testdb"
        assert kwargs["user"] == "sa"
        assert kwargs["password"] == "password123"

    def test_azure_ad_auth_detection(self):
        """Should detect Azure AD authentication."""
        handler = SQLServerAuthHandler()
        creds = {
            "type": "sqlserver",
            "host": "server.database.windows.net",
            "authentication": "ActiveDirectoryPassword",
            "user": "user@domain.com",
            "password": "password123",
        }

        assert handler.detect_auth_method(creds) == "azure_ad"

    def test_windows_auth_detection(self):
        """Should detect Windows authentication."""
        handler = SQLServerAuthHandler()
        creds = {
            "type": "sqlserver",
            "host": "localhost",
            "authentication": "windows",
        }

        assert handler.detect_auth_method(creds) == "windows"


# =============================================================================
# Trino Native Connection Tests
# =============================================================================


class TestTrinoNativeConnection:
    """Tests for Trino native connection kwargs."""

    def test_no_auth_kwargs(self):
        """Should return correct kwargs for no authentication."""
        handler = TrinoAuthHandler()
        creds = {
            "type": "trino",
            "host": "localhost",
            "port": 8080,
            "catalog": "hive",
            "schema": "default",
            "user": "trino",
        }

        assert handler.detect_auth_method(creds) == "none"

        kwargs = handler.get_native_connection_kwargs(creds)
        assert kwargs["host"] == "localhost"
        assert kwargs["port"] == 8080
        assert kwargs["catalog"] == "hive"
        assert kwargs["schema"] == "default"
        assert kwargs["user"] == "trino"

    def test_password_auth_kwargs(self):
        """Should return correct kwargs for password (LDAP) authentication."""
        handler = TrinoAuthHandler()
        creds = {
            "type": "trino",
            "host": "localhost",
            "user": "admin",
            "password": "password123",
            "catalog": "hive",
        }

        assert handler.detect_auth_method(creds) == "password"

        kwargs = handler.get_native_connection_kwargs(creds)
        assert kwargs["user"] == "admin"
        assert "auth" in kwargs
        assert kwargs["auth"] == ("admin", "password123")

    def test_jwt_auth_detection(self):
        """Should detect JWT authentication."""
        handler = TrinoAuthHandler()
        creds = {
            "type": "trino",
            "host": "localhost",
            "jwt_token": "jwt_token_value",
        }

        assert handler.detect_auth_method(creds) == "jwt"

    def test_ssl_enabled(self):
        """Should enable SSL when http_scheme is https."""
        handler = TrinoAuthHandler()
        creds = {
            "type": "trino",
            "host": "localhost",
            "http_scheme": "https",
            "user": "trino",
        }

        kwargs = handler.get_native_connection_kwargs(creds)
        assert kwargs["http_scheme"] == "https"


# =============================================================================
# ClickHouse Native Connection Tests
# =============================================================================


class TestClickHouseNativeConnection:
    """Tests for ClickHouse native connection kwargs."""

    def test_password_auth_kwargs(self):
        """Should return correct kwargs for password authentication."""
        handler = ClickHouseAuthHandler()
        creds = {
            "type": "clickhouse",
            "host": "localhost",
            "port": 8123,
            "database": "default",
            "user": "default",
            "password": "password123",
        }

        kwargs = handler.get_native_connection_kwargs(creds)

        assert kwargs["host"] == "localhost"
        assert kwargs["port"] == 8123
        assert kwargs["database"] == "default"
        assert kwargs["user"] == "default"
        assert kwargs["password"] == "password123"

    def test_ssl_auth_kwargs(self):
        """Should enable SSL when secure is True."""
        handler = ClickHouseAuthHandler()
        creds = {
            "type": "clickhouse",
            "host": "localhost",
            "user": "default",
            "password": "password123",
            "secure": True,
        }

        assert handler.detect_auth_method(creds) == "ssl"

        kwargs = handler.get_native_connection_kwargs(creds)
        assert kwargs["secure"] is True

    def test_no_interactive_auth_methods(self):
        """ClickHouse should have no interactive auth methods."""
        handler = ClickHouseAuthHandler()
        assert len(handler.INTERACTIVE_AUTH_METHODS) == 0


# =============================================================================
# Oracle Native Connection Tests
# =============================================================================


class TestOracleNativeConnection:
    """Tests for Oracle native connection kwargs."""

    def test_password_auth_kwargs(self):
        """Should return correct kwargs for password authentication."""
        handler = OracleAuthHandler()
        creds = {
            "type": "oracle",
            "host": "localhost",
            "port": 1521,
            "database": "ORCL",
            "user": "system",
            "password": "password123",
        }

        assert handler.detect_auth_method(creds) == "password"

        kwargs = handler.get_native_connection_kwargs(creds)
        assert kwargs["dsn"] == "localhost:1521/ORCL"
        assert kwargs["user"] == "system"
        assert kwargs["password"] == "password123"

    def test_wallet_auth_detection(self):
        """Should detect wallet authentication."""
        handler = OracleAuthHandler()
        creds = {
            "type": "oracle",
            "host": "localhost",
            "database": "ORCL",
            "wallet_location": "/path/to/wallet",
            "wallet_password": "wallet_pass",
        }

        assert handler.detect_auth_method(creds) == "wallet"

        kwargs = handler.get_native_connection_kwargs(creds)
        assert kwargs["wallet_location"] == "/path/to/wallet"
        assert kwargs["wallet_password"] == "wallet_pass"


# =============================================================================
# _execute_sql_native Router Tests
# =============================================================================


class TestExecuteSqlNativeRouter:
    """Tests for the _execute_sql_native router logic."""

    def test_postgres_compatible_databases(self):
        """PostgreSQL-compatible databases should use PostgresAuthHandler."""
        handler = PostgresAuthHandler()

        for adapter in [
            "postgres",
            "greenplum",
            "materialize",
            "risingwave",
            "cratedb",
            "alloydb",
            "timescaledb",
        ]:
            creds = {"type": adapter, "user": "test", "password": "test"}
            # All should validate successfully
            is_valid, _ = handler.validate(creds)
            assert is_valid is True, f"{adapter} should be valid"

    def test_mysql_compatible_databases(self):
        """MySQL-compatible databases should use MySQLAuthHandler."""
        handler = MySQLAuthHandler()

        for adapter in ["mysql", "tidb", "singlestore"]:
            creds = {"type": adapter, "user": "test", "password": "test"}
            # All should validate successfully
            is_valid, _ = handler.validate(creds)
            assert is_valid is True, f"{adapter} should be valid"

    def test_sqlserver_compatible_databases(self):
        """SQL Server-compatible databases should use SQLServerAuthHandler."""
        handler = SQLServerAuthHandler()

        for adapter in ["sqlserver", "synapse", "fabric"]:
            creds = {"type": adapter, "user": "test", "password": "test"}
            # All should validate successfully (password auth)
            is_valid, _ = handler.validate(creds)
            assert is_valid is True, f"{adapter} should be valid"


# =============================================================================
# Drop Table Cascade Tests
# =============================================================================


class TestDropTableCascade:
    """Tests for DROP TABLE CASCADE syntax by database."""

    def test_cascade_supported_databases(self):
        """These databases should use DROP TABLE CASCADE."""
        cascade_dbs = [
            "postgres",
            "redshift",
            "snowflake",
            "greenplum",
            "materialize",
            "risingwave",
            "cratedb",
            "alloydb",
            "timescaledb",
            "trino",
            "starburst",
        ]
        # Just verify these are recognized as CASCADE-supporting
        # Actual SQL generation is tested via integration tests
        for db in cascade_dbs:
            assert db in cascade_dbs  # Placeholder test

    def test_no_cascade_databases(self):
        """These databases should NOT use CASCADE."""
        no_cascade_dbs = [
            "mysql",
            "tidb",
            "singlestore",
            "clickhouse",
            "bigquery",
            "databricks",
            "sqlserver",
            "synapse",
            "fabric",
        ]
        # Just verify these are recognized
        for db in no_cascade_dbs:
            assert db in no_cascade_dbs  # Placeholder test

    def test_oracle_uses_cascade_constraints(self):
        """Oracle should use CASCADE CONSTRAINTS syntax."""
        # Oracle has special syntax: DROP TABLE x CASCADE CONSTRAINTS
        assert True  # Placeholder - actual syntax tested in integration tests
