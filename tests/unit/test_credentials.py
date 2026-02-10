# coding=utf-8
"""Unit tests for DVT federation credential extraction.

Tests the dynamic credential extraction utilities that enable DVT federation
to support all authentication methods without hardcoding field names.
"""

from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

import pytest

from dvt.federation.credentials import (
    EXCLUDE_FIELDS,
    extract_all_credentials,
    get_credential_field,
)


# =============================================================================
# Test Fixtures - Mock Credential Classes
# =============================================================================


@dataclass
class MockSnowflakeCredentials:
    """Mock Snowflake credentials dataclass for testing."""

    type: str = "snowflake"
    user: str = ""
    password: Optional[str] = None
    account: str = ""
    database: Optional[str] = None
    schema_field: Optional[str] = None  # 'schema' is reserved
    warehouse: Optional[str] = None
    role: Optional[str] = None
    private_key_path: Optional[str] = None
    private_key: Optional[str] = None
    private_key_passphrase: Optional[str] = None
    authenticator: Optional[str] = None
    token: Optional[str] = None
    threads: int = 4


@dataclass
class MockPostgresCredentials:
    """Mock PostgreSQL credentials dataclass for testing."""

    type: str = "postgres"
    user: str = ""
    password: Optional[str] = None
    host: str = "localhost"
    port: int = 5432
    database: Optional[str] = None
    dbname: Optional[str] = None  # Alternate name for database
    sslmode: Optional[str] = None
    sslcert: Optional[str] = None
    sslkey: Optional[str] = None
    sslrootcert: Optional[str] = None
    threads: int = 4


@dataclass
class MockBigQueryCredentials:
    """Mock BigQuery credentials dataclass for testing."""

    type: str = "bigquery"
    method: Optional[str] = None
    project: str = ""
    dataset: Optional[str] = None
    keyfile: Optional[str] = None
    keyfile_json: Optional[Dict[str, Any]] = None
    impersonate_service_account: Optional[str] = None
    threads: int = 4


@dataclass
class MockRedshiftCredentials:
    """Mock Redshift credentials dataclass for testing."""

    type: str = "redshift"
    method: Optional[str] = None  # "database" or "iam"
    user: str = ""
    password: Optional[str] = None
    host: str = ""
    port: int = 5439
    database: Optional[str] = None
    cluster_id: Optional[str] = None
    iam_profile: Optional[str] = None
    region: Optional[str] = None
    threads: int = 4


@dataclass
class MockDatabricksCredentials:
    """Mock Databricks credentials dataclass for testing."""

    type: str = "databricks"
    host: str = ""
    http_path: Optional[str] = None
    token: Optional[str] = None
    client_id: Optional[str] = None
    client_secret: Optional[str] = None
    auth_type: Optional[str] = None
    catalog: Optional[str] = None
    schema_field: Optional[str] = None
    threads: int = 4


class LegacyCredentials:
    """Non-dataclass credentials object for testing fallback extraction."""

    def __init__(self):
        self.type = "legacy"
        self.user = "legacy_user"
        self.password = "legacy_pass"
        self.host = "legacy.host.com"
        self.port = 1234
        self._internal = "should_be_excluded"
        self.threads = 4

    def some_method(self):
        """Methods should be excluded."""
        return "method_result"


# =============================================================================
# Extract All Credentials Tests
# =============================================================================


class TestExtractAllCredentials:
    """Tests for extract_all_credentials function."""

    def test_extract_basic_password_creds(self):
        """Should extract basic username/password credentials."""
        creds = MockPostgresCredentials(
            user="test_user",
            password="test_pass",
            host="db.example.com",
            port=5432,
            database="mydb",
        )
        result = extract_all_credentials(creds)

        assert result["type"] == "postgres"
        assert result["user"] == "test_user"
        assert result["password"] == "test_pass"
        assert result["host"] == "db.example.com"
        assert result["port"] == 5432
        assert result["database"] == "mydb"

    def test_excludes_threads_field(self):
        """Should exclude 'threads' field from extraction."""
        creds = MockPostgresCredentials(
            user="test_user",
            password="test_pass",
            threads=8,
        )
        result = extract_all_credentials(creds)

        assert "threads" not in result

    def test_excludes_internal_fields(self):
        """Should exclude fields starting with underscore."""
        creds = LegacyCredentials()
        result = extract_all_credentials(creds)

        assert "_internal" not in result

    def test_excludes_none_values(self):
        """Should exclude None values from result."""
        creds = MockSnowflakeCredentials(
            user="test_user",
            password="test_pass",
            account="test_account",
            # Leave optional fields as None
        )
        result = extract_all_credentials(creds)

        assert "warehouse" not in result
        assert "role" not in result
        assert "private_key_path" not in result

    def test_includes_non_none_optional_values(self):
        """Should include non-None optional values."""
        creds = MockSnowflakeCredentials(
            user="test_user",
            password="test_pass",
            account="test_account",
            warehouse="my_warehouse",
            role="my_role",
        )
        result = extract_all_credentials(creds)

        assert result["warehouse"] == "my_warehouse"
        assert result["role"] == "my_role"

    def test_extract_keypair_auth_fields(self):
        """Should extract keypair authentication fields."""
        creds = MockSnowflakeCredentials(
            user="test_user",
            account="test_account",
            private_key_path="/path/to/key.p8",
            private_key_passphrase="key_password",
        )
        result = extract_all_credentials(creds)

        assert result["private_key_path"] == "/path/to/key.p8"
        assert result["private_key_passphrase"] == "key_password"

    def test_extract_oauth_auth_fields(self):
        """Should extract OAuth authentication fields."""
        creds = MockSnowflakeCredentials(
            user="test_user",
            account="test_account",
            authenticator="oauth",
            token="oauth_token_value",
        )
        result = extract_all_credentials(creds)

        assert result["authenticator"] == "oauth"
        assert result["token"] == "oauth_token_value"

    def test_extract_bigquery_service_account(self):
        """Should extract BigQuery service account credentials."""
        keyfile_json = {
            "type": "service_account",
            "project_id": "my-project",
            "client_email": "sa@my-project.iam.gserviceaccount.com",
        }
        creds = MockBigQueryCredentials(
            project="my-project",
            dataset="my_dataset",
            method="service-account-json",
            keyfile_json=keyfile_json,
        )
        result = extract_all_credentials(creds)

        assert result["type"] == "bigquery"
        assert result["project"] == "my-project"
        assert result["dataset"] == "my_dataset"
        assert result["method"] == "service-account-json"
        assert result["keyfile_json"] == keyfile_json

    def test_extract_redshift_iam_fields(self):
        """Should extract Redshift IAM authentication fields."""
        creds = MockRedshiftCredentials(
            method="iam",
            host="my-cluster.region.redshift.amazonaws.com",
            database="mydb",
            cluster_id="my-cluster",
            iam_profile="my-profile",
            region="us-east-1",
        )
        result = extract_all_credentials(creds)

        assert result["method"] == "iam"
        assert result["cluster_id"] == "my-cluster"
        assert result["iam_profile"] == "my-profile"
        assert result["region"] == "us-east-1"

    def test_extract_databricks_token_auth(self):
        """Should extract Databricks token authentication fields."""
        creds = MockDatabricksCredentials(
            host="https://my-workspace.cloud.databricks.com",
            http_path="/sql/1.0/warehouses/abc123",
            token="dapi_token_value",
            catalog="main",
        )
        result = extract_all_credentials(creds)

        assert result["host"] == "https://my-workspace.cloud.databricks.com"
        assert result["http_path"] == "/sql/1.0/warehouses/abc123"
        assert result["token"] == "dapi_token_value"
        assert result["catalog"] == "main"

    def test_extract_databricks_oauth_m2m(self):
        """Should extract Databricks OAuth M2M fields."""
        creds = MockDatabricksCredentials(
            host="https://my-workspace.cloud.databricks.com",
            client_id="my_client_id",
            client_secret="my_client_secret",
        )
        result = extract_all_credentials(creds)

        assert result["client_id"] == "my_client_id"
        assert result["client_secret"] == "my_client_secret"

    def test_extract_ssl_cert_fields(self):
        """Should extract SSL certificate fields."""
        creds = MockPostgresCredentials(
            user="test_user",
            password="test_pass",
            host="db.example.com",
            sslmode="verify-full",
            sslcert="/path/to/cert.pem",
            sslkey="/path/to/key.pem",
            sslrootcert="/path/to/ca.pem",
        )
        result = extract_all_credentials(creds)

        assert result["sslmode"] == "verify-full"
        assert result["sslcert"] == "/path/to/cert.pem"
        assert result["sslkey"] == "/path/to/key.pem"
        assert result["sslrootcert"] == "/path/to/ca.pem"

    def test_extract_from_legacy_non_dataclass(self):
        """Should extract credentials from non-dataclass objects."""
        creds = LegacyCredentials()
        result = extract_all_credentials(creds)

        assert result["type"] == "legacy"
        assert result["user"] == "legacy_user"
        assert result["password"] == "legacy_pass"
        assert result["host"] == "legacy.host.com"
        assert result["port"] == 1234

    def test_legacy_excludes_methods(self):
        """Should exclude callable methods from non-dataclass objects."""
        creds = LegacyCredentials()
        result = extract_all_credentials(creds)

        assert "some_method" not in result

    def test_always_includes_type(self):
        """Should always include 'type' field even if empty."""
        creds = MockPostgresCredentials(user="test")
        result = extract_all_credentials(creds)

        assert "type" in result


# =============================================================================
# Get Credential Field Tests
# =============================================================================


class TestGetCredentialField:
    """Tests for get_credential_field function."""

    def test_get_single_field(self):
        """Should return value for single field name."""
        creds = {"user": "test_user", "password": "test_pass"}
        result = get_credential_field(creds, "user")

        assert result == "test_user"

    def test_get_first_available_field(self):
        """Should return first available field from multiple options."""
        creds = {"database": "mydb", "dbname": "other_db"}
        result = get_credential_field(creds, "database", "dbname")

        assert result == "mydb"

    def test_fallback_to_second_field(self):
        """Should fallback to second field if first is None."""
        creds = {"dbname": "mydb"}
        result = get_credential_field(creds, "database", "dbname")

        assert result == "mydb"

    def test_fallback_to_third_field(self):
        """Should fallback to third field if first two are None."""
        creds = {"catalog": "my_catalog"}
        result = get_credential_field(creds, "database", "dbname", "catalog")

        assert result == "my_catalog"

    def test_returns_default_if_none_found(self):
        """Should return default if no matching field found."""
        creds = {"user": "test_user"}
        result = get_credential_field(creds, "database", "dbname", default="default_db")

        assert result == "default_db"

    def test_default_is_none_if_not_specified(self):
        """Default should be None if not explicitly specified."""
        creds = {"user": "test_user"}
        result = get_credential_field(creds, "database")

        assert result is None

    def test_empty_string_is_not_none(self):
        """Empty string should be returned, not treated as None."""
        creds = {"database": "", "dbname": "fallback"}
        result = get_credential_field(creds, "database", "dbname")

        assert result == ""  # Empty string is valid, not None

    def test_zero_is_not_none(self):
        """Zero should be returned, not treated as None."""
        creds = {"port": 0, "default_port": 5432}
        result = get_credential_field(creds, "port", "default_port")

        assert result == 0  # Zero is valid, not None

    def test_false_is_not_none(self):
        """False should be returned, not treated as None."""
        creds = {"ssl": False, "use_ssl": True}
        result = get_credential_field(creds, "ssl", "use_ssl")

        assert result is False  # False is valid, not None


# =============================================================================
# Exclude Fields Tests
# =============================================================================


class TestExcludeFields:
    """Tests for EXCLUDE_FIELDS constant."""

    def test_type_is_excluded_from_general_extraction(self):
        """'type' should be in EXCLUDE_FIELDS (handled specially)."""
        assert "type" in EXCLUDE_FIELDS

    def test_threads_is_excluded(self):
        """'threads' should be excluded (not relevant for auth)."""
        assert "threads" in EXCLUDE_FIELDS

    def test_internal_fields_are_excluded(self):
        """Internal dbt fields should be excluded."""
        assert "_ALIASES" in EXCLUDE_FIELDS
        assert "_events" in EXCLUDE_FIELDS
        assert "__dataclass_fields__" in EXCLUDE_FIELDS


# =============================================================================
# Edge Cases
# =============================================================================


class TestEdgeCases:
    """Tests for edge cases and error handling."""

    def test_empty_dataclass(self):
        """Should handle dataclass with only default values."""

        @dataclass
        class EmptyCreds:
            type: str = "empty"

        creds = EmptyCreds()
        result = extract_all_credentials(creds)

        assert result["type"] == "empty"

    def test_nested_dict_preserved(self):
        """Should preserve nested dictionaries (like keyfile_json)."""
        creds = MockBigQueryCredentials(
            project="my-project",
            keyfile_json={
                "type": "service_account",
                "private_key": "-----BEGIN PRIVATE KEY-----...",
                "nested": {"level2": {"level3": "value"}},
            },
        )
        result = extract_all_credentials(creds)

        assert result["keyfile_json"]["type"] == "service_account"
        assert result["keyfile_json"]["nested"]["level2"]["level3"] == "value"

    def test_list_field_preserved(self):
        """Should preserve list fields."""

        @dataclass
        class CredsWithList:
            type: str = "test"
            scopes: Optional[List[str]] = None

        creds = CredsWithList(scopes=["scope1", "scope2", "scope3"])
        result = extract_all_credentials(creds)

        assert result["scopes"] == ["scope1", "scope2", "scope3"]

    def test_special_characters_in_values(self):
        """Should handle special characters in credential values."""
        creds = MockPostgresCredentials(
            user="test@user.com",
            password="p@ss!w0rd#$%^&*()",
            database="db-name_with.special",
        )
        result = extract_all_credentials(creds)

        assert result["user"] == "test@user.com"
        assert result["password"] == "p@ss!w0rd#$%^&*()"
        assert result["database"] == "db-name_with.special"

    def test_unicode_in_values(self):
        """Should handle unicode characters in credential values."""
        creds = MockPostgresCredentials(
            user="用户名",
            password="密码",
            database="数据库",
        )
        result = extract_all_credentials(creds)

        assert result["user"] == "用户名"
        assert result["password"] == "密码"
        assert result["database"] == "数据库"

    def test_very_long_values(self):
        """Should handle very long credential values."""
        long_token = "x" * 10000
        creds = MockSnowflakeCredentials(
            user="test_user",
            account="test_account",
            token=long_token,
        )
        result = extract_all_credentials(creds)

        assert result["token"] == long_token
        assert len(result["token"]) == 10000
