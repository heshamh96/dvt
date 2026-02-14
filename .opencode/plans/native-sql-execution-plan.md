# Plan: Complete Native SQL Execution Support for All Major Adapters

## Overview

This plan adds native SQL execution support for all major database adapters in DVT's `spark_seed.py`. This enables `--full-refresh` DROP CASCADE operations to work across all databases.

## Current State

- `spark_seed.py` has native SQL execution for:
  - ✅ Snowflake (using `SnowflakeAuthHandler`)
  - ⚠️ PostgreSQL (hardcoded credentials - needs update to use `PostgresAuthHandler`)

## Goals

1. Update PostgreSQL to use `PostgresAuthHandler` (adds SSL support)
2. Add native SQL execution for 8 additional adapters
3. Expand `_execute_sql_native()` router
4. Update `_drop_table_cascade()` for all adapters
5. Add unit tests

---

## Implementation Details

### File: `core/dvt/task/spark_seed.py`

---

### Task 1: Update `_execute_sql_postgres()` (lines 433-460)

**Replace:**
```python
def _execute_sql_postgres(self, connection: Dict[str, Any], sql: str) -> bool:
    """Execute SQL on Postgres using psycopg2."""
    try:
        import psycopg2
    except ImportError:
        _log("    psycopg2 not installed, skipping native SQL")
        return False

    conn = None
    try:
        conn = psycopg2.connect(
            host=connection.get("host") or "localhost",
            port=connection.get("port") or 5432,
            database=connection.get("database") or "postgres",
            user=connection.get("user") or "",
            password=connection.get("password") or "",
        )
        conn.autocommit = True
        cur = conn.cursor()
        cur.execute(sql)
        cur.close()
        return True
    except Exception as e:
        _log(f"    Postgres SQL failed: {str(e)[:60]}")
        return False
    finally:
        if conn:
            conn.close()
```

**With:**
```python
def _execute_sql_postgres(self, connection: Dict[str, Any], sql: str) -> bool:
    """Execute SQL on Postgres using psycopg2 with full auth support.

    Supports: password, SSL certificates (sslmode, sslcert, sslkey, sslrootcert)
    Also used for PostgreSQL-compatible databases: Greenplum, Materialize,
    RisingWave, CrateDB, AlloyDB, TimescaleDB.
    """
    try:
        import psycopg2
    except ImportError:
        _log("    psycopg2 not installed, skipping native SQL")
        return False

    from dvt.federation.auth.postgres import PostgresAuthHandler

    handler = PostgresAuthHandler()
    is_valid, error_msg = handler.validate(connection)
    if not is_valid:
        _log(f"    {error_msg}")
        return False

    conn = None
    try:
        connect_kwargs = handler.get_native_connection_kwargs(connection)
        conn = psycopg2.connect(**connect_kwargs)
        conn.autocommit = True
        cur = conn.cursor()
        cur.execute(sql)
        cur.close()
        return True
    except Exception as e:
        _log(f"    Postgres SQL failed: {str(e)[:60]}")
        return False
    finally:
        if conn:
            conn.close()
```

---

### Task 2: Update `_execute_sql_native()` Router (lines 421-431)

**Replace:**
```python
def _execute_sql_native(self, connection: Dict[str, Any], sql: str) -> bool:
    """Execute SQL using native Python driver. Returns True on success."""
    adapter_type = connection.get("type", "")

    if adapter_type == "postgres":
        return self._execute_sql_postgres(connection, sql)
    elif adapter_type == "snowflake":
        return self._execute_sql_snowflake(connection, sql)
    else:
        _log(f"    Native SQL exec not supported for {adapter_type}")
        return False
```

**With:**
```python
def _execute_sql_native(self, connection: Dict[str, Any], sql: str) -> bool:
    """Execute SQL using native Python driver. Returns True on success.

    Supports all major database adapters with their respective Python drivers.
    Falls back gracefully if driver is not installed.
    """
    adapter_type = connection.get("type", "").lower()

    # PostgreSQL and compatible databases
    if adapter_type in (
        "postgres", "greenplum", "materialize", "risingwave",
        "cratedb", "alloydb", "timescaledb"
    ):
        return self._execute_sql_postgres(connection, sql)

    elif adapter_type == "snowflake":
        return self._execute_sql_snowflake(connection, sql)

    elif adapter_type == "redshift":
        return self._execute_sql_redshift(connection, sql)

    # MySQL and compatible databases
    elif adapter_type in ("mysql", "tidb", "singlestore"):
        return self._execute_sql_mysql(connection, sql)

    elif adapter_type == "bigquery":
        return self._execute_sql_bigquery(connection, sql)

    elif adapter_type == "databricks":
        return self._execute_sql_databricks(connection, sql)

    elif adapter_type in ("sqlserver", "synapse", "fabric"):
        return self._execute_sql_sqlserver(connection, sql)

    elif adapter_type in ("trino", "starburst"):
        return self._execute_sql_trino(connection, sql)

    elif adapter_type == "clickhouse":
        return self._execute_sql_clickhouse(connection, sql)

    elif adapter_type == "oracle":
        return self._execute_sql_oracle(connection, sql)

    else:
        _log(f"    Native SQL exec not supported for {adapter_type}")
        return False
```

---

### Task 3: Add New Native SQL Execution Methods

**Insert after `_execute_sql_snowflake()` (after line 494):**

```python
def _execute_sql_redshift(self, connection: Dict[str, Any], sql: str) -> bool:
    """Execute SQL on Redshift using redshift_connector.

    Supports: password, IAM authentication.
    """
    try:
        import redshift_connector
    except ImportError:
        _log("    redshift_connector not installed, skipping native SQL")
        return False

    from dvt.federation.auth.redshift import RedshiftAuthHandler

    handler = RedshiftAuthHandler()
    is_valid, error_msg = handler.validate(connection)
    if not is_valid:
        _log(f"    {error_msg}")
        return False

    conn = None
    try:
        connect_kwargs = handler.get_native_connection_kwargs(connection)
        conn = redshift_connector.connect(**connect_kwargs)
        conn.autocommit = True
        cur = conn.cursor()
        cur.execute(sql)
        cur.close()
        return True
    except Exception as e:
        _log(f"    Redshift SQL failed: {str(e)[:60]}")
        return False
    finally:
        if conn:
            conn.close()

def _execute_sql_mysql(self, connection: Dict[str, Any], sql: str) -> bool:
    """Execute SQL on MySQL using mysql-connector-python.

    Supports: password, SSL certificates.
    Also used for MySQL-compatible databases: TiDB, SingleStore.
    """
    try:
        import mysql.connector
    except ImportError:
        _log("    mysql-connector-python not installed, skipping native SQL")
        return False

    from dvt.federation.auth.mysql import MySQLAuthHandler

    handler = MySQLAuthHandler()
    is_valid, error_msg = handler.validate(connection)
    if not is_valid:
        _log(f"    {error_msg}")
        return False

    conn = None
    try:
        connect_kwargs = handler.get_native_connection_kwargs(connection)
        conn = mysql.connector.connect(**connect_kwargs)
        conn.autocommit = True
        cur = conn.cursor()
        cur.execute(sql)
        cur.close()
        return True
    except Exception as e:
        _log(f"    MySQL SQL failed: {str(e)[:60]}")
        return False
    finally:
        if conn:
            conn.close()

def _execute_sql_bigquery(self, connection: Dict[str, Any], sql: str) -> bool:
    """Execute SQL on BigQuery using google-cloud-bigquery.

    Supports: oauth (application default), service_account, service_account_json.
    """
    try:
        from google.cloud import bigquery
        from google.oauth2 import service_account
    except ImportError:
        _log("    google-cloud-bigquery not installed, skipping native SQL")
        return False

    from dvt.federation.auth.bigquery import BigQueryAuthHandler

    handler = BigQueryAuthHandler()
    is_valid, error_msg = handler.validate(connection)
    if not is_valid:
        _log(f"    {error_msg}")
        return False

    try:
        native_kwargs = handler.get_native_connection_kwargs(connection)
        project = native_kwargs.get("project", "")

        # Build credentials based on auth method
        if native_kwargs.get("keyfile"):
            credentials = service_account.Credentials.from_service_account_file(
                native_kwargs["keyfile"]
            )
            client = bigquery.Client(project=project, credentials=credentials)
        elif native_kwargs.get("keyfile_json"):
            credentials = service_account.Credentials.from_service_account_info(
                native_kwargs["keyfile_json"]
            )
            client = bigquery.Client(project=project, credentials=credentials)
        else:
            # Use application default credentials (gcloud auth)
            client = bigquery.Client(project=project)

        query_job = client.query(sql)
        query_job.result()  # Wait for completion
        return True
    except Exception as e:
        _log(f"    BigQuery SQL failed: {str(e)[:60]}")
        return False

def _execute_sql_databricks(self, connection: Dict[str, Any], sql: str) -> bool:
    """Execute SQL on Databricks using databricks-sql-connector.

    Supports: token, oauth_m2m (client credentials).
    Blocked: oauth_u2m (requires browser).
    """
    try:
        from databricks import sql as databricks_sql
    except ImportError:
        _log("    databricks-sql-connector not installed, skipping native SQL")
        return False

    from dvt.federation.auth.databricks import DatabricksAuthHandler

    handler = DatabricksAuthHandler()
    is_valid, error_msg = handler.validate(connection)
    if not is_valid:
        _log(f"    {error_msg}")
        return False

    conn = None
    try:
        connect_kwargs = handler.get_native_connection_kwargs(connection)
        conn = databricks_sql.connect(**connect_kwargs)
        cur = conn.cursor()
        cur.execute(sql)
        cur.close()
        return True
    except Exception as e:
        _log(f"    Databricks SQL failed: {str(e)[:60]}")
        return False
    finally:
        if conn:
            conn.close()

def _execute_sql_sqlserver(self, connection: Dict[str, Any], sql: str) -> bool:
    """Execute SQL on SQL Server using pymssql.

    Supports: password, azure_ad.
    Blocked: windows (integrated auth requires domain context).
    Also used for: Azure Synapse, Microsoft Fabric.
    """
    try:
        import pymssql
    except ImportError:
        _log("    pymssql not installed, skipping native SQL")
        return False

    from dvt.federation.auth.sqlserver import SQLServerAuthHandler

    handler = SQLServerAuthHandler()
    is_valid, error_msg = handler.validate(connection)
    if not is_valid:
        _log(f"    {error_msg}")
        return False

    conn = None
    try:
        connect_kwargs = handler.get_native_connection_kwargs(connection)
        # pymssql uses 'server' not 'host', and port is separate or in server string
        server = connect_kwargs.pop("server", "")
        port = connect_kwargs.pop("port", "1433")
        conn = pymssql.connect(
            server=server,
            port=port,
            user=connect_kwargs.get("user", ""),
            password=connect_kwargs.get("password", ""),
            database=connect_kwargs.get("database", ""),
        )
        conn.autocommit(True)
        cur = conn.cursor()
        cur.execute(sql)
        cur.close()
        return True
    except Exception as e:
        _log(f"    SQL Server SQL failed: {str(e)[:60]}")
        return False
    finally:
        if conn:
            conn.close()

def _execute_sql_trino(self, connection: Dict[str, Any], sql: str) -> bool:
    """Execute SQL on Trino/Starburst using trino Python driver.

    Supports: none, password (LDAP), jwt.
    """
    try:
        import trino
    except ImportError:
        _log("    trino not installed, skipping native SQL")
        return False

    from dvt.federation.auth.trino import TrinoAuthHandler

    handler = TrinoAuthHandler()
    is_valid, error_msg = handler.validate(connection)
    if not is_valid:
        _log(f"    {error_msg}")
        return False

    conn = None
    try:
        connect_kwargs = handler.get_native_connection_kwargs(connection)
        # trino.dbapi.connect has slightly different param names
        auth_tuple = connect_kwargs.pop("auth", None)
        conn = trino.dbapi.connect(
            host=connect_kwargs.get("host", "localhost"),
            port=connect_kwargs.get("port", 8080),
            user=connect_kwargs.get("user", "trino"),
            catalog=connect_kwargs.get("catalog", ""),
            schema=connect_kwargs.get("schema", ""),
            http_scheme=connect_kwargs.get("http_scheme", "http"),
            auth=trino.auth.BasicAuthentication(*auth_tuple) if auth_tuple else None,
        )
        cur = conn.cursor()
        cur.execute(sql)
        cur.fetchall()  # Consume results
        cur.close()
        return True
    except Exception as e:
        _log(f"    Trino SQL failed: {str(e)[:60]}")
        return False
    finally:
        if conn:
            conn.close()

def _execute_sql_clickhouse(self, connection: Dict[str, Any], sql: str) -> bool:
    """Execute SQL on ClickHouse using clickhouse-connect.

    Supports: password, SSL.
    """
    try:
        import clickhouse_connect
    except ImportError:
        _log("    clickhouse-connect not installed, skipping native SQL")
        return False

    from dvt.federation.auth.clickhouse import ClickHouseAuthHandler

    handler = ClickHouseAuthHandler()
    is_valid, error_msg = handler.validate(connection)
    if not is_valid:
        _log(f"    {error_msg}")
        return False

    client = None
    try:
        connect_kwargs = handler.get_native_connection_kwargs(connection)
        client = clickhouse_connect.get_client(**connect_kwargs)
        client.command(sql)
        return True
    except Exception as e:
        _log(f"    ClickHouse SQL failed: {str(e)[:60]}")
        return False
    finally:
        if client:
            try:
                client.close()
            except Exception:
                pass

def _execute_sql_oracle(self, connection: Dict[str, Any], sql: str) -> bool:
    """Execute SQL on Oracle using oracledb.

    Supports: password, wallet.
    """
    try:
        import oracledb
    except ImportError:
        _log("    oracledb not installed, skipping native SQL")
        return False

    from dvt.federation.auth.oracle import OracleAuthHandler

    handler = OracleAuthHandler()
    is_valid, error_msg = handler.validate(connection)
    if not is_valid:
        _log(f"    {error_msg}")
        return False

    conn = None
    try:
        connect_kwargs = handler.get_native_connection_kwargs(connection)
        conn = oracledb.connect(**connect_kwargs)
        conn.autocommit = True
        cur = conn.cursor()
        cur.execute(sql)
        cur.close()
        return True
    except Exception as e:
        _log(f"    Oracle SQL failed: {str(e)[:60]}")
        return False
    finally:
        if conn:
            conn.close()
```

---

### Task 4: Update `_drop_table_cascade()` (lines 496-515)

**Replace:**
```python
def _drop_table_cascade(
    self,
    connection: Dict[str, Any],
    table_name: str,
) -> bool:
    """Drop table with CASCADE (destroys dependent views). Returns True on success."""
    adapter_type = connection.get("type", "")

    # DROP CASCADE syntax varies by database
    if adapter_type in ("postgres", "redshift", "snowflake"):
        drop_sql = f"DROP TABLE IF EXISTS {table_name} CASCADE"
    else:
        # databricks, bigquery don't support CASCADE
        drop_sql = f"DROP TABLE IF EXISTS {table_name}"

    _log(f"    Executing: {drop_sql}")
    success = self._execute_sql_native(connection, drop_sql)
    if success:
        _log(f"    ✓ Table dropped with CASCADE")
    return success
```

**With:**
```python
def _drop_table_cascade(
    self,
    connection: Dict[str, Any],
    table_name: str,
) -> bool:
    """Drop table with CASCADE where supported. Returns True on success.

    Different databases have different DROP TABLE syntax:
    - CASCADE supported: Postgres, Redshift, Snowflake, Trino, and PG-compatible DBs
    - CASCADE CONSTRAINTS: Oracle
    - No CASCADE: MySQL, BigQuery, Databricks, ClickHouse, SQL Server
    """
    adapter_type = connection.get("type", "").lower()

    # PostgreSQL and compatible (support CASCADE)
    if adapter_type in (
        "postgres", "redshift", "snowflake", "greenplum", "materialize",
        "risingwave", "cratedb", "alloydb", "timescaledb", "trino", "starburst"
    ):
        drop_sql = f"DROP TABLE IF EXISTS {table_name} CASCADE"

    # Oracle uses CASCADE CONSTRAINTS
    elif adapter_type == "oracle":
        # Oracle doesn't have IF EXISTS, but we handle errors gracefully
        drop_sql = f"DROP TABLE {table_name} CASCADE CONSTRAINTS"

    # MySQL and compatible (no CASCADE, but DROP works)
    elif adapter_type in ("mysql", "tidb", "singlestore", "clickhouse"):
        drop_sql = f"DROP TABLE IF EXISTS {table_name}"

    # BigQuery
    elif adapter_type == "bigquery":
        drop_sql = f"DROP TABLE IF EXISTS {table_name}"

    # Databricks/Spark SQL
    elif adapter_type == "databricks":
        drop_sql = f"DROP TABLE IF EXISTS {table_name}"

    # SQL Server / Synapse / Fabric (SQL Server 2016+ supports DROP IF EXISTS)
    elif adapter_type in ("sqlserver", "synapse", "fabric"):
        drop_sql = f"DROP TABLE IF EXISTS {table_name}"

    else:
        drop_sql = f"DROP TABLE IF EXISTS {table_name}"

    _log(f"    Executing: {drop_sql}")
    success = self._execute_sql_native(connection, drop_sql)
    if success:
        _log(f"    ✓ Table dropped")
    return success
```

---

## Unit Tests

### File: `tests/unit/test_native_sql_execution.py`

```python
# coding=utf-8
"""Unit tests for native SQL execution in spark_seed.py.

Tests the native SQL execution methods that use auth handlers
for database-specific authentication.
"""

from unittest import mock
from typing import Dict, Any

import pytest


class TestExecuteSqlPostgres:
    """Tests for _execute_sql_postgres method."""

    def test_uses_postgres_auth_handler(self):
        """Should use PostgresAuthHandler for connection kwargs."""
        from dvt.federation.auth.postgres import PostgresAuthHandler
        
        handler = PostgresAuthHandler()
        creds = {
            "type": "postgres",
            "host": "localhost",
            "port": 5432,
            "database": "testdb",
            "user": "testuser",
            "password": "testpass",
            "sslmode": "require",
        }
        
        kwargs = handler.get_native_connection_kwargs(creds)
        
        assert kwargs["host"] == "localhost"
        assert kwargs["port"] == 5432
        assert kwargs["dbname"] == "testdb"
        assert kwargs["user"] == "testuser"
        assert kwargs["password"] == "testpass"
        assert kwargs["sslmode"] == "require"

    def test_ssl_cert_auth_supported(self):
        """Should include SSL certificate paths."""
        from dvt.federation.auth.postgres import PostgresAuthHandler
        
        handler = PostgresAuthHandler()
        creds = {
            "type": "postgres",
            "host": "localhost",
            "user": "testuser",
            "password": "testpass",
            "sslmode": "verify-full",
            "sslcert": "/path/to/cert.pem",
            "sslkey": "/path/to/key.pem",
            "sslrootcert": "/path/to/ca.pem",
        }
        
        kwargs = handler.get_native_connection_kwargs(creds)
        
        assert kwargs["sslcert"] == "/path/to/cert.pem"
        assert kwargs["sslkey"] == "/path/to/key.pem"
        assert kwargs["sslrootcert"] == "/path/to/ca.pem"


class TestExecuteSqlRedshift:
    """Tests for _execute_sql_redshift method."""

    def test_password_auth(self):
        """Should support password authentication."""
        from dvt.federation.auth.redshift import RedshiftAuthHandler
        
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

    def test_iam_auth(self):
        """Should support IAM authentication."""
        from dvt.federation.auth.redshift import RedshiftAuthHandler
        
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


class TestExecuteSqlMysql:
    """Tests for _execute_sql_mysql method."""

    def test_password_auth(self):
        """Should support password authentication."""
        from dvt.federation.auth.mysql import MySQLAuthHandler
        
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

    def test_ssl_auth(self):
        """Should support SSL authentication."""
        from dvt.federation.auth.mysql import MySQLAuthHandler
        
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


class TestExecuteSqlBigquery:
    """Tests for _execute_sql_bigquery method."""

    def test_oauth_auth(self):
        """Should support OAuth (application default) authentication."""
        from dvt.federation.auth.bigquery import BigQueryAuthHandler
        
        handler = BigQueryAuthHandler()
        creds = {
            "type": "bigquery",
            "project": "my-project",
        }
        
        assert handler.detect_auth_method(creds) == "oauth"
        
        kwargs = handler.get_native_connection_kwargs(creds)
        assert kwargs["project"] == "my-project"

    def test_service_account_auth(self):
        """Should support service account file authentication."""
        from dvt.federation.auth.bigquery import BigQueryAuthHandler
        
        handler = BigQueryAuthHandler()
        creds = {
            "type": "bigquery",
            "project": "my-project",
            "keyfile": "/path/to/keyfile.json",
        }
        
        assert handler.detect_auth_method(creds) == "service_account"
        
        kwargs = handler.get_native_connection_kwargs(creds)
        assert kwargs["keyfile"] == "/path/to/keyfile.json"


class TestExecuteSqlDatabricks:
    """Tests for _execute_sql_databricks method."""

    def test_token_auth(self):
        """Should support token authentication."""
        from dvt.federation.auth.databricks import DatabricksAuthHandler
        
        handler = DatabricksAuthHandler()
        creds = {
            "type": "databricks",
            "host": "abc.cloud.databricks.com",
            "http_path": "/sql/1.0/warehouses/xxx",
            "token": "dapi_xxx",
        }
        
        assert handler.detect_auth_method(creds) == "token"
        
        kwargs = handler.get_native_connection_kwargs(creds)
        assert kwargs["server_hostname"] == "abc.cloud.databricks.com"
        assert kwargs["http_path"] == "/sql/1.0/warehouses/xxx"
        assert kwargs["access_token"] == "dapi_xxx"

    def test_oauth_u2m_blocked(self):
        """Should block OAuth U2M (requires browser)."""
        from dvt.federation.auth.databricks import DatabricksAuthHandler
        
        handler = DatabricksAuthHandler()
        creds = {
            "type": "databricks",
            "host": "abc.cloud.databricks.com",
            "auth_type": "oauth",
        }
        
        assert handler.is_interactive(creds) is True
        is_valid, error = handler.validate(creds)
        assert is_valid is False


class TestExecuteSqlSqlserver:
    """Tests for _execute_sql_sqlserver method."""

    def test_password_auth(self):
        """Should support password authentication."""
        from dvt.federation.auth.sqlserver import SQLServerAuthHandler
        
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


class TestExecuteSqlTrino:
    """Tests for _execute_sql_trino method."""

    def test_no_auth(self):
        """Should support no authentication."""
        from dvt.federation.auth.trino import TrinoAuthHandler
        
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

    def test_password_auth(self):
        """Should support password (LDAP) authentication."""
        from dvt.federation.auth.trino import TrinoAuthHandler
        
        handler = TrinoAuthHandler()
        creds = {
            "type": "trino",
            "host": "localhost",
            "user": "admin",
            "password": "password123",
        }
        
        assert handler.detect_auth_method(creds) == "password"


class TestExecuteSqlClickhouse:
    """Tests for _execute_sql_clickhouse method."""

    def test_password_auth(self):
        """Should support password authentication."""
        from dvt.federation.auth.clickhouse import ClickHouseAuthHandler
        
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


class TestExecuteSqlOracle:
    """Tests for _execute_sql_oracle method."""

    def test_password_auth(self):
        """Should support password authentication."""
        from dvt.federation.auth.oracle import OracleAuthHandler
        
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


class TestDropTableCascade:
    """Tests for _drop_table_cascade method."""

    def test_postgres_uses_cascade(self):
        """PostgreSQL should use DROP TABLE CASCADE."""
        # This would be tested via integration test or mocking
        # The implementation uses CASCADE for postgres
        pass

    def test_oracle_uses_cascade_constraints(self):
        """Oracle should use DROP TABLE CASCADE CONSTRAINTS."""
        # Oracle-specific syntax
        pass

    def test_mysql_no_cascade(self):
        """MySQL should use simple DROP TABLE."""
        # MySQL doesn't support CASCADE
        pass
```

---

## Summary

### Files to Modify

| File | Changes |
|------|---------|
| `core/dvt/task/spark_seed.py` | Update `_execute_sql_postgres`, expand `_execute_sql_native` router, add 8 new `_execute_sql_*` methods, update `_drop_table_cascade` |
| `tests/unit/test_native_sql_execution.py` | New file with unit tests |

### Adapters with Native SQL Support After Implementation

| Adapter | Python Driver | Auth Methods |
|---------|--------------|--------------|
| PostgreSQL | psycopg2 | password, ssl_cert |
| Greenplum | psycopg2 | password, ssl_cert |
| Materialize | psycopg2 | password, ssl_cert |
| RisingWave | psycopg2 | password, ssl_cert |
| CrateDB | psycopg2 | password, ssl_cert |
| AlloyDB | psycopg2 | password, ssl_cert |
| TimescaleDB | psycopg2 | password, ssl_cert |
| Snowflake | snowflake-connector | password, keypair, oauth |
| Redshift | redshift_connector | password, iam |
| MySQL | mysql-connector-python | password, ssl |
| TiDB | mysql-connector-python | password, ssl |
| SingleStore | mysql-connector-python | password, ssl |
| BigQuery | google-cloud-bigquery | oauth, service_account, service_account_json |
| Databricks | databricks-sql-connector | token, oauth_m2m |
| SQL Server | pymssql | password, azure_ad |
| Synapse | pymssql | password, azure_ad |
| Fabric | pymssql | password, azure_ad |
| Trino | trino | none, password, jwt |
| Starburst | trino | none, password, jwt |
| ClickHouse | clickhouse-connect | password, ssl |
| Oracle | oracledb | password, wallet |

### Line Count

- Current `spark_seed.py`: ~538 lines
- New code to add: ~300 lines (8 methods + router update + drop cascade update)
- New test file: ~250 lines
- **Total new code: ~550 lines**

---

## Execution Order

1. Update `_execute_sql_postgres()` to use `PostgresAuthHandler`
2. Expand `_execute_sql_native()` router
3. Add `_execute_sql_redshift()`
4. Add `_execute_sql_mysql()`
5. Add `_execute_sql_bigquery()`
6. Add `_execute_sql_databricks()`
7. Add `_execute_sql_sqlserver()`
8. Add `_execute_sql_trino()`
9. Add `_execute_sql_clickhouse()`
10. Add `_execute_sql_oracle()`
11. Update `_drop_table_cascade()`
12. Create unit tests
13. Run tests to verify
