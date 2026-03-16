"""
Map dbt profiles.yml adapter configs to Sling connection URLs.

Each adapter type (postgres, snowflake, etc.) has a specific URL format
that Sling understands. This module translates the structured dbt profile
output config into a Sling-compatible connection URL or dict.
"""

from typing import Any, Dict, Optional
from urllib.parse import quote_plus


def map_to_sling_url(output_config: Dict[str, Any]) -> str:
    """Convert a dbt profiles.yml output config to a Sling connection URL.

    Args:
        output_config: A single output from profiles.yml with at minimum a 'type' key.

    Returns:
        A Sling-compatible connection URL string.

    Raises:
        ValueError: If the adapter type is not supported or required fields are missing.
    """
    adapter_type = output_config.get("type")
    if not adapter_type:
        raise ValueError("Output config must have a 'type' field")

    mapper = _MAPPERS.get(adapter_type)
    if not mapper:
        raise ValueError(
            f"Unsupported adapter type '{adapter_type}'. "
            f"Supported: {', '.join(sorted(_MAPPERS.keys()))}"
        )

    return mapper(output_config)


def _safe(val: Any, default: str = "") -> str:
    """Safely convert a value to string, URL-encoding if needed."""
    if val is None:
        return default
    return str(val)


def _quote(val: Any) -> str:
    """URL-encode a value for use in connection strings."""
    if val is None:
        return ""
    return quote_plus(str(val))


# ---------------------------------------------------------------------------
# Per-adapter mappers
# ---------------------------------------------------------------------------


def _map_postgres(c: Dict[str, Any]) -> str:
    user = _quote(c.get("user", ""))
    password = _quote(c.get("password", c.get("pass", "")))
    host = _safe(c.get("host", "localhost"))
    port = _safe(c.get("port", "5432"))
    dbname = _safe(c.get("dbname", c.get("database", "")))
    sslmode = _safe(c.get("sslmode", "disable"))
    url = f"postgres://{user}:{password}@{host}:{port}/{dbname}?sslmode={sslmode}"
    schema = c.get("schema")
    if schema:
        url += f"&search_path={_quote(schema)}"
    return url


def _map_redshift(c: Dict[str, Any]) -> str:
    user = _quote(c.get("user", ""))
    password = _quote(c.get("password", c.get("pass", "")))
    host = _safe(c.get("host", ""))
    port = _safe(c.get("port", "5439"))
    dbname = _safe(c.get("dbname", c.get("database", "")))
    sslmode = _safe(c.get("sslmode", "require"))
    return f"redshift://{user}:{password}@{host}:{port}/{dbname}?sslmode={sslmode}"


def _map_mysql(c: Dict[str, Any]) -> str:
    user = _quote(c.get("user", c.get("username", "")))
    password = _quote(c.get("password", c.get("pass", "")))
    host = _safe(c.get("host", c.get("server", "localhost")))
    port = _safe(c.get("port", "3306"))
    database = _safe(c.get("database", c.get("schema", "")))
    return f"mysql://{user}:{password}@{host}:{port}/{database}"


def _map_sqlserver(c: Dict[str, Any]) -> str:
    user = _quote(c.get("user", ""))
    password = _quote(c.get("password", c.get("pass", "")))
    host = _safe(c.get("host", c.get("server", "")))
    port = _safe(c.get("port", "1433"))
    database = _safe(c.get("database", ""))
    url = f"sqlserver://{user}:{password}@{host}:{port}/{database}"
    params = []
    encrypt = c.get("encrypt")
    if encrypt is not None:
        params.append(f"encrypt={str(encrypt).lower()}")
    trust_cert = c.get("trust_cert", c.get("TrustServerCertificate"))
    if trust_cert is not None:
        params.append(f"TrustServerCertificate={str(trust_cert).lower()}")
    elif encrypt is not None and not encrypt:
        # When encrypt=false on a local Docker, also trust the self-signed cert
        params.append("TrustServerCertificate=true")
    if params:
        url += "?" + "&".join(params)
    return url


def _map_snowflake(c: Dict[str, Any]) -> str:
    user = _quote(c.get("user", ""))
    password = _quote(c.get("password", c.get("pass", "")))
    account = _safe(c.get("account", ""))
    database = _safe(c.get("database", ""))
    parts = [f"snowflake://{user}:{password}@{account}/{database}"]
    params = []
    schema = c.get("schema")
    if schema:
        params.append(f"schema={_quote(schema)}")
    warehouse = c.get("warehouse")
    if warehouse:
        params.append(f"warehouse={_quote(warehouse)}")
    role = c.get("role")
    if role:
        params.append(f"role={_quote(role)}")
    if params:
        parts.append("?" + "&".join(params))
    return "".join(parts)


def _map_bigquery(c: Dict[str, Any]) -> str:
    project = _safe(c.get("project", c.get("database", "")))
    dataset = _safe(c.get("dataset", c.get("schema", "")))
    location = c.get("location")
    url = f"bigquery://{project}"
    params = []
    if dataset:
        params.append(f"dataset={_quote(dataset)}")
    if location:
        params.append(f"location={_quote(location)}")
    if params:
        url += "?" + "&".join(params)
    return url


def _map_databricks(c: Dict[str, Any]) -> str:
    host = _safe(c.get("host", ""))
    port = _safe(c.get("port", "443"))
    token = _safe(c.get("token", ""))
    http_path = _safe(c.get("http_path", ""))
    catalog = c.get("catalog")
    schema = c.get("schema")
    url = f"databricks://{host}:{port}"
    params = []
    if token:
        params.append(f"token={_quote(token)}")
    if http_path:
        params.append(f"http_path={http_path}")
    if catalog:
        params.append(f"catalog={_quote(catalog)}")
    if schema:
        params.append(f"schema={_quote(schema)}")
    if params:
        url += "?" + "&".join(params)
    return url


def _map_oracle(c: Dict[str, Any]) -> str:
    user = _quote(c.get("user", ""))
    password = _quote(c.get("password", c.get("pass", "")))
    host = _safe(c.get("host", ""))
    port = _safe(c.get("port", "1521"))
    service = _safe(c.get("service", c.get("database", "")))
    # Oracle Sling connection uses service_name parameter
    return f"oracle://{user}:{password}@{host}:{port}?service_name={service}"


def _map_clickhouse(c: Dict[str, Any]) -> str:
    user = _quote(c.get("user", c.get("username", "default")))
    password = _quote(c.get("password", c.get("pass", "")))
    host = _safe(c.get("host", "localhost"))
    port = _safe(c.get("port", "9000"))
    database = _safe(c.get("database", c.get("schema", "default")))
    return f"clickhouse://{user}:{password}@{host}:{port}/{database}"


def _map_trino(c: Dict[str, Any]) -> str:
    user = _quote(c.get("user", ""))
    host = _safe(c.get("host", ""))
    port = _safe(c.get("port", "8080"))
    catalog = _safe(c.get("catalog", c.get("database", "")))
    schema = c.get("schema")
    url = f"trino://{user}@{host}:{port}/{catalog}"
    if schema:
        url += f"?schema={_quote(schema)}"
    return url


def _map_duckdb(c: Dict[str, Any]) -> str:
    path = _safe(c.get("path", c.get("database", ":memory:")))
    return f"duckdb://{path}"


def _map_sqlite(c: Dict[str, Any]) -> str:
    path = _safe(c.get("path", c.get("database", "")))
    return f"sqlite://{path}"


def _map_s3(c: Dict[str, Any]) -> str:
    bucket = _safe(c.get("bucket", ""))
    region = c.get("region")
    access_key = c.get("access_key_id")
    secret_key = c.get("secret_access_key")
    url = f"s3://{bucket}"
    params = []
    if region:
        params.append(f"region={_quote(region)}")
    if access_key:
        params.append(f"access_key_id={_quote(access_key)}")
    if secret_key:
        params.append(f"secret_access_key={_quote(secret_key)}")
    if params:
        url += "?" + "&".join(params)
    return url


def _map_gcs(c: Dict[str, Any]) -> str:
    bucket = _safe(c.get("bucket", ""))
    project = c.get("project")
    url = f"gs://{bucket}"
    if project:
        url += f"?project={_quote(project)}"
    return url


def _map_azure(c: Dict[str, Any]) -> str:
    account = _safe(c.get("account", ""))
    container = _safe(c.get("container", ""))
    access_key = c.get("access_key")
    url = f"azure://{account}/{container}"
    if access_key:
        url += f"?access_key={_quote(access_key)}"
    return url


# Registry of all mappers
_MAPPERS = {
    "postgres": _map_postgres,
    "redshift": _map_redshift,
    "mysql": _map_mysql,
    "mariadb": _map_mysql,
    "sqlserver": _map_sqlserver,
    "snowflake": _map_snowflake,
    "bigquery": _map_bigquery,
    "databricks": _map_databricks,
    "oracle": _map_oracle,
    "clickhouse": _map_clickhouse,
    "trino": _map_trino,
    "duckdb": _map_duckdb,
    "sqlite": _map_sqlite,
    "s3": _map_s3,
    "gcs": _map_gcs,
    "azure": _map_azure,
}


def supported_types() -> list:
    """Return list of supported adapter types."""
    return sorted(_MAPPERS.keys())
