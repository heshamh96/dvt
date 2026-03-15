"""
Dialect-specific watermark literal formatting.

When DVT pre-resolves the incremental watermark from the target and needs to
inject it into a source-side extraction query, the literal must be formatted
in the SOURCE engine's SQL dialect.

For example, Oracle requires TO_TIMESTAMP(...), SQL Server requires CONVERT(...),
while MySQL/Postgres accept plain string literals.
"""

from datetime import date, datetime
from typing import Any, Union


def format_watermark(
    value: Any,
    source_type: str,
    column_type: str = "timestamp",
) -> str:
    """Format a watermark value as a valid SQL literal for the source dialect.

    Args:
        value: The raw watermark value (datetime, date, int, float, str, None).
        source_type: The source adapter type (postgres, mysql, oracle, etc.).
        column_type: The logical column type (timestamp, date, integer, string, etc.).

    Returns:
        A valid SQL literal string for the source dialect.

    Raises:
        ValueError: If the value cannot be formatted for the given dialect.
    """
    if value is None:
        return "NULL"

    # Integer/numeric — universal
    if column_type in ("integer", "bigint", "smallint", "int"):
        return str(int(value))

    if column_type in ("float", "decimal", "numeric", "number"):
        return str(value)

    # Date
    if column_type == "date":
        if isinstance(value, (datetime, date)):
            date_str = value.strftime("%Y-%m-%d")
        else:
            date_str = str(value)[:10]  # take YYYY-MM-DD portion
        formatter = _DATE_FORMATTERS.get(source_type, _date_default)
        return formatter(date_str)

    # Timestamp (default)
    if column_type in (
        "timestamp",
        "timestampz",
        "datetime",
        "timestamp_ntz",
        "timestamp_tz",
    ):
        if isinstance(value, datetime):
            ts_str = value.strftime("%Y-%m-%d %H:%M:%S.%f")
        elif isinstance(value, date):
            ts_str = value.strftime("%Y-%m-%d") + " 00:00:00.000000"
        else:
            ts_str = str(value)
        formatter = _TIMESTAMP_FORMATTERS.get(source_type, _ts_default)
        return formatter(ts_str)

    # String — dialect-appropriate quoting
    escaped = str(value).replace("'", "''")
    formatter = _STRING_FORMATTERS.get(source_type, _str_default)
    return formatter(escaped)


# ---------------------------------------------------------------------------
# Timestamp formatters by dialect
# ---------------------------------------------------------------------------


def _ts_postgres(v: str) -> str:
    return f"'{v}'::TIMESTAMP"


def _ts_redshift(v: str) -> str:
    return f"'{v}'::TIMESTAMP"


def _ts_mysql(v: str) -> str:
    return f"'{v}'"


def _ts_sqlserver(v: str) -> str:
    return f"CONVERT(DATETIME2, '{v}', 121)"


def _ts_oracle(v: str) -> str:
    return f"TO_TIMESTAMP('{v}', 'YYYY-MM-DD HH24:MI:SS.FF6')"


def _ts_snowflake(v: str) -> str:
    return f"TO_TIMESTAMP('{v}')"


def _ts_bigquery(v: str) -> str:
    return f"TIMESTAMP '{v}'"


def _ts_databricks(v: str) -> str:
    return f"TIMESTAMP '{v}'"


def _ts_clickhouse(v: str) -> str:
    return f"toDateTime64('{v}', 6)"


def _ts_trino(v: str) -> str:
    return f"TIMESTAMP '{v}'"


def _ts_duckdb(v: str) -> str:
    return f"TIMESTAMP '{v}'"


def _ts_sqlite(v: str) -> str:
    return f"'{v}'"


def _ts_default(v: str) -> str:
    return f"'{v}'"


_TIMESTAMP_FORMATTERS = {
    "postgres": _ts_postgres,
    "redshift": _ts_redshift,
    "mysql": _ts_mysql,
    "mariadb": _ts_mysql,
    "sqlserver": _ts_sqlserver,
    "oracle": _ts_oracle,
    "snowflake": _ts_snowflake,
    "bigquery": _ts_bigquery,
    "databricks": _ts_databricks,
    "clickhouse": _ts_clickhouse,
    "trino": _ts_trino,
    "duckdb": _ts_duckdb,
    "sqlite": _ts_sqlite,
}


# ---------------------------------------------------------------------------
# Date formatters by dialect
# ---------------------------------------------------------------------------


def _date_postgres(v: str) -> str:
    return f"'{v}'::DATE"


def _date_redshift(v: str) -> str:
    return f"'{v}'::DATE"


def _date_mysql(v: str) -> str:
    return f"'{v}'"


def _date_sqlserver(v: str) -> str:
    return f"CONVERT(DATE, '{v}', 23)"


def _date_oracle(v: str) -> str:
    return f"TO_DATE('{v}', 'YYYY-MM-DD')"


def _date_snowflake(v: str) -> str:
    return f"TO_DATE('{v}')"


def _date_bigquery(v: str) -> str:
    return f"DATE '{v}'"


def _date_databricks(v: str) -> str:
    return f"DATE '{v}'"


def _date_clickhouse(v: str) -> str:
    return f"toDate('{v}')"


def _date_trino(v: str) -> str:
    return f"DATE '{v}'"


def _date_duckdb(v: str) -> str:
    return f"DATE '{v}'"


def _date_sqlite(v: str) -> str:
    return f"'{v}'"


def _date_default(v: str) -> str:
    return f"'{v}'"


_DATE_FORMATTERS = {
    "postgres": _date_postgres,
    "redshift": _date_redshift,
    "mysql": _date_mysql,
    "mariadb": _date_mysql,
    "sqlserver": _date_sqlserver,
    "oracle": _date_oracle,
    "snowflake": _date_snowflake,
    "bigquery": _date_bigquery,
    "databricks": _date_databricks,
    "clickhouse": _date_clickhouse,
    "trino": _date_trino,
    "duckdb": _date_duckdb,
    "sqlite": _date_sqlite,
}


# ---------------------------------------------------------------------------
# String formatters by dialect
# ---------------------------------------------------------------------------


def _str_sqlserver(v: str) -> str:
    return f"N'{v}'"  # Unicode prefix for SQL Server


def _str_default(v: str) -> str:
    return f"'{v}'"


_STRING_FORMATTERS = {
    "sqlserver": _str_sqlserver,
}


# ---------------------------------------------------------------------------
# Public helpers
# ---------------------------------------------------------------------------


def supported_dialects() -> list:
    """Return list of dialects with explicit timestamp formatting support."""
    return sorted(_TIMESTAMP_FORMATTERS.keys())
