"""
Dialect-specific SQL syntax fallbacks for 25+ database adapters.

This module provides comprehensive fallback syntax for SQL operations
that may not transpile correctly through SQLGlot. It covers:

- LIMIT/TOP/FETCH FIRST syntax variations
- TABLESAMPLE/SAMPLE syntax variations
- OFFSET syntax variations
- Random ordering for sampling fallback

Adapter coverage:
- PostgreSQL family: postgres, greenplum, redshift, materialize, risingwave,
                     cratedb, alloydb, timescaledb
- MySQL family: mysql, tidb, singlestore, mariadb
- SQL Server family: sqlserver, synapse, fabric
- Oracle family: oracle, exasol
- Cloud DWs: snowflake, bigquery, databricks, athena, firebolt
- Hadoop: hive, impala, trino, starburst
- Others: clickhouse, duckdb, vertica, db2

Usage:
    from dvt.federation.dialect_fallbacks import (
        get_limit_clause,
        get_sample_clause,
        build_extraction_query_fallback,
    )

    # Get LIMIT clause for SQL Server
    position, clause = get_limit_clause("sqlserver", 100)
    # Returns: ("prefix", "TOP 100")

    # Get SAMPLE clause for Snowflake
    sample = get_sample_clause("snowflake", 10.0)
    # Returns: "SAMPLE (10)"
"""

from typing import Dict, List, Optional, Tuple


# =============================================================================
# Adapter Family Groupings
# =============================================================================

POSTGRES_FAMILY = [
    "postgres",
    "greenplum",
    "redshift",
    "materialize",
    "risingwave",
    "cratedb",
    "alloydb",
    "timescaledb",
]

MYSQL_FAMILY = [
    "mysql",
    "tidb",
    "singlestore",
    "mariadb",
]

SQLSERVER_FAMILY = [
    "sqlserver",
    "synapse",
    "fabric",
]

ORACLE_FAMILY = [
    "oracle",
    "exasol",
]

TRINO_FAMILY = [
    "trino",
    "starburst",
]


# =============================================================================
# LIMIT Syntax
# =============================================================================
# Returns (position, template) where:
# - position: "prefix" = after SELECT keyword, "suffix" = at query end
# - template: format string with {n} placeholder

LIMIT_SYNTAX: Dict[str, Tuple[str, str]] = {
    # PostgreSQL family - LIMIT at end
    "postgres": ("suffix", "LIMIT {n}"),
    "greenplum": ("suffix", "LIMIT {n}"),
    "redshift": ("suffix", "LIMIT {n}"),
    "materialize": ("suffix", "LIMIT {n}"),
    "risingwave": ("suffix", "LIMIT {n}"),
    "cratedb": ("suffix", "LIMIT {n}"),
    "alloydb": ("suffix", "LIMIT {n}"),
    "timescaledb": ("suffix", "LIMIT {n}"),
    # MySQL family - LIMIT at end
    "mysql": ("suffix", "LIMIT {n}"),
    "tidb": ("suffix", "LIMIT {n}"),
    "singlestore": ("suffix", "LIMIT {n}"),
    "mariadb": ("suffix", "LIMIT {n}"),
    # SQL Server family - TOP after SELECT
    "sqlserver": ("prefix", "TOP {n}"),
    "synapse": ("prefix", "TOP {n}"),
    "fabric": ("prefix", "TOP {n}"),
    # Oracle family - FETCH FIRST at end (Oracle 12c+)
    "oracle": ("suffix", "FETCH FIRST {n} ROWS ONLY"),
    "exasol": ("suffix", "LIMIT {n}"),
    # Cloud DWs
    "snowflake": ("suffix", "LIMIT {n}"),
    "bigquery": ("suffix", "LIMIT {n}"),
    "databricks": ("suffix", "LIMIT {n}"),
    "athena": ("suffix", "LIMIT {n}"),
    "firebolt": ("suffix", "LIMIT {n}"),
    # Hadoop ecosystem
    "hive": ("suffix", "LIMIT {n}"),
    "impala": ("suffix", "LIMIT {n}"),
    "trino": ("suffix", "LIMIT {n}"),
    "starburst": ("suffix", "LIMIT {n}"),
    # Others
    "clickhouse": ("suffix", "LIMIT {n}"),
    "duckdb": ("suffix", "LIMIT {n}"),
    "vertica": ("suffix", "LIMIT {n}"),
    "db2": ("suffix", "FETCH FIRST {n} ROWS ONLY"),
    "teradata": ("prefix", "TOP {n}"),
}


# =============================================================================
# OFFSET Syntax
# =============================================================================

OFFSET_SYNTAX: Dict[str, str] = {
    # Standard OFFSET
    "postgres": "OFFSET {n}",
    "greenplum": "OFFSET {n}",
    "redshift": "OFFSET {n}",
    "materialize": "OFFSET {n}",
    "risingwave": "OFFSET {n}",
    "cratedb": "OFFSET {n}",
    "alloydb": "OFFSET {n}",
    "timescaledb": "OFFSET {n}",
    "mysql": "OFFSET {n}",
    "tidb": "OFFSET {n}",
    "singlestore": "OFFSET {n}",
    "mariadb": "OFFSET {n}",
    "snowflake": "OFFSET {n}",
    "bigquery": "OFFSET {n}",
    "databricks": "OFFSET {n}",
    "hive": "OFFSET {n}",  # Hive 2.0+
    "trino": "OFFSET {n}",
    "starburst": "OFFSET {n}",
    "clickhouse": "OFFSET {n}",
    "duckdb": "OFFSET {n}",
    "vertica": "OFFSET {n}",
    "firebolt": "OFFSET {n}",
    "exasol": "OFFSET {n}",
    # Oracle 12c+ style
    "oracle": "OFFSET {n} ROWS",
    "db2": "OFFSET {n} ROWS",
    # SQL Server - requires ORDER BY for OFFSET
    "sqlserver": "OFFSET {n} ROWS",
    "synapse": "OFFSET {n} ROWS",
    "fabric": "OFFSET {n} ROWS",
}


# =============================================================================
# TABLESAMPLE Syntax
# =============================================================================
# Returns template string or None if sampling not supported
# Template uses {percent} placeholder for percentage value

SAMPLE_SYNTAX: Dict[str, Optional[str]] = {
    # PostgreSQL - BERNOULLI for row-level, SYSTEM for block-level
    "postgres": "TABLESAMPLE BERNOULLI ({percent})",
    "greenplum": "TABLESAMPLE BERNOULLI ({percent})",
    "timescaledb": "TABLESAMPLE BERNOULLI ({percent})",
    "alloydb": "TABLESAMPLE BERNOULLI ({percent})",
    # Redshift - no native TABLESAMPLE
    "redshift": None,
    "materialize": None,
    "risingwave": None,
    "cratedb": None,
    # MySQL - no native TABLESAMPLE
    "mysql": None,
    "tidb": None,
    "singlestore": None,
    "mariadb": None,
    # SQL Server
    "sqlserver": "TABLESAMPLE ({percent} PERCENT)",
    "synapse": "TABLESAMPLE ({percent} PERCENT)",
    "fabric": "TABLESAMPLE ({percent} PERCENT)",
    # Oracle
    "oracle": "SAMPLE ({percent})",
    "exasol": None,
    # Cloud DWs
    "snowflake": "SAMPLE ({percent})",
    "bigquery": "TABLESAMPLE SYSTEM ({percent} PERCENT)",
    "databricks": "TABLESAMPLE ({percent} PERCENT)",
    "athena": None,
    "firebolt": None,
    # Hadoop ecosystem
    "hive": "TABLESAMPLE ({percent} PERCENT)",
    "impala": "TABLESAMPLE SYSTEM ({percent})",
    "trino": "TABLESAMPLE BERNOULLI ({percent})",
    "starburst": "TABLESAMPLE BERNOULLI ({percent})",
    # Others
    "clickhouse": None,  # Use LIMIT with ORDER BY rand()
    "duckdb": "TABLESAMPLE ({percent}%)",
    "vertica": None,
    "db2": "TABLESAMPLE BERNOULLI ({percent})",
}


# =============================================================================
# Random Ordering for Sampling Fallback
# =============================================================================
# Used when TABLESAMPLE not supported: ORDER BY random_func() LIMIT n

RANDOM_FUNCTION: Dict[str, str] = {
    "postgres": "RANDOM()",
    "greenplum": "RANDOM()",
    "redshift": "RANDOM()",
    "materialize": "RANDOM()",
    "risingwave": "RANDOM()",
    "cratedb": "RANDOM()",
    "alloydb": "RANDOM()",
    "timescaledb": "RANDOM()",
    "mysql": "RAND()",
    "tidb": "RAND()",
    "singlestore": "RAND()",
    "mariadb": "RAND()",
    "sqlserver": "NEWID()",
    "synapse": "NEWID()",
    "fabric": "NEWID()",
    "oracle": "DBMS_RANDOM.VALUE",
    "exasol": "RANDOM()",
    "snowflake": "RANDOM()",
    "bigquery": "RAND()",
    "databricks": "RAND()",
    "athena": "RAND()",
    "firebolt": "RANDOM()",
    "hive": "RAND()",
    "impala": "RAND()",
    "trino": "RAND()",
    "starburst": "RAND()",
    "clickhouse": "rand()",
    "duckdb": "RANDOM()",
    "vertica": "RANDOM()",
    "db2": "RAND()",
}


# =============================================================================
# Quote Characters
# =============================================================================

IDENTIFIER_QUOTE: Dict[str, str] = {
    # Double quotes (ANSI standard)
    "postgres": '"',
    "greenplum": '"',
    "redshift": '"',
    "materialize": '"',
    "risingwave": '"',
    "cratedb": '"',
    "alloydb": '"',
    "timescaledb": '"',
    "oracle": '"',
    "exasol": '"',
    "snowflake": '"',
    "bigquery": "`",  # Backticks
    "databricks": "`",
    "athena": '"',
    "firebolt": '"',
    "hive": "`",
    "impala": "`",
    "trino": '"',
    "starburst": '"',
    "clickhouse": "`",
    "duckdb": '"',
    "vertica": '"',
    "db2": '"',
    # Brackets for SQL Server family
    "sqlserver": "[",  # Use [] pairs
    "synapse": "[",
    "fabric": "[",
    # Backticks for MySQL family
    "mysql": "`",
    "tidb": "`",
    "singlestore": "`",
    "mariadb": "`",
}


# =============================================================================
# Public Functions
# =============================================================================


def get_limit_clause(dialect: str, n: int) -> Tuple[str, str]:
    """Get LIMIT clause for a specific dialect.

    Args:
        dialect: Database dialect name (e.g., 'postgres', 'sqlserver')
        n: Number of rows to limit

    Returns:
        Tuple of (position, clause) where:
        - position: 'prefix' (after SELECT) or 'suffix' (at query end)
        - clause: Formatted LIMIT clause (e.g., 'LIMIT 100' or 'TOP 100')

    Example:
        >>> get_limit_clause("postgres", 100)
        ('suffix', 'LIMIT 100')
        >>> get_limit_clause("sqlserver", 100)
        ('prefix', 'TOP 100')
    """
    dialect = dialect.lower()
    if dialect not in LIMIT_SYNTAX:
        # Default to standard LIMIT
        return ("suffix", f"LIMIT {n}")

    position, template = LIMIT_SYNTAX[dialect]
    return (position, template.format(n=n))


def get_offset_clause(dialect: str, n: int) -> str:
    """Get OFFSET clause for a specific dialect.

    Args:
        dialect: Database dialect name
        n: Number of rows to offset

    Returns:
        Formatted OFFSET clause
    """
    dialect = dialect.lower()
    template = OFFSET_SYNTAX.get(dialect, "OFFSET {n}")
    return template.format(n=n)


def get_sample_clause(dialect: str, percent: float) -> Optional[str]:
    """Get TABLESAMPLE clause for a specific dialect.

    Args:
        dialect: Database dialect name
        percent: Percentage of rows to sample (0-100)

    Returns:
        Formatted SAMPLE clause, or None if not supported

    Example:
        >>> get_sample_clause("snowflake", 10.0)
        'SAMPLE (10)'
        >>> get_sample_clause("mysql", 10.0)
        None
    """
    dialect = dialect.lower()
    template = SAMPLE_SYNTAX.get(dialect)
    if template is None:
        return None
    return template.format(percent=percent)


def get_random_function(dialect: str) -> str:
    """Get random function for a specific dialect.

    Used for ORDER BY random() LIMIT n when TABLESAMPLE not supported.

    Args:
        dialect: Database dialect name

    Returns:
        Random function call string
    """
    dialect = dialect.lower()
    return RANDOM_FUNCTION.get(dialect, "RANDOM()")


def get_identifier_quote(dialect: str) -> str:
    """Get identifier quote character for a specific dialect.

    Args:
        dialect: Database dialect name

    Returns:
        Quote character (", `, or [)
    """
    dialect = dialect.lower()
    return IDENTIFIER_QUOTE.get(dialect, '"')


def quote_identifier(dialect: str, identifier: str) -> str:
    """Quote an identifier for a specific dialect.

    Args:
        dialect: Database dialect name
        identifier: Identifier to quote

    Returns:
        Quoted identifier
    """
    quote = get_identifier_quote(dialect)
    if quote == "[":
        return f"[{identifier}]"
    return f"{quote}{identifier}{quote}"


def supports_sample(dialect: str) -> bool:
    """Check if a dialect supports TABLESAMPLE.

    Args:
        dialect: Database dialect name

    Returns:
        True if TABLESAMPLE is supported
    """
    dialect = dialect.lower()
    return SAMPLE_SYNTAX.get(dialect) is not None


def build_extraction_query_fallback(
    dialect: str,
    schema: str,
    table: str,
    columns: Optional[List[str]] = None,
    predicates: Optional[List[str]] = None,
    limit: Optional[int] = None,
    offset: Optional[int] = None,
    sample_percent: Optional[float] = None,
    order_by: Optional[List[str]] = None,
) -> str:
    """Build an extraction query with dialect-specific syntax.

    This is used as a fallback when SQLGlot transpilation fails
    or for complex syntax that requires dialect-specific handling.

    Args:
        dialect: Database dialect name
        schema: Database schema
        table: Table name
        columns: Columns to select (None = all)
        predicates: WHERE conditions
        limit: Maximum rows to return
        offset: Number of rows to skip
        sample_percent: Percentage of rows to sample
        order_by: ORDER BY columns

    Returns:
        Complete SQL query string

    Example:
        >>> build_extraction_query_fallback(
        ...     dialect="postgres",
        ...     schema="public",
        ...     table="orders",
        ...     columns=["id", "date", "amount"],
        ...     predicates=["date > '2024-01-01'"],
        ...     limit=1000,
        ... )
        "SELECT id, date, amount FROM public.orders WHERE date > '2024-01-01' LIMIT 1000"
    """
    dialect = dialect.lower()
    parts = []

    # SELECT clause
    col_str = ", ".join(columns) if columns else "*"

    # Handle TOP for SQL Server family
    limit_position, limit_clause = (
        get_limit_clause(dialect, limit) if limit else ("", "")
    )

    if limit and limit_position == "prefix":
        parts.append(f"SELECT {limit_clause} {col_str}")
    else:
        parts.append(f"SELECT {col_str}")

    # FROM clause with optional TABLESAMPLE
    from_clause = f"FROM {schema}.{table}"

    if sample_percent is not None:
        sample_clause = get_sample_clause(dialect, sample_percent)
        if sample_clause:
            from_clause += f" {sample_clause}"
        else:
            # Fallback: use ORDER BY random() LIMIT n
            # Calculate approximate row count for limit
            if limit is None and sample_percent < 100:
                # Can't do random sampling without knowing row count
                # Just add ORDER BY random() and let limit handle it
                if order_by is None:
                    order_by = [get_random_function(dialect)]

    parts.append(from_clause)

    # WHERE clause
    if predicates:
        where_clause = " AND ".join(predicates)
        parts.append(f"WHERE {where_clause}")

    # ORDER BY clause
    if order_by:
        order_str = ", ".join(order_by)
        parts.append(f"ORDER BY {order_str}")

    # LIMIT clause (suffix position)
    if limit and limit_position == "suffix":
        parts.append(limit_clause)

    # OFFSET clause
    if offset:
        parts.append(get_offset_clause(dialect, offset))

    return " ".join(parts)


def get_dialect_for_adapter(adapter_type: str) -> str:
    """Map adapter type to SQLGlot dialect name.

    Args:
        adapter_type: DVT adapter type (e.g., 'postgres', 'snowflake')

    Returns:
        SQLGlot dialect name
    """
    # Most map directly, but some need translation
    dialect_map = {
        # PostgreSQL family → postgres
        "greenplum": "postgres",
        "materialize": "postgres",
        "risingwave": "postgres",
        "cratedb": "postgres",
        "alloydb": "postgres",
        "timescaledb": "postgres",
        # MySQL family → mysql
        "tidb": "mysql",
        "singlestore": "mysql",
        "mariadb": "mysql",
        # SQL Server family → tsql
        "sqlserver": "tsql",
        "synapse": "tsql",
        "fabric": "tsql",
        # Trino family → trino
        "starburst": "trino",
        # Others with direct mapping
        "postgres": "postgres",
        "redshift": "redshift",
        "mysql": "mysql",
        "oracle": "oracle",
        "snowflake": "snowflake",
        "bigquery": "bigquery",
        "databricks": "databricks",
        "hive": "hive",
        "trino": "trino",
        "clickhouse": "clickhouse",
        "duckdb": "duckdb",
        # Spark
        "spark": "spark",
    }

    return dialect_map.get(adapter_type.lower(), adapter_type.lower())
