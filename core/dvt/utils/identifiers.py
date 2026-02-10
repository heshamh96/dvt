# coding=utf-8
"""Identifier sanitization utilities using SQLGlot.

Provides cross-dialect column name sanitization for safe SQL operations.
This module handles the universal sanitization of column names to ensure
they work across all database adapters (Postgres, Snowflake, Databricks, etc.).

Key features:
- Strips leading/trailing whitespace
- Replaces spaces and special characters with underscores
- Handles dialect-specific identifier quoting via SQLGlot
- Preserves case sensitivity

Usage:
    from dvt.utils.identifiers import sanitize_column_name, sanitize_dataframe_columns

    # Sanitize a single column name
    clean_name = sanitize_column_name("Customer Code")  # Returns "Customer_Code"

    # Sanitize all columns in a Spark DataFrame
    df_clean = sanitize_dataframe_columns(df, adapter_type="databricks")
"""

import re
from typing import Any, Dict, List, Optional, Tuple

# SQLGlot is used for dialect-aware identifier quoting when needed
try:
    import sqlglot  # noqa: F401

    SQLGLOT_AVAILABLE = True
except ImportError:
    SQLGLOT_AVAILABLE = False


# Map DVT adapter types to SQLGlot dialect names
ADAPTER_TO_SQLGLOT: Dict[str, str] = {
    "postgres": "postgres",
    "snowflake": "snowflake",
    "mysql": "mysql",
    "redshift": "redshift",
    "bigquery": "bigquery",
    "databricks": "databricks",
    "spark": "spark",
    "trino": "trino",
    "duckdb": "duckdb",
    "clickhouse": "clickhouse",
    "oracle": "oracle",
    "sqlserver": "tsql",
    "hive": "hive",
}


def sanitize_column_name(name: str) -> str:
    """Sanitize a column name for universal SQL compatibility.

    Transforms column names to be safe for all SQL dialects by:
    - Stripping leading/trailing whitespace
    - Replacing spaces and special characters with underscores
    - Collapsing multiple consecutive underscores
    - Prefixing with underscore if name starts with a digit
    - Handling empty or all-special-character names

    Args:
        name: Original column name (e.g., "Customer Code", " Price ")

    Returns:
        Sanitized column name safe for all SQL dialects
        (e.g., "Customer_Code", "Price")

    Examples:
        >>> sanitize_column_name("Customer Code")
        'Customer_Code'
        >>> sanitize_column_name(" Total Price ")
        'Total_Price'
        >>> sanitize_column_name("123_start")
        '_123_start'
        >>> sanitize_column_name("normal_col")
        'normal_col'
    """
    if not name:
        return "_column"

    # Strip leading/trailing whitespace
    result = name.strip()

    if not result:
        return "_column"

    # Replace non-alphanumeric characters (except underscore) with underscore
    result = re.sub(r"[^a-zA-Z0-9_]", "_", result)

    # Collapse multiple consecutive underscores into one
    result = re.sub(r"_+", "_", result)

    # Remove leading/trailing underscores (created by special chars at edges)
    result = result.strip("_")

    # If empty after all processing, use default
    if not result:
        return "_column"

    # Prefix with underscore if starts with a digit (SQL identifiers can't start with numbers)
    if result[0].isdigit():
        result = "_" + result

    return result


def needs_sanitization(name: str) -> bool:
    """Check if a column name needs sanitization.

    A column name needs sanitization if it:
    - Has leading or trailing whitespace
    - Contains spaces or special characters
    - Starts with a digit
    - Is empty

    Args:
        name: Column name to check

    Returns:
        True if the name contains characters that require sanitization

    Examples:
        >>> needs_sanitization("Customer Code")
        True
        >>> needs_sanitization(" Price ")
        True
        >>> needs_sanitization("normal_col")
        False
        >>> needs_sanitization("123start")
        True
    """
    if not name:
        return True

    # Check for leading/trailing whitespace
    if name != name.strip():
        return True

    # Check for non-alphanumeric characters (except underscore)
    if re.search(r"[^a-zA-Z0-9_]", name):
        return True

    # Check if starts with digit
    if name[0].isdigit():
        return True

    return False


def sanitize_dataframe_columns(
    df: Any, adapter_type: Optional[str] = None
) -> Tuple[Any, Dict[str, str]]:
    """Sanitize all column names in a Spark DataFrame.

    Applies sanitization to all columns that need it, preserving
    columns that are already valid SQL identifiers.

    Args:
        df: PySpark DataFrame with potentially problematic column names
        adapter_type: Target database adapter type (for future dialect-specific handling)

    Returns:
        Tuple of:
        - DataFrame with sanitized column names
        - Dict mapping original names to new names (only for changed columns)

    Example:
        >>> df_clean, renames = sanitize_dataframe_columns(df, "databricks")
        >>> print(renames)
        {'Customer Code': 'Customer_Code', ' Price ': 'Price'}
    """
    renames: Dict[str, str] = {}
    new_names: List[str] = []

    for col in df.columns:
        if needs_sanitization(col):
            new_name = sanitize_column_name(col)
            renames[col] = new_name
            new_names.append(new_name)
        else:
            new_names.append(col)

    # Handle duplicate column names after sanitization
    seen: Dict[str, int] = {}
    final_names: List[str] = []

    for name in new_names:
        if name in seen:
            # Add suffix to make unique
            seen[name] += 1
            unique_name = f"{name}_{seen[name]}"
            final_names.append(unique_name)
        else:
            seen[name] = 0
            final_names.append(name)

    # Apply renames to DataFrame
    if renames:
        df = df.toDF(*final_names)

    return df, renames


def quote_identifier(name: str, adapter_type: str) -> str:
    """Quote an identifier for a specific SQL dialect using SQLGlot.

    Uses SQLGlot to generate properly quoted identifiers for the target
    database dialect. This is useful when you need to preserve the original
    column name with special characters.

    Args:
        name: Identifier name to quote
        adapter_type: Target database adapter type (e.g., "postgres", "databricks")

    Returns:
        Quoted identifier string (e.g., '"Customer Code"' for Postgres,
        '`Customer Code`' for Databricks)

    Raises:
        ImportError: If SQLGlot is not available

    Example:
        >>> quote_identifier("Customer Code", "postgres")
        '"Customer Code"'
        >>> quote_identifier("Customer Code", "databricks")
        '`Customer Code`'
    """
    if not SQLGLOT_AVAILABLE:
        raise ImportError(
            "SQLGlot is required for identifier quoting. "
            "Install it with: pip install sqlglot"
        )

    from sqlglot import exp as sqlglot_exp

    # Get SQLGlot dialect name
    sqlglot_dialect = ADAPTER_TO_SQLGLOT.get(adapter_type.lower(), adapter_type.lower())

    # Create quoted identifier and generate SQL
    ident = sqlglot_exp.to_identifier(name, quoted=True)
    return ident.sql(dialect=sqlglot_dialect)


def get_sqlglot_dialect(adapter_type: str) -> str:
    """Get the SQLGlot dialect name for a DVT adapter type.

    Args:
        adapter_type: DVT adapter type (e.g., "postgres", "databricks")

    Returns:
        SQLGlot dialect name

    Example:
        >>> get_sqlglot_dialect("sqlserver")
        'tsql'
    """
    return ADAPTER_TO_SQLGLOT.get(adapter_type.lower(), adapter_type.lower())


def spark_type_to_sql_type(spark_type: Any, adapter_type: str) -> str:
    """Convert a Spark DataType to SQL type string for a specific dialect.

    Uses SQLGlot to generate dialect-appropriate SQL type names.
    For example, DOUBLE becomes DOUBLE PRECISION in Postgres.

    Args:
        spark_type: PySpark DataType instance (e.g., StringType(), IntegerType())
        adapter_type: Target database adapter (e.g., "databricks", "postgres")

    Returns:
        SQL type string appropriate for the target dialect

    Examples:
        >>> from pyspark.sql.types import DoubleType, StringType
        >>> spark_type_to_sql_type(DoubleType(), "postgres")
        'DOUBLE PRECISION'
        >>> spark_type_to_sql_type(DoubleType(), "databricks")
        'DOUBLE'
        >>> spark_type_to_sql_type(StringType(), "snowflake")
        'VARCHAR(65535)'
    """
    if not SQLGLOT_AVAILABLE:
        raise ImportError(
            "SQLGlot is required for SQL type conversion. "
            "Install it with: pip install sqlglot"
        )

    from sqlglot import exp as sqlglot_exp

    # Get the Spark type name
    type_name = spark_type.typeName()

    # Map Spark type names to SQL type names
    # Using VARCHAR(65535) for strings to handle large text fields
    sql_type_map = {
        "string": "VARCHAR(65535)",
        "integer": "INTEGER",
        "long": "BIGINT",
        "double": "DOUBLE",
        "float": "FLOAT",
        "boolean": "BOOLEAN",
        "date": "DATE",
        "timestamp": "TIMESTAMP",
        "short": "SMALLINT",
        "byte": "TINYINT",
        "binary": "VARBINARY(65535)",
    }

    # Handle decimal with precision/scale
    if type_name == "decimal":
        precision = getattr(spark_type, "precision", 10)
        scale = getattr(spark_type, "scale", 0)
        sql_type = f"DECIMAL({precision},{scale})"
    else:
        sql_type = sql_type_map.get(type_name, "VARCHAR(65535)")

    # Use SQLGlot to get dialect-specific type representation
    sqlglot_dialect = get_sqlglot_dialect(adapter_type)
    data_type = sqlglot_exp.DataType.build(sql_type)
    return data_type.sql(dialect=sqlglot_dialect)


def spark_type_to_jdbc_type(spark_type: Any) -> str:
    """Convert a Spark DataType to JDBC-compatible SQL type string.

    DEPRECATED: This function is no longer used by the loader system.
    Loaders now let Spark infer types directly, and DDL is handled
    via dbt adapters with proper quoting.

    IMPORTANT: Spark's JDBC createTableColumnTypes option expects
    Spark SQL types, NOT database-specific types. For example:
    - Use DOUBLE, not DOUBLE PRECISION
    - Use STRING or VARCHAR, not TEXT

    Args:
        spark_type: PySpark DataType instance (e.g., StringType(), IntegerType())

    Returns:
        SQL type string compatible with Spark JDBC

    Examples:
        >>> from pyspark.sql.types import DoubleType, StringType
        >>> spark_type_to_jdbc_type(DoubleType())
        'DOUBLE'
        >>> spark_type_to_jdbc_type(StringType())
        'VARCHAR(65535)'
    """
    import warnings

    warnings.warn(
        "spark_type_to_jdbc_type is deprecated. "
        "Loaders now use adapter-managed DDL and let Spark infer types.",
        DeprecationWarning,
        stacklevel=2,
    )
    # Get the Spark type name
    type_name = spark_type.typeName()

    # Map Spark type names to JDBC-compatible SQL types
    # These are Spark SQL types that the JDBC driver understands
    jdbc_type_map = {
        "string": "VARCHAR(65535)",
        "integer": "INT",
        "long": "BIGINT",
        "double": "DOUBLE",  # NOT "DOUBLE PRECISION"
        "float": "FLOAT",
        "boolean": "BOOLEAN",
        "date": "DATE",
        "timestamp": "TIMESTAMP",
        "short": "SMALLINT",
        "byte": "TINYINT",
        "binary": "BINARY",
    }

    # Handle decimal with precision/scale
    if type_name == "decimal":
        precision = getattr(spark_type, "precision", 10)
        scale = getattr(spark_type, "scale", 0)
        return f"DECIMAL({precision},{scale})"

    return jdbc_type_map.get(type_name, "VARCHAR(65535)")


def build_create_table_column_types(df: Any, adapter_type: str) -> str:
    """Build the JDBC createTableColumnTypes option value.

    DEPRECATED: This function is no longer used by the loader system.
    Loaders now use dbt adapters for DDL operations with proper quoting,
    and let Spark infer column types automatically.

    Generates a comma-separated list of column definitions with:
    - Dialect-appropriate quoted column names (via SQLGlot)
    - Spark SQL types (NOT database-specific types)

    Uses SQLGlot to quote identifiers correctly for each dialect:
    - PostgreSQL: "column_name"
    - Databricks/Spark: `column_name`
    - MySQL: `column_name`
    - SQL Server: [column_name]

    Args:
        df: PySpark DataFrame with the schema to convert
        adapter_type: Target database adapter for proper quoting

    Returns:
        String suitable for JDBC createTableColumnTypes option

    Examples:
        >>> # For a DataFrame with columns ['Customer_Code', 'Price', 'Quantity']
        >>> build_create_table_column_types(df, "databricks")
        '`Customer_Code` VARCHAR(65535), `Price` DOUBLE, `Quantity` BIGINT'
        >>> build_create_table_column_types(df, "postgres")
        '"Customer_Code" VARCHAR(65535), "Price" DOUBLE, "Quantity" BIGINT'
    """
    import warnings

    warnings.warn(
        "build_create_table_column_types is deprecated. "
        "Loaders now use adapter-managed DDL and let Spark infer types.",
        DeprecationWarning,
        stacklevel=2,
    )
    col_defs = []

    for field in df.schema.fields:
        # Use SQLGlot to quote identifier for the target dialect
        col_name = quote_identifier(field.name, adapter_type)

        # Get Spark JDBC-compatible SQL type (NOT dialect-specific)
        sql_type = spark_type_to_jdbc_type(field.dataType)

        col_defs.append(f"{col_name} {sql_type}")

    return ", ".join(col_defs)
