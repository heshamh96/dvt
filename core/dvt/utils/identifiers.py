# coding=utf-8
"""Identifier quoting and type conversion utilities using SQLGlot.

Provides cross-dialect identifier quoting and DDL generation for safe SQL
operations. Column names are always preserved as-is and quoted using
dialect-appropriate quote characters (double quotes, backticks, brackets).

Key features:
- Dialect-specific identifier quoting via SQLGlot
- Spark-to-SQL type conversion per dialect
- CREATE TABLE DDL generation from DataFrame schemas
- Preserves original column names (spaces, special chars) via quoting

Usage:
    from dvt.utils.identifiers import quote_identifier, build_create_table_sql

    # Quote a column name for a specific dialect
    quoted = quote_identifier("Customer Code", "postgres")  # '"Customer Code"'

    # Build CREATE TABLE from a Spark DataFrame schema
    sql = build_create_table_sql(df, "postgres", '"public"."my_table"')
"""

from typing import Any, Dict

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


def build_create_table_sql(df: Any, adapter_type: str, quoted_table_name: str) -> str:
    """Build a CREATE TABLE IF NOT EXISTS statement from a DataFrame schema.

    Uses dialect-aware quoting for column names and dialect-specific SQL types
    to generate a CREATE TABLE statement that preserves original column names
    (including those with spaces, special characters, etc.) by quoting them.

    Args:
        df: PySpark DataFrame whose schema defines the table structure
        adapter_type: Target database adapter type (e.g., "postgres", "databricks")
        quoted_table_name: Already-quoted table name (e.g., '"schema"."table"')

    Returns:
        Complete CREATE TABLE IF NOT EXISTS SQL statement

    Examples:
        >>> build_create_table_sql(df, "postgres", '"public"."my_seeds"')
        'CREATE TABLE IF NOT EXISTS "public"."my_seeds" (\\n  "Customer Code" ...'
    """
    col_defs = []

    for field in df.schema.fields:
        col_name = quote_identifier(field.name, adapter_type)
        sql_type = spark_type_to_sql_type(field.dataType, adapter_type)
        col_defs.append(f"{col_name} {sql_type}")

    columns_sql = ",\n  ".join(col_defs)
    return f"CREATE TABLE IF NOT EXISTS {quoted_table_name} (\n  {columns_sql}\n)"


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
