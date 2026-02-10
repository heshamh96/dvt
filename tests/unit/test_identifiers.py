# coding=utf-8
"""Unit tests for dvt.utils.identifiers module.

Tests the column name sanitization utilities that ensure SQL compatibility
across all database adapters (Postgres, Snowflake, Databricks, etc.).
"""

import pytest

from dvt.utils.identifiers import (
    ADAPTER_TO_SQLGLOT,
    build_create_table_column_types,
    get_sqlglot_dialect,
    needs_sanitization,
    quote_identifier,
    sanitize_column_name,
    spark_type_to_sql_type,
)


class TestSanitizeColumnName:
    """Tests for sanitize_column_name function."""

    def test_simple_name_unchanged(self):
        """Simple alphanumeric names should remain unchanged."""
        assert sanitize_column_name("customer_id") == "customer_id"
        assert sanitize_column_name("Name") == "Name"
        assert sanitize_column_name("COLUMN_NAME") == "COLUMN_NAME"

    def test_spaces_replaced_with_underscores(self):
        """Spaces should be replaced with underscores."""
        assert sanitize_column_name("Customer Code") == "Customer_Code"
        assert sanitize_column_name("First Name") == "First_Name"
        assert sanitize_column_name("A B C") == "A_B_C"

    def test_leading_trailing_whitespace_stripped(self):
        """Leading and trailing whitespace should be stripped."""
        assert sanitize_column_name(" Price ") == "Price"
        assert sanitize_column_name("  Total Cost  ") == "Total_Cost"
        assert sanitize_column_name("\tColumn\t") == "Column"

    def test_special_characters_replaced(self):
        """Special characters should be replaced with underscores."""
        assert sanitize_column_name("price($)") == "price"
        assert sanitize_column_name("amount-total") == "amount_total"
        assert sanitize_column_name("user@email") == "user_email"
        assert sanitize_column_name("col.name") == "col_name"

    def test_multiple_underscores_collapsed(self):
        """Multiple consecutive underscores should be collapsed to one."""
        assert sanitize_column_name("a  b") == "a_b"
        assert sanitize_column_name("a___b") == "a_b"
        assert sanitize_column_name("a - b") == "a_b"

    def test_leading_digit_prefixed(self):
        """Names starting with a digit should be prefixed with underscore."""
        assert sanitize_column_name("123column") == "_123column"
        assert sanitize_column_name("1st_place") == "_1st_place"
        assert sanitize_column_name("2023_data") == "_2023_data"

    def test_empty_string_returns_default(self):
        """Empty or whitespace-only strings should return default name."""
        assert sanitize_column_name("") == "_column"
        assert sanitize_column_name("   ") == "_column"
        assert sanitize_column_name("\t\n") == "_column"

    def test_only_special_chars_returns_default(self):
        """Strings with only special characters should return default name."""
        assert sanitize_column_name("@#$%") == "_column"
        assert sanitize_column_name("...") == "_column"
        assert sanitize_column_name("   -   ") == "_column"

    def test_preserves_case(self):
        """Original case should be preserved."""
        assert sanitize_column_name("CustomerName") == "CustomerName"
        assert sanitize_column_name("UPPER CASE") == "UPPER_CASE"
        assert sanitize_column_name("lower case") == "lower_case"

    def test_unicode_characters_replaced(self):
        """Unicode/non-ASCII characters should be replaced."""
        assert sanitize_column_name("café") == "caf"
        assert sanitize_column_name("über") == "ber"
        assert sanitize_column_name("日本語") == "_column"  # All non-ASCII

    def test_real_world_examples(self):
        """Test with real column names from the failing seed files."""
        # From customers_db_1.csv
        assert sanitize_column_name("Customer Code") == "Customer_Code"
        assert sanitize_column_name("Customer name") == "Customer_name"
        assert sanitize_column_name("Customer Channel type") == "Customer_Channel_type"
        assert (
            sanitize_column_name(" Customer coordinates X ") == "Customer_coordinates_X"
        )
        assert (
            sanitize_column_name(" Customer coordinates y ") == "Customer_coordinates_y"
        )
        assert sanitize_column_name("Number of Doors") == "Number_of_Doors"
        assert sanitize_column_name("Rep name") == "Rep_name"

        # From dim_files.csv
        assert sanitize_column_name(" file_name") == "file_name"

        # From packs.csv
        assert sanitize_column_name("SKU Code") == "SKU_Code"
        assert sanitize_column_name(" Price ") == "Price"
        assert sanitize_column_name(" Total Cost ") == "Total_Cost"


class TestNeedsSanitization:
    """Tests for needs_sanitization function."""

    def test_clean_names_dont_need_sanitization(self):
        """Clean SQL-safe names should not need sanitization."""
        assert needs_sanitization("customer_id") is False
        assert needs_sanitization("Name") is False
        assert needs_sanitization("column_123") is False
        assert needs_sanitization("_private") is False

    def test_names_with_spaces_need_sanitization(self):
        """Names with spaces should need sanitization."""
        assert needs_sanitization("Customer Code") is True
        assert needs_sanitization("First Name") is True

    def test_names_with_leading_trailing_whitespace_need_sanitization(self):
        """Names with leading/trailing whitespace should need sanitization."""
        assert needs_sanitization(" Price") is True
        assert needs_sanitization("Price ") is True
        assert needs_sanitization(" Price ") is True

    def test_names_with_special_chars_need_sanitization(self):
        """Names with special characters should need sanitization."""
        assert needs_sanitization("price($)") is True
        assert needs_sanitization("col-name") is True
        assert needs_sanitization("user@email") is True

    def test_names_starting_with_digit_need_sanitization(self):
        """Names starting with a digit should need sanitization."""
        assert needs_sanitization("123column") is True
        assert needs_sanitization("1st") is True

    def test_empty_string_needs_sanitization(self):
        """Empty string should need sanitization."""
        assert needs_sanitization("") is True


class TestQuoteIdentifier:
    """Tests for quote_identifier function using SQLGlot."""

    def test_postgres_double_quotes(self):
        """Postgres should use double quotes."""
        assert quote_identifier("Customer Code", "postgres") == '"Customer Code"'
        assert quote_identifier("normal", "postgres") == '"normal"'

    def test_databricks_backticks(self):
        """Databricks should use backticks."""
        assert quote_identifier("Customer Code", "databricks") == "`Customer Code`"
        assert quote_identifier("normal", "databricks") == "`normal`"

    def test_snowflake_double_quotes(self):
        """Snowflake should use double quotes."""
        assert quote_identifier("Customer Code", "snowflake") == '"Customer Code"'

    def test_mysql_backticks(self):
        """MySQL should use backticks."""
        assert quote_identifier("Customer Code", "mysql") == "`Customer Code`"

    def test_bigquery_backticks(self):
        """BigQuery should use backticks."""
        assert quote_identifier("Customer Code", "bigquery") == "`Customer Code`"

    def test_spark_backticks(self):
        """Spark should use backticks."""
        assert quote_identifier("Customer Code", "spark") == "`Customer Code`"


class TestGetSqlglotDialect:
    """Tests for get_sqlglot_dialect function."""

    def test_known_adapters(self):
        """Known adapter types should return correct SQLGlot dialect."""
        assert get_sqlglot_dialect("postgres") == "postgres"
        assert get_sqlglot_dialect("snowflake") == "snowflake"
        assert get_sqlglot_dialect("databricks") == "databricks"
        assert get_sqlglot_dialect("sqlserver") == "tsql"

    def test_case_insensitive(self):
        """Adapter type lookup should be case-insensitive."""
        assert get_sqlglot_dialect("POSTGRES") == "postgres"
        assert get_sqlglot_dialect("Snowflake") == "snowflake"

    def test_unknown_adapter_returns_as_is(self):
        """Unknown adapter types should be returned as-is."""
        assert get_sqlglot_dialect("unknown_db") == "unknown_db"


class TestAdapterToSqlglotMapping:
    """Tests for the ADAPTER_TO_SQLGLOT mapping."""

    def test_common_adapters_mapped(self):
        """Common adapter types should be in the mapping."""
        expected_adapters = [
            "postgres",
            "snowflake",
            "mysql",
            "redshift",
            "bigquery",
            "databricks",
            "spark",
            "trino",
            "duckdb",
            "oracle",
            "sqlserver",
        ]
        for adapter in expected_adapters:
            assert adapter in ADAPTER_TO_SQLGLOT, f"{adapter} should be in mapping"

    def test_sqlserver_maps_to_tsql(self):
        """SQL Server should map to 'tsql' for SQLGlot."""
        assert ADAPTER_TO_SQLGLOT["sqlserver"] == "tsql"


class TestSparkTypeToSqlType:
    """Tests for spark_type_to_sql_type function."""

    def test_string_type_returns_varchar(self):
        """StringType should return VARCHAR."""
        from pyspark.sql.types import StringType

        result = spark_type_to_sql_type(StringType(), "postgres")
        assert "VARCHAR" in result

    def test_integer_type(self):
        """IntegerType should return INTEGER."""
        from pyspark.sql.types import IntegerType

        result = spark_type_to_sql_type(IntegerType(), "postgres")
        assert result == "INT"

    def test_long_type_returns_bigint(self):
        """LongType should return BIGINT."""
        from pyspark.sql.types import LongType

        result = spark_type_to_sql_type(LongType(), "databricks")
        assert result == "BIGINT"

    def test_double_postgres_uses_double_precision(self):
        """Postgres should use DOUBLE PRECISION for DoubleType."""
        from pyspark.sql.types import DoubleType

        result = spark_type_to_sql_type(DoubleType(), "postgres")
        assert result == "DOUBLE PRECISION"

    def test_double_databricks_uses_double(self):
        """Databricks should use DOUBLE for DoubleType."""
        from pyspark.sql.types import DoubleType

        result = spark_type_to_sql_type(DoubleType(), "databricks")
        assert result == "DOUBLE"

    def test_boolean_type(self):
        """BooleanType should return BOOLEAN."""
        from pyspark.sql.types import BooleanType

        result = spark_type_to_sql_type(BooleanType(), "snowflake")
        assert result == "BOOLEAN"

    def test_date_type(self):
        """DateType should return DATE."""
        from pyspark.sql.types import DateType

        result = spark_type_to_sql_type(DateType(), "postgres")
        assert result == "DATE"

    def test_timestamp_type(self):
        """TimestampType should return TIMESTAMP."""
        from pyspark.sql.types import TimestampType

        result = spark_type_to_sql_type(TimestampType(), "databricks")
        assert result == "TIMESTAMP"

    def test_decimal_type_with_precision_scale(self):
        """DecimalType should include precision and scale."""
        from pyspark.sql.types import DecimalType

        result = spark_type_to_sql_type(DecimalType(10, 2), "postgres")
        assert "DECIMAL" in result or "NUMERIC" in result
        assert "10" in result
        assert "2" in result


class TestBuildCreateTableColumnTypes:
    """Tests for build_create_table_column_types function."""

    def test_generates_unquoted_column_defs_databricks(self):
        """Should generate unquoted columns for Spark JDBC (Databricks handles quoting)."""
        from pyspark.sql import SparkSession

        spark = SparkSession.builder.appName("test").master("local[1]").getOrCreate()
        try:
            df = spark.createDataFrame([("Alice", 100)], ["Name", "Age"])
            result = build_create_table_column_types(df, "databricks")

            # Spark JDBC expects unquoted column names - it handles quoting internally
            assert "Name" in result
            assert "Age" in result
            # Should NOT have dialect-specific quotes
            assert "`Name`" not in result
            assert "`Age`" not in result
            # Should have types
            assert "VARCHAR" in result or "STRING" in result
            assert "BIGINT" in result
        finally:
            spark.stop()

    def test_generates_unquoted_column_defs_postgres(self):
        """Should generate unquoted columns for Spark JDBC (Postgres handles quoting)."""
        from pyspark.sql import SparkSession

        spark = SparkSession.builder.appName("test").master("local[1]").getOrCreate()
        try:
            df = spark.createDataFrame([("Alice", 100)], ["Name", "Age"])
            result = build_create_table_column_types(df, "postgres")

            # Spark JDBC expects unquoted column names
            assert "Name" in result
            assert "Age" in result
            # Should NOT have double quotes
            assert '"Name"' not in result
            assert '"Age"' not in result
        finally:
            spark.stop()

    def test_handles_sanitized_column_names(self):
        """Should work with already-sanitized column names (unquoted)."""
        from pyspark.sql import SparkSession

        spark = SparkSession.builder.appName("test").master("local[1]").getOrCreate()
        try:
            # Use sanitized names (underscores instead of spaces)
            df = spark.createDataFrame(
                [("Alice", 100, 3.14)], ["Customer_Name", "Total_Quantity", "Price"]
            )
            result = build_create_table_column_types(df, "databricks")

            # Column names should be present (unquoted)
            assert "Customer_Name" in result
            assert "Total_Quantity" in result
            assert "Price" in result
            # Should NOT be quoted
            assert "`Customer_Name`" not in result
        finally:
            spark.stop()

    def test_comma_separated_format(self):
        """Result should be comma-separated column definitions."""
        from pyspark.sql import SparkSession

        spark = SparkSession.builder.appName("test").master("local[1]").getOrCreate()
        try:
            df = spark.createDataFrame([("A", 1, 2.0)], ["col1", "col2", "col3"])
            result = build_create_table_column_types(df, "postgres")

            # Should have commas separating definitions
            assert result.count(",") == 2  # 3 columns = 2 commas
        finally:
            spark.stop()
