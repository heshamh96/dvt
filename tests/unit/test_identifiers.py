# coding=utf-8
"""Unit tests for dvt.utils.identifiers module.

Tests the identifier quoting and DDL generation utilities that preserve
original column names across all database adapters via dialect-aware quoting.
"""

import pytest

from dvt.utils.identifiers import (
    ADAPTER_TO_SQLGLOT,
    build_create_table_column_types,
    build_create_table_sql,
    get_sqlglot_dialect,
    needs_column_mapping,
    quote_identifier,
    spark_type_to_sql_type,
)


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

    def test_generates_quoted_column_defs_databricks(self):
        """Should generate quoted columns for Databricks (backtick quoting)."""
        from pyspark.sql import SparkSession

        spark = SparkSession.builder.appName("test").master("local[1]").getOrCreate()
        try:
            df = spark.createDataFrame([("Alice", 100)], ["Name", "Age"])
            result = build_create_table_column_types(df, "databricks")

            assert "`Name`" in result
            assert "`Age`" in result
            assert "VARCHAR" in result or "STRING" in result
            assert "BIGINT" in result
        finally:
            spark.stop()

    def test_generates_quoted_column_defs_postgres(self):
        """Should generate quoted columns for Postgres (double-quote quoting)."""
        from pyspark.sql import SparkSession

        spark = SparkSession.builder.appName("test").master("local[1]").getOrCreate()
        try:
            df = spark.createDataFrame([("Alice", 100)], ["Name", "Age"])
            result = build_create_table_column_types(df, "postgres")

            assert '"Name"' in result
            assert '"Age"' in result
        finally:
            spark.stop()

    def test_handles_column_names_with_spaces(self):
        """Should properly quote column names that contain spaces."""
        from pyspark.sql import SparkSession

        spark = SparkSession.builder.appName("test").master("local[1]").getOrCreate()
        try:
            df = spark.createDataFrame(
                [("Alice", 100, 3.14)], ["Customer Name", "Total Quantity", "Price"]
            )
            result = build_create_table_column_types(df, "databricks")

            assert "`Customer Name`" in result
            assert "`Total Quantity`" in result
            assert "`Price`" in result
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


class TestBuildCreateTableSql:
    """Tests for build_create_table_sql function.

    Verifies that CREATE TABLE DDL is generated with properly quoted
    column names that preserve original names (spaces, special chars).
    """

    def test_postgres_simple_columns(self):
        """Postgres CREATE TABLE with simple column names."""
        from pyspark.sql import SparkSession

        spark = SparkSession.builder.appName("test").master("local[1]").getOrCreate()
        try:
            df = spark.createDataFrame([("Alice", 30)], ["name", "age"])
            result = build_create_table_sql(df, "postgres", '"public"."users"')

            assert 'CREATE TABLE IF NOT EXISTS "public"."users"' in result
            assert '"name"' in result
            assert '"age"' in result
        finally:
            spark.stop()

    def test_postgres_columns_with_spaces(self):
        """Postgres CREATE TABLE preserves column names with spaces via quoting."""
        from pyspark.sql import SparkSession

        spark = SparkSession.builder.appName("test").master("local[1]").getOrCreate()
        try:
            df = spark.createDataFrame(
                [("CUST001", "Acme Corp")], ["Customer Code", "Customer Name"]
            )
            result = build_create_table_sql(df, "postgres", '"public"."customers"')

            assert '"Customer Code"' in result
            assert '"Customer Name"' in result
            assert "VARCHAR" in result
        finally:
            spark.stop()

    def test_databricks_columns_with_spaces(self):
        """Databricks CREATE TABLE uses backtick quoting for column names."""
        from pyspark.sql import SparkSession

        spark = SparkSession.builder.appName("test").master("local[1]").getOrCreate()
        try:
            df = spark.createDataFrame(
                [("CUST001", 100)], ["Customer Code", "Total Amount"]
            )
            result = build_create_table_sql(
                df, "databricks", "`catalog`.`schema`.`table`"
            )

            assert "`Customer Code`" in result
            assert "`Total Amount`" in result
            assert "CREATE TABLE IF NOT EXISTS" in result
        finally:
            spark.stop()

    def test_mixed_column_types(self):
        """CREATE TABLE should generate correct types for each column."""
        from pyspark.sql import SparkSession
        from pyspark.sql.types import (
            DoubleType,
            LongType,
            StringType,
            StructField,
            StructType,
        )

        spark = SparkSession.builder.appName("test").master("local[1]").getOrCreate()
        try:
            schema = StructType(
                [
                    StructField("Product Name", StringType(), True),
                    StructField("Unit Price", DoubleType(), True),
                    StructField("Quantity", LongType(), True),
                ]
            )
            df = spark.createDataFrame([], schema)
            result = build_create_table_sql(df, "postgres", '"public"."products"')

            assert '"Product Name"' in result
            assert '"Unit Price"' in result
            assert '"Quantity"' in result
            assert "VARCHAR" in result
            assert "DOUBLE PRECISION" in result
            assert "BIGINT" in result
        finally:
            spark.stop()

    def test_contains_if_not_exists(self):
        """Generated SQL should use IF NOT EXISTS."""
        from pyspark.sql import SparkSession

        spark = SparkSession.builder.appName("test").master("local[1]").getOrCreate()
        try:
            df = spark.createDataFrame([(1,)], ["id"])
            result = build_create_table_sql(df, "postgres", '"public"."test"')

            assert "IF NOT EXISTS" in result
        finally:
            spark.stop()

    def test_databricks_includes_tblproperties(self):
        """Databricks CREATE TABLE should include Delta Column Mapping TBLPROPERTIES."""
        from pyspark.sql import SparkSession

        spark = SparkSession.builder.appName("test").master("local[1]").getOrCreate()
        try:
            df = spark.createDataFrame(
                [("CUST001", 100)], ["Customer Code", "Total Amount"]
            )
            result = build_create_table_sql(
                df, "databricks", "`catalog`.`schema`.`table`"
            )

            assert "TBLPROPERTIES" in result
            assert "'delta.columnMapping.mode' = 'name'" in result
            assert "'delta.minReaderVersion' = '2'" in result
            assert "'delta.minWriterVersion' = '5'" in result
        finally:
            spark.stop()

    def test_spark_includes_tblproperties_with_special_columns(self):
        """Spark CREATE TABLE should include TBLPROPERTIES when columns have spaces."""
        from pyspark.sql import SparkSession

        spark = SparkSession.builder.appName("test").master("local[1]").getOrCreate()
        try:
            df = spark.createDataFrame([("Alice", 30)], ["First Name", "User Age"])
            result = build_create_table_sql(df, "spark", "`schema`.`table`")

            assert "TBLPROPERTIES" in result
            assert "'delta.columnMapping.mode' = 'name'" in result
        finally:
            spark.stop()

    def test_spark_no_tblproperties_with_simple_columns(self):
        """Spark CREATE TABLE should NOT include TBLPROPERTIES for simple column names.

        Delta Column Mapping TBLPROPERTIES cause an incompatibility with Spark's
        JDBC writer: the JDBC driver cannot resolve column-mapped physical names.
        For tables with simple names (no spaces/special chars), column mapping
        is unnecessary and skipped.
        """
        from pyspark.sql import SparkSession

        spark = SparkSession.builder.appName("test").master("local[1]").getOrCreate()
        try:
            df = spark.createDataFrame([("Alice", 30)], ["name", "age"])
            result = build_create_table_sql(df, "spark", "`schema`.`table`")

            assert "TBLPROPERTIES" not in result
        finally:
            spark.stop()

    def test_databricks_no_tblproperties_simple_columns(self):
        """Databricks CREATE TABLE should NOT include TBLPROPERTIES for simple names."""
        from pyspark.sql import SparkSession

        spark = SparkSession.builder.appName("test").master("local[1]").getOrCreate()
        try:
            df = spark.createDataFrame(
                [("Alice", 30)], ["customer_name", "customer_age"]
            )
            result = build_create_table_sql(
                df, "databricks", "`catalog`.`schema`.`table`"
            )

            assert "TBLPROPERTIES" not in result
            assert "CREATE TABLE IF NOT EXISTS" in result
            assert "`customer_name`" in result
        finally:
            spark.stop()

    def test_postgres_no_tblproperties(self):
        """Postgres CREATE TABLE should NOT include TBLPROPERTIES."""
        from pyspark.sql import SparkSession

        spark = SparkSession.builder.appName("test").master("local[1]").getOrCreate()
        try:
            df = spark.createDataFrame([("Alice", 30)], ["name", "age"])
            result = build_create_table_sql(df, "postgres", '"public"."test"')

            assert "TBLPROPERTIES" not in result
        finally:
            spark.stop()

    def test_snowflake_no_tblproperties(self):
        """Snowflake CREATE TABLE should NOT include TBLPROPERTIES."""
        from pyspark.sql import SparkSession

        spark = SparkSession.builder.appName("test").master("local[1]").getOrCreate()
        try:
            df = spark.createDataFrame([("Alice", 30)], ["name", "age"])
            result = build_create_table_sql(df, "snowflake", '"schema"."test"')

            assert "TBLPROPERTIES" not in result
        finally:
            spark.stop()

    def test_databricks_mixed_columns_gets_tblproperties(self):
        """Databricks: if ANY column has special chars, ALL get TBLPROPERTIES."""
        from pyspark.sql import SparkSession

        spark = SparkSession.builder.appName("test").master("local[1]").getOrCreate()
        try:
            # One column with space, one without
            df = spark.createDataFrame(
                [("CUST001", 100)], ["Customer Code", "quantity"]
            )
            result = build_create_table_sql(
                df, "databricks", "`catalog`.`schema`.`table`"
            )

            # One column has a space -> TBLPROPERTIES should be added
            assert "TBLPROPERTIES" in result
            assert "'delta.columnMapping.mode' = 'name'" in result
        finally:
            spark.stop()


class TestNeedsColumnMapping:
    """Tests for needs_column_mapping function."""

    def test_simple_names_no_mapping(self):
        """Simple alphanumeric+underscore names don't need column mapping."""
        from pyspark.sql import SparkSession

        spark = SparkSession.builder.appName("test").master("local[1]").getOrCreate()
        try:
            df = spark.createDataFrame([(1, "a")], ["id", "name"])
            assert needs_column_mapping(df) is False
        finally:
            spark.stop()

    def test_leading_underscore_no_mapping(self):
        """Leading underscore is valid — no mapping needed."""
        from pyspark.sql import SparkSession

        spark = SparkSession.builder.appName("test").master("local[1]").getOrCreate()
        try:
            df = spark.createDataFrame([(1,)], ["_internal_id"])
            assert needs_column_mapping(df) is False
        finally:
            spark.stop()

    def test_spaces_need_mapping(self):
        """Columns with spaces need column mapping."""
        from pyspark.sql import SparkSession

        spark = SparkSession.builder.appName("test").master("local[1]").getOrCreate()
        try:
            df = spark.createDataFrame([("a",)], ["Customer Code"])
            assert needs_column_mapping(df) is True
        finally:
            spark.stop()

    def test_hyphens_need_mapping(self):
        """Columns with hyphens need column mapping."""
        from pyspark.sql import SparkSession

        spark = SparkSession.builder.appName("test").master("local[1]").getOrCreate()
        try:
            df = spark.createDataFrame([("a",)], ["customer-code"])
            assert needs_column_mapping(df) is True
        finally:
            spark.stop()

    def test_dots_need_mapping(self):
        """Columns with dots need column mapping."""
        from pyspark.sql import SparkSession

        spark = SparkSession.builder.appName("test").master("local[1]").getOrCreate()
        try:
            df = spark.createDataFrame([("a",)], ["first.name"])
            assert needs_column_mapping(df) is True
        finally:
            spark.stop()

    def test_leading_digit_needs_mapping(self):
        """Columns starting with a digit need column mapping."""
        from pyspark.sql import SparkSession

        spark = SparkSession.builder.appName("test").master("local[1]").getOrCreate()
        try:
            df = spark.createDataFrame([("a",)], ["1st_column"])
            assert needs_column_mapping(df) is True
        finally:
            spark.stop()

    def test_mixed_one_bad_triggers(self):
        """If ANY column needs mapping, function returns True."""
        from pyspark.sql import SparkSession

        spark = SparkSession.builder.appName("test").master("local[1]").getOrCreate()
        try:
            df = spark.createDataFrame(
                [("a", 1, 2)], ["normal_col", "Customer Code", "quantity"]
            )
            assert needs_column_mapping(df) is True
        finally:
            spark.stop()
