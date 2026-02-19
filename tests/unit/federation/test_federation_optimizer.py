"""
Tests for FederationOptimizer — the blackbox that decomposes Spark SQL
into per-source extraction plans with column projection, predicate pushdown,
LIMIT pushdown, and full dialect transpilation.
"""

import pytest

from dvt.federation.federation_optimizer import (
    ExtractionPlan,
    FederationOptimizer,
    SourceInfo,
)


# =============================================================================
# Fixtures
# =============================================================================


@pytest.fixture
def optimizer():
    return FederationOptimizer()


def _pg_source(source_id, view_name, schema, table, columns):
    return SourceInfo(
        source_id=source_id,
        view_name=view_name,
        schema=schema,
        table=table,
        columns=columns,
        dialect="postgres",
    )


def _dbx_source(source_id, view_name, schema, table, columns):
    return SourceInfo(
        source_id=source_id,
        view_name=view_name,
        schema=schema,
        table=table,
        columns=columns,
        dialect="databricks",
    )


def _sf_source(source_id, view_name, schema, table, columns):
    return SourceInfo(
        source_id=source_id,
        view_name=view_name,
        schema=schema,
        table=table,
        columns=columns,
        dialect="snowflake",
    )


# =============================================================================
# Column Projection Tests
# =============================================================================


class TestColumnProjection:
    """Test that only referenced columns are extracted per source."""

    def test_two_source_join_columns(self, optimizer):
        """Each source gets only its referenced columns."""
        spark_sql = (
            "SELECT c.`Customer Code`, c.`Customer name`, r.region_name "
            "FROM _dvt_c c "
            "LEFT JOIN _dvt_r r ON c.region_code = r.region_code"
        )
        source_info = {
            "src_c": _pg_source(
                "src_c",
                "_dvt_c",
                "public",
                "customers_db_1",
                ["Customer Code", "Customer name", "region_code", "Route", "Depot"],
            ),
            "src_r": _dbx_source(
                "src_r",
                "_dvt_r",
                "dvt_test",
                "dim_regions",
                ["region_code", "region_name", "country"],
            ),
        }
        plans = optimizer.optimize(spark_sql, source_info)

        # customers: Customer Code, Customer name, region_code (JOIN key)
        assert set(plans["src_c"].columns) == {
            "Customer Code",
            "Customer name",
            "region_code",
        }
        # Route and Depot should NOT be extracted
        assert "Route" not in plans["src_c"].columns
        assert "Depot" not in plans["src_c"].columns

        # regions: region_name (SELECT) + region_code (JOIN key)
        assert set(plans["src_r"].columns) == {"region_name", "region_code"}
        # country should NOT be extracted
        assert "country" not in plans["src_r"].columns

    def test_where_columns_included(self, optimizer):
        """Columns from WHERE clause are included in extraction."""
        spark_sql = (
            "SELECT c.`Customer Code` FROM _dvt_c c "
            "WHERE c.`Customer Channel type` = 'Modern Trade'"
        )
        source_info = {
            "src_c": _pg_source(
                "src_c",
                "_dvt_c",
                "public",
                "customers",
                ["Customer Code", "Customer Channel type", "Route"],
            ),
        }
        plans = optimizer.optimize(spark_sql, source_info)
        assert "Customer Channel type" in plans["src_c"].columns
        assert "Customer Code" in plans["src_c"].columns
        assert "Route" not in plans["src_c"].columns

    def test_order_by_columns_included(self, optimizer):
        """Columns from ORDER BY are included."""
        spark_sql = "SELECT c.name FROM _dvt_c c ORDER BY c.created_at"
        source_info = {
            "src_c": _pg_source(
                "src_c",
                "_dvt_c",
                "public",
                "customers",
                ["name", "created_at", "email"],
            ),
        }
        plans = optimizer.optimize(spark_sql, source_info)
        assert set(plans["src_c"].columns) == {"name", "created_at"}

    def test_group_by_having_columns(self, optimizer):
        """Columns from GROUP BY and HAVING are included."""
        spark_sql = (
            "SELECT s.region_code, COUNT(s.sale_id) as cnt "
            "FROM _dvt_s s "
            "GROUP BY s.region_code "
            "HAVING COUNT(s.sale_id) > 10"
        )
        source_info = {
            "src_s": _pg_source(
                "src_s",
                "_dvt_s",
                "public",
                "sales",
                ["sale_id", "region_code", "amount", "status"],
            ),
        }
        plans = optimizer.optimize(spark_sql, source_info)
        assert "region_code" in plans["src_s"].columns
        assert "sale_id" in plans["src_s"].columns
        # amount and status not referenced
        assert "amount" not in plans["src_s"].columns

    def test_window_function_columns(self, optimizer):
        """Columns from PARTITION BY and ORDER BY in window functions."""
        spark_sql = (
            "SELECT s.sale_id, "
            "ROW_NUMBER() OVER (PARTITION BY s.region_code ORDER BY s.sale_date DESC) as rn "
            "FROM _dvt_s s"
        )
        source_info = {
            "src_s": _pg_source(
                "src_s",
                "_dvt_s",
                "public",
                "sales",
                ["sale_id", "region_code", "sale_date", "amount"],
            ),
        }
        plans = optimizer.optimize(spark_sql, source_info)
        assert set(plans["src_s"].columns) == {"sale_id", "region_code", "sale_date"}

    def test_case_when_columns(self, optimizer):
        """Columns inside CASE WHEN expressions are included."""
        spark_sql = (
            "SELECT s.sale_id, "
            "CASE WHEN s.amount > 100 THEN 'high' ELSE 'low' END as tier "
            "FROM _dvt_s s"
        )
        source_info = {
            "src_s": _pg_source(
                "src_s",
                "_dvt_s",
                "public",
                "sales",
                ["sale_id", "amount", "status"],
            ),
        }
        plans = optimizer.optimize(spark_sql, source_info)
        assert set(plans["src_s"].columns) == {"sale_id", "amount"}

    def test_three_source_join(self, optimizer):
        """Three-source JOIN — each source gets only its columns."""
        spark_sql = (
            "SELECT c.name, r.region_name, p.product_name "
            "FROM _dvt_c c "
            "JOIN _dvt_r r ON c.region_code = r.region_code "
            "JOIN _dvt_p p ON c.product_id = p.id"
        )
        source_info = {
            "src_c": _pg_source(
                "src_c",
                "_dvt_c",
                "public",
                "customers",
                ["name", "region_code", "product_id", "email"],
            ),
            "src_r": _dbx_source(
                "src_r",
                "_dvt_r",
                "dvt_test",
                "regions",
                ["region_code", "region_name", "country"],
            ),
            "src_p": _sf_source(
                "src_p", "_dvt_p", "public", "products", ["id", "product_name", "price"]
            ),
        }
        plans = optimizer.optimize(spark_sql, source_info)
        assert set(plans["src_c"].columns) == {"name", "region_code", "product_id"}
        assert set(plans["src_r"].columns) == {"region_code", "region_name"}
        assert set(plans["src_p"].columns) == {"id", "product_name"}

    def test_select_star_with_schema(self, optimizer):
        """SELECT * expands to all columns when schema is available."""
        spark_sql = (
            "SELECT * FROM _dvt_c c LEFT JOIN _dvt_r r ON c.region_code = r.region_code"
        )
        source_info = {
            "src_c": _pg_source(
                "src_c", "_dvt_c", "public", "customers", ["id", "name", "region_code"]
            ),
            "src_r": _dbx_source(
                "src_r", "_dvt_r", "dvt_test", "regions", ["region_code", "region_name"]
            ),
        }
        plans = optimizer.optimize(spark_sql, source_info)
        # SELECT * → all columns from both sources
        assert set(plans["src_c"].columns) == {"id", "name", "region_code"}
        assert set(plans["src_r"].columns) == {"region_code", "region_name"}


# =============================================================================
# Predicate Pushdown Tests
# =============================================================================


class TestPredicatePushdown:
    """Test that WHERE predicates are correctly attributed and transpiled."""

    def test_single_source_predicate(self, optimizer):
        """Predicate on one source is pushed to that source only."""
        spark_sql = (
            "SELECT c.name, r.region_name FROM _dvt_c c "
            "LEFT JOIN _dvt_r r ON c.region_code = r.region_code "
            "WHERE c.status = 'active'"
        )
        source_info = {
            "src_c": _pg_source(
                "src_c",
                "_dvt_c",
                "public",
                "customers",
                ["name", "status", "region_code"],
            ),
            "src_r": _dbx_source(
                "src_r", "_dvt_r", "dvt_test", "regions", ["region_code", "region_name"]
            ),
        }
        plans = optimizer.optimize(spark_sql, source_info)
        assert len(plans["src_c"].predicates) == 1
        assert "active" in plans["src_c"].predicates[0]
        assert len(plans["src_r"].predicates) == 0

    def test_multiple_predicates_split(self, optimizer):
        """Multiple predicates split to correct sources."""
        spark_sql = (
            "SELECT c.name, r.region_name FROM _dvt_c c "
            "LEFT JOIN _dvt_r r ON c.region_code = r.region_code "
            "WHERE c.status = 'active' AND r.region_name LIKE 'North%' AND c.amount > 100"
        )
        source_info = {
            "src_c": _pg_source(
                "src_c",
                "_dvt_c",
                "public",
                "customers",
                ["name", "status", "region_code", "amount"],
            ),
            "src_r": _dbx_source(
                "src_r", "_dvt_r", "dvt_test", "regions", ["region_code", "region_name"]
            ),
        }
        plans = optimizer.optimize(spark_sql, source_info)
        assert len(plans["src_c"].predicates) == 2  # status + amount
        assert len(plans["src_r"].predicates) == 1  # region_name

    def test_cross_source_predicate_not_pushed(self, optimizer):
        """Cross-source predicate (WHERE s.id = r.id) is NOT pushed."""
        spark_sql = (
            "SELECT c.name, r.region_name FROM _dvt_c c "
            "LEFT JOIN _dvt_r r ON c.region_code = r.region_code "
            "WHERE c.region_code = r.region_code"
        )
        source_info = {
            "src_c": _pg_source(
                "src_c", "_dvt_c", "public", "customers", ["name", "region_code"]
            ),
            "src_r": _dbx_source(
                "src_r", "_dvt_r", "dvt_test", "regions", ["region_code", "region_name"]
            ),
        }
        plans = optimizer.optimize(spark_sql, source_info)
        # Cross-source predicate should NOT be pushed to either
        assert len(plans["src_c"].predicates) == 0
        assert len(plans["src_r"].predicates) == 0

    def test_no_where_clause(self, optimizer):
        """No WHERE → no predicates pushed."""
        spark_sql = "SELECT c.name FROM _dvt_c c"
        source_info = {
            "src_c": _pg_source("src_c", "_dvt_c", "public", "customers", ["name"]),
        }
        plans = optimizer.optimize(spark_sql, source_info)
        assert plans["src_c"].predicates == []


# =============================================================================
# Predicate Transpilation Tests
# =============================================================================


class TestPredicateTranspilation:
    """Test that predicates are transpiled to source dialect."""

    def test_predicate_in_pg_dialect(self, optimizer):
        """Predicate output uses PG double-quote syntax."""
        spark_sql = (
            "SELECT c.`Customer Code` FROM _dvt_c c "
            "WHERE c.`Customer Channel type` = 'Modern Trade'"
        )
        source_info = {
            "src_c": _pg_source(
                "src_c",
                "_dvt_c",
                "public",
                "customers",
                ["Customer Code", "Customer Channel type"],
            ),
        }
        plans = optimizer.optimize(spark_sql, source_info)
        pred = plans["src_c"].predicates[0]
        # PG uses double quotes for identifiers
        assert '"Customer Channel type"' in pred

    def test_predicate_in_dbx_dialect(self, optimizer):
        """Predicate output uses Databricks backtick syntax."""
        spark_sql = (
            "SELECT r.`region_name` FROM _dvt_r r WHERE r.`region_name` LIKE 'North%'"
        )
        source_info = {
            "src_r": _dbx_source(
                "src_r", "_dvt_r", "dvt_test", "regions", ["region_name"]
            ),
        }
        plans = optimizer.optimize(spark_sql, source_info)
        pred = plans["src_r"].predicates[0]
        # Databricks uses backticks
        assert "`region_name`" in pred or "region_name" in pred


# =============================================================================
# LIMIT Pushdown Tests
# =============================================================================


class TestLimitPushdown:
    """Test LIMIT pushdown behavior."""

    def test_single_source_limit_pushed(self, optimizer):
        """LIMIT is pushed when there's only one source."""
        spark_sql = "SELECT c.name FROM _dvt_c c LIMIT 50"
        source_info = {
            "src_c": _pg_source("src_c", "_dvt_c", "public", "customers", ["name"]),
        }
        plans = optimizer.optimize(spark_sql, source_info)
        assert plans["src_c"].limit == 50

    def test_multi_source_limit_not_pushed(self, optimizer):
        """LIMIT is NOT pushed when there are multiple sources (JOIN)."""
        spark_sql = (
            "SELECT c.name, r.region_name FROM _dvt_c c "
            "LEFT JOIN _dvt_r r ON c.region_code = r.region_code "
            "LIMIT 25"
        )
        source_info = {
            "src_c": _pg_source(
                "src_c", "_dvt_c", "public", "customers", ["name", "region_code"]
            ),
            "src_r": _dbx_source(
                "src_r", "_dvt_r", "dvt_test", "regions", ["region_code", "region_name"]
            ),
        }
        plans = optimizer.optimize(spark_sql, source_info)
        assert plans["src_c"].limit is None
        assert plans["src_r"].limit is None

    def test_no_limit(self, optimizer):
        """No LIMIT → limit is None."""
        spark_sql = "SELECT c.name FROM _dvt_c c"
        source_info = {
            "src_c": _pg_source("src_c", "_dvt_c", "public", "customers", ["name"]),
        }
        plans = optimizer.optimize(spark_sql, source_info)
        assert plans["src_c"].limit is None


# =============================================================================
# Extraction SQL Transpilation Tests
# =============================================================================


class TestExtractionSQLTranspilation:
    """Test that extraction_sql is properly transpiled per dialect."""

    def test_pg_extraction_sql(self, optimizer):
        """PostgreSQL extraction: double quotes, LIMIT syntax."""
        spark_sql = "SELECT c.`Customer Code` FROM _dvt_c c LIMIT 100"
        source_info = {
            "src_c": _pg_source(
                "src_c", "_dvt_c", "public", "customers", ["Customer Code"]
            ),
        }
        plans = optimizer.optimize(spark_sql, source_info)
        sql = plans["src_c"].extraction_sql
        assert '"Customer Code"' in sql  # PG double quotes
        assert "LIMIT 100" in sql  # PG LIMIT syntax
        assert '"public"' in sql  # Schema quoted

    def test_dbx_extraction_sql(self, optimizer):
        """Databricks extraction: backticks, LIMIT syntax."""
        spark_sql = "SELECT r.region_name FROM _dvt_r r LIMIT 100"
        source_info = {
            "src_r": _dbx_source(
                "src_r", "_dvt_r", "dvt_test", "regions", ["region_name"]
            ),
        }
        plans = optimizer.optimize(spark_sql, source_info)
        sql = plans["src_r"].extraction_sql
        assert "`region_name`" in sql  # DBX backticks
        assert "LIMIT 100" in sql  # DBX also uses LIMIT

    def test_sqlserver_extraction_sql(self, optimizer):
        """SQL Server extraction: brackets, TOP instead of LIMIT."""
        spark_sql = "SELECT c.`id`, c.`name` FROM _dvt_c c LIMIT 100"
        source_info = {
            "src_c": SourceInfo(
                "src_c", "_dvt_c", "dbo", "customers", ["id", "name"], "sqlserver"
            ),
        }
        plans = optimizer.optimize(spark_sql, source_info)
        sql = plans["src_c"].extraction_sql
        assert "TOP 100" in sql  # MSSQL uses TOP
        assert "[id]" in sql or "[name]" in sql  # MSSQL brackets

    def test_oracle_extraction_sql(self, optimizer):
        """Oracle extraction: FETCH FIRST instead of LIMIT."""
        spark_sql = "SELECT c.`id`, c.`name` FROM _dvt_c c LIMIT 100"
        source_info = {
            "src_c": SourceInfo(
                "src_c", "_dvt_c", "HR", "employees", ["id", "name"], "oracle"
            ),
        }
        plans = optimizer.optimize(spark_sql, source_info)
        sql = plans["src_c"].extraction_sql
        assert "FETCH FIRST 100 ROWS ONLY" in sql  # Oracle syntax

    def test_snowflake_extraction_sql(self, optimizer):
        """Snowflake extraction: double quotes, LIMIT syntax."""
        spark_sql = "SELECT s.`sale_id` FROM _dvt_s s WHERE s.amount > 100 LIMIT 50"
        source_info = {
            "src_s": _sf_source(
                "src_s", "_dvt_s", "public", "sales", ["sale_id", "amount"]
            ),
        }
        plans = optimizer.optimize(spark_sql, source_info)
        sql = plans["src_s"].extraction_sql
        assert '"sale_id"' in sql  # Snowflake double quotes
        assert "LIMIT 50" in sql
        assert "amount > 100" in sql  # Predicate included

    def test_extraction_sql_has_columns_and_predicates(self, optimizer):
        """extraction_sql combines columns + predicates + LIMIT."""
        spark_sql = (
            "SELECT c.`Customer Code`, c.`Customer name` FROM _dvt_c c "
            "WHERE c.`Customer Channel type` = 'Modern Trade' LIMIT 25"
        )
        source_info = {
            "src_c": _pg_source(
                "src_c",
                "_dvt_c",
                "public",
                "customers",
                ["Customer Code", "Customer name", "Customer Channel type"],
            ),
        }
        plans = optimizer.optimize(spark_sql, source_info)
        sql = plans["src_c"].extraction_sql
        assert '"Customer Code"' in sql
        assert '"Customer name"' in sql
        assert '"Customer Channel type"' in sql  # WHERE column included
        assert "Modern Trade" in sql  # Predicate value
        assert "LIMIT 25" in sql


# =============================================================================
# Column Case Resolution Tests
# =============================================================================


class TestColumnCaseResolution:
    """Test that sqlglot column names are resolved to real DB casing."""

    def test_case_insensitive_match(self, optimizer):
        """Lowercased sqlglot names match real DB column names."""
        spark_sql = "SELECT c.region_code FROM _dvt_c c"
        source_info = {
            "src_c": _pg_source(
                "src_c", "_dvt_c", "public", "customers", ["Region_Code"]
            ),  # DB has mixed case
        }
        plans = optimizer.optimize(spark_sql, source_info)
        # Should resolve to DB casing
        assert plans["src_c"].columns == ["Region_Code"]

    def test_quoted_column_preserves_case(self, optimizer):
        """Quoted column names preserve their original case."""
        spark_sql = "SELECT c.`Customer Code` FROM _dvt_c c"
        source_info = {
            "src_c": _pg_source(
                "src_c", "_dvt_c", "public", "customers", ["Customer Code"]
            ),
        }
        plans = optimizer.optimize(spark_sql, source_info)
        assert "Customer Code" in plans["src_c"].columns


# =============================================================================
# Fallback / Edge Cases
# =============================================================================


class TestFallbackBehavior:
    """Test graceful degradation on errors."""

    def test_empty_source_info(self, optimizer):
        """Empty source_info returns empty plans."""
        plans = optimizer.optimize("SELECT 1", {})
        assert plans == {}

    def test_invalid_sql_fallback(self, optimizer):
        """Invalid SQL falls back to SELECT * plans."""
        source_info = {
            "src_c": _pg_source(
                "src_c", "_dvt_c", "public", "customers", ["id", "name"]
            ),
        }
        plans = optimizer.optimize("THIS IS NOT VALID SQL !!! ;;;", source_info)
        # Should fall back to SELECT * (columns=None)
        assert plans["src_c"].columns is None
        assert plans["src_c"].predicates == []

    def test_plan_has_correct_metadata(self, optimizer):
        """ExtractionPlan has correct source_dialect."""
        spark_sql = "SELECT c.name FROM _dvt_c c"
        source_info = {
            "src_c": _pg_source("src_c", "_dvt_c", "public", "customers", ["name"]),
        }
        plans = optimizer.optimize(spark_sql, source_info)
        assert plans["src_c"].source_dialect == "postgres"
        assert plans["src_c"].source_id == "src_c"

    def test_no_columns_fallback_to_select_star(self, optimizer):
        """Source with empty columns list falls back to SELECT *."""
        spark_sql = "SELECT c.name FROM _dvt_c c"
        source_info = {
            "src_c": _pg_source(
                "src_c", "_dvt_c", "public", "customers", []
            ),  # Empty columns — schema not available
        }
        plans = optimizer.optimize(spark_sql, source_info)
        # With empty column list, scope.source_columns returns names but
        # resolve_to_real_columns can't match → falls back to None
        assert plans["src_c"].columns is None


# =============================================================================
# Self-Join / Alias Tests
# =============================================================================


class TestAliasHandling:
    """Test proper alias resolution."""

    def test_no_alias_uses_view_name(self, optimizer):
        """Table without alias uses the view name directly."""
        spark_sql = "SELECT _dvt_c.name FROM _dvt_c"
        source_info = {
            "src_c": _pg_source("src_c", "_dvt_c", "public", "customers", ["name"]),
        }
        plans = optimizer.optimize(spark_sql, source_info)
        assert plans["src_c"].columns == ["name"]


# =============================================================================
# Union-of-Columns (EL Layer Integration) Tests
# =============================================================================


class TestExtractionPlanDataclass:
    """Test ExtractionPlan dataclass defaults and fields."""

    def test_default_values(self):
        plan = ExtractionPlan(source_id="test")
        assert plan.columns is None
        assert plan.predicates == []
        assert plan.limit is None
        assert plan.source_dialect == ""
        assert plan.extraction_sql == ""

    def test_with_values(self):
        plan = ExtractionPlan(
            source_id="src_c",
            columns=["id", "name"],
            predicates=["id > 10"],
            limit=100,
            source_dialect="postgres",
            extraction_sql='SELECT "id", "name" FROM "public"."customers" LIMIT 100',
        )
        assert plan.columns == ["id", "name"]
        assert plan.limit == 100
        assert "LIMIT 100" in plan.extraction_sql
