# coding=utf-8
"""Unit tests for Phase 4: Incremental load strategies (merge / delete+insert).

Tests:
A. _get_staging_table_name() — naming convention for staging tables
B. _build_merge_sql() — per-dialect MERGE SQL generation
C. _build_delete_insert_sql() — DELETE + INSERT SQL generation
D. load() dispatcher — routing to merge / delete+insert / jdbc
E. _load_merge() code structure — temp table pattern with try/finally
F. _load_delete_insert() code structure — temp table pattern with try/finally
G. Engine _write_to_target() wiring — incremental_strategy + unique_key propagation
H. LoadConfig dataclass — Phase 4 fields
"""

import inspect
from typing import List, Optional
from unittest.mock import MagicMock, patch

import pytest


# =============================================================================
# Helpers
# =============================================================================


def _make_mock_adapter(adapter_type: str = "postgres"):
    """Create a mock dbt adapter that supports get_quoted_table_name."""
    adapter = MagicMock()
    adapter.type.return_value = adapter_type

    # Build a mock Relation class that quotes identifiers.
    # We simulate get_quoted_table_name by patching at call site.
    return adapter


def _get_loader():
    """Get a fresh FederationLoader instance."""
    from dvt.federation.loaders.base import FederationLoader

    return FederationLoader(on_progress=lambda msg: None)


# =============================================================================
# A. Staging Table Naming Tests
# =============================================================================


class TestGetStagingTableName:
    """Tests for _get_staging_table_name()."""

    def test_two_part_table_name(self):
        """schema.table -> schema._dvt_staging_table."""
        from dvt.federation.loaders.base import LoadConfig

        loader = _get_loader()
        config = LoadConfig(table_name="dvt_test.my_model")
        result = loader._get_staging_table_name(config)
        assert result == "dvt_test._dvt_staging_my_model"

    def test_three_part_table_name(self):
        """catalog.schema.table -> schema._dvt_staging_table (schema from 2nd part)."""
        from dvt.federation.loaders.base import LoadConfig

        loader = _get_loader()
        config = LoadConfig(table_name="my_catalog.dvt_test.my_model")
        result = loader._get_staging_table_name(config)
        assert result == "dvt_test._dvt_staging_my_model"

    def test_one_part_table_name(self):
        """my_model -> _dvt_staging_my_model (no schema prefix)."""
        from dvt.federation.loaders.base import LoadConfig

        loader = _get_loader()
        config = LoadConfig(table_name="my_model")
        result = loader._get_staging_table_name(config)
        assert result == "_dvt_staging_my_model"

    def test_prefix_is_deterministic(self):
        """Same table name should always produce same staging name."""
        from dvt.federation.loaders.base import LoadConfig

        loader = _get_loader()
        config = LoadConfig(table_name="public.orders")
        r1 = loader._get_staging_table_name(config)
        r2 = loader._get_staging_table_name(config)
        assert r1 == r2

    def test_different_tables_different_staging(self):
        """Different target tables should produce different staging names."""
        from dvt.federation.loaders.base import LoadConfig

        loader = _get_loader()
        c1 = LoadConfig(table_name="public.orders")
        c2 = LoadConfig(table_name="public.customers")
        assert loader._get_staging_table_name(c1) != loader._get_staging_table_name(c2)


# =============================================================================
# B. MERGE SQL Generation Tests
# =============================================================================


class TestBuildMergeSql:
    """Tests for _build_merge_sql() — per-dialect MERGE generation."""

    def _build(
        self,
        adapter_type: str,
        target_table: str = "public.orders",
        staging_table: str = "public._dvt_staging_orders",
        unique_key: Optional[List[str]] = None,
        columns: Optional[List[str]] = None,
    ) -> str:
        """Helper to build MERGE SQL with mocked adapter quoting."""
        if unique_key is None:
            unique_key = ["id"]
        if columns is None:
            columns = ["id", "customer_code", "quantity", "price"]

        loader = _get_loader()
        adapter = _make_mock_adapter(adapter_type)

        # Patch get_quoted_table_name at source module (inline import target)
        with patch(
            "dvt.federation.adapter_manager.get_quoted_table_name",
            side_effect=lambda a, t, **kw: f'"{t}"',
        ):
            return loader._build_merge_sql(
                adapter_type=adapter_type,
                adapter=adapter,
                target_table=target_table,
                staging_table=staging_table,
                unique_key=unique_key,
                columns=columns,
            )

    # --- Postgres ---

    def test_postgres_uses_on_conflict(self):
        """Postgres MERGE should use INSERT ... ON CONFLICT ... DO UPDATE SET."""
        sql = self._build("postgres")
        assert "ON CONFLICT" in sql
        assert "DO UPDATE SET" in sql

    def test_postgres_has_insert_into(self):
        """Postgres MERGE should have INSERT INTO target."""
        sql = self._build("postgres")
        assert "INSERT INTO" in sql

    def test_postgres_selects_from_staging(self):
        """Postgres MERGE should SELECT from staging table."""
        sql = self._build("postgres")
        assert "_dvt_staging_orders" in sql

    def test_postgres_excludes_key_from_update(self):
        """Postgres UPDATE SET should use EXCLUDED and exclude key columns."""
        sql = self._build("postgres")
        assert "EXCLUDED" in sql
        # Non-key columns should be updated
        assert "customer_code" in sql.lower() or '"customer_code"' in sql

    def test_postgres_single_key(self):
        """Postgres ON CONFLICT with single key column."""
        sql = self._build("postgres", unique_key=["id"])
        assert "ON CONFLICT" in sql
        # Key should appear in conflict clause
        assert '"id"' in sql

    def test_postgres_multi_key(self):
        """Postgres ON CONFLICT with composite key."""
        sql = self._build(
            "postgres",
            unique_key=["customer_code", "sku_code"],
            columns=["customer_code", "sku_code", "quantity"],
        )
        assert "ON CONFLICT" in sql
        assert '"customer_code"' in sql
        assert '"sku_code"' in sql

    def test_postgres_all_columns_are_keys(self):
        """When all columns are keys, UPDATE SET should still have something."""
        sql = self._build(
            "postgres",
            unique_key=["id", "code"],
            columns=["id", "code"],
        )
        assert "ON CONFLICT" in sql
        assert "DO UPDATE SET" in sql
        # Should update first key as no-op
        assert "EXCLUDED" in sql

    # --- MySQL ---

    def test_mysql_uses_on_duplicate_key(self):
        """MySQL MERGE should use ON DUPLICATE KEY UPDATE."""
        sql = self._build("mysql")
        assert "ON DUPLICATE KEY UPDATE" in sql

    def test_mysql_uses_values_function(self):
        """MySQL UPDATE uses VALUES(column) syntax."""
        sql = self._build("mysql")
        assert "VALUES(" in sql

    def test_mysql_has_insert_into(self):
        """MySQL MERGE should have INSERT INTO target."""
        sql = self._build("mysql")
        assert "INSERT INTO" in sql

    # --- Redshift ---

    def test_redshift_uses_delete_insert(self):
        """Redshift MERGE should use DELETE + INSERT (no native MERGE)."""
        sql = self._build("redshift")
        assert "DELETE FROM" in sql
        assert "INSERT INTO" in sql

    def test_redshift_uses_using_clause(self):
        """Redshift DELETE should use USING clause."""
        sql = self._build("redshift")
        assert "USING" in sql

    def test_redshift_has_semicolon_separator(self):
        """Redshift returns two statements separated by semicolon."""
        sql = self._build("redshift")
        assert ";" in sql

    # --- Standard (Databricks, Snowflake, BigQuery, SQL Server) ---

    def test_databricks_uses_merge_into(self):
        """Databricks should use standard MERGE INTO syntax."""
        sql = self._build("databricks")
        assert "MERGE INTO" in sql

    def test_snowflake_uses_merge_into(self):
        """Snowflake should use standard MERGE INTO syntax."""
        sql = self._build("snowflake")
        assert "MERGE INTO" in sql

    def test_bigquery_uses_merge_into(self):
        """BigQuery should use standard MERGE INTO syntax."""
        sql = self._build("bigquery")
        assert "MERGE INTO" in sql

    def test_sqlserver_uses_merge_into(self):
        """SQL Server should use standard MERGE INTO syntax."""
        sql = self._build("sqlserver")
        assert "MERGE INTO" in sql

    def test_standard_has_using_clause(self):
        """Standard MERGE should have USING staging AS stg."""
        sql = self._build("databricks")
        assert "USING" in sql
        assert "AS stg" in sql

    def test_standard_has_target_alias(self):
        """Standard MERGE should alias target AS target."""
        sql = self._build("databricks")
        assert "AS target" in sql

    def test_standard_has_when_matched(self):
        """Standard MERGE should have WHEN MATCHED THEN UPDATE SET."""
        sql = self._build("databricks")
        assert "WHEN MATCHED THEN UPDATE SET" in sql

    def test_standard_has_when_not_matched(self):
        """Standard MERGE should have WHEN NOT MATCHED THEN INSERT."""
        sql = self._build("databricks")
        assert "WHEN NOT MATCHED THEN INSERT" in sql

    def test_standard_on_clause_uses_keys(self):
        """ON clause should join on unique_key columns."""
        sql = self._build("databricks", unique_key=["id"])
        # Databricks uses backtick quoting
        assert "target.`id` = stg.`id`" in sql

    def test_standard_multi_key_on_clause(self):
        """ON clause should AND-join multiple unique key columns."""
        sql = self._build(
            "databricks",
            unique_key=["customer_code", "sku_code"],
            columns=["customer_code", "sku_code", "quantity"],
        )
        assert " AND " in sql
        # Databricks uses backtick quoting
        assert "target.`customer_code` = stg.`customer_code`" in sql
        assert "target.`sku_code` = stg.`sku_code`" in sql

    def test_standard_update_excludes_keys(self):
        """WHEN MATCHED UPDATE SET should only update non-key columns."""
        sql = self._build(
            "databricks",
            unique_key=["id"],
            columns=["id", "customer_code", "quantity"],
        )
        # Non-key columns should be in the SET clause (backtick quoting for databricks)
        assert "target.`customer_code` = stg.`customer_code`" in sql
        assert "target.`quantity` = stg.`quantity`" in sql

    def test_standard_values_uses_stg_prefix(self):
        """INSERT VALUES should use stg.column references."""
        sql = self._build("databricks")
        assert "stg." in sql

    def test_standard_all_columns_are_keys_noop_update(self):
        """When all columns are keys, UPDATE SET should be a no-op on first key."""
        sql = self._build(
            "databricks",
            unique_key=["id", "code"],
            columns=["id", "code"],
        )
        assert "WHEN MATCHED THEN UPDATE SET" in sql
        # Should have a no-op update on the first key (backtick quoting for databricks)
        assert "target.`id` = stg.`id`" in sql


# =============================================================================
# C. DELETE+INSERT SQL Generation Tests
# =============================================================================


class TestBuildDeleteInsertSql:
    """Tests for _build_delete_insert_sql() — DELETE + INSERT generation."""

    def _build(
        self,
        adapter_type: str,
        target_table: str = "public.orders",
        staging_table: str = "public._dvt_staging_orders",
        unique_key: Optional[List[str]] = None,
        columns: Optional[List[str]] = None,
    ) -> tuple:
        """Helper to build DELETE + INSERT SQL."""
        if unique_key is None:
            unique_key = ["id"]
        if columns is None:
            columns = ["id", "customer_code", "quantity", "price"]

        loader = _get_loader()
        adapter = _make_mock_adapter(adapter_type)

        with patch(
            "dvt.federation.adapter_manager.get_quoted_table_name",
            side_effect=lambda a, t, **kw: f'"{t}"',
        ):
            return loader._build_delete_insert_sql(
                adapter_type=adapter_type,
                adapter=adapter,
                target_table=target_table,
                staging_table=staging_table,
                unique_key=unique_key,
                columns=columns,
            )

    # --- Postgres / Redshift (DELETE USING) ---

    def test_postgres_uses_delete_using(self):
        """Postgres DELETE should use DELETE FROM ... USING ... WHERE."""
        delete_sql, _ = self._build("postgres")
        assert "DELETE FROM" in delete_sql
        assert "USING" in delete_sql

    def test_redshift_uses_delete_using(self):
        """Redshift DELETE should also use DELETE FROM ... USING ... WHERE."""
        delete_sql, _ = self._build("redshift")
        assert "DELETE FROM" in delete_sql
        assert "USING" in delete_sql

    def test_postgres_delete_matches_on_keys(self):
        """Postgres DELETE WHERE should match on unique key columns."""
        delete_sql, _ = self._build("postgres", unique_key=["id"])
        assert '"id"' in delete_sql

    def test_postgres_multi_key_delete(self):
        """Postgres DELETE with composite key uses AND-joined conditions."""
        delete_sql, _ = self._build(
            "postgres",
            unique_key=["customer_code", "sku_code"],
            columns=["customer_code", "sku_code", "quantity"],
        )
        assert " AND " in delete_sql
        assert '"customer_code"' in delete_sql
        assert '"sku_code"' in delete_sql

    # --- Standard (Databricks, Snowflake, etc.) ---

    def test_standard_single_key_uses_in_subquery(self):
        """Standard DELETE with single key uses WHERE key IN (SELECT ...)."""
        delete_sql, _ = self._build("databricks", unique_key=["id"])
        assert "DELETE FROM" in delete_sql
        assert "IN" in delete_sql
        assert "SELECT" in delete_sql

    def test_standard_multi_key_uses_exists(self):
        """Standard DELETE with multi-column key uses WHERE EXISTS correlated."""
        delete_sql, _ = self._build(
            "databricks",
            unique_key=["customer_code", "sku_code"],
            columns=["customer_code", "sku_code", "quantity"],
        )
        assert "DELETE FROM" in delete_sql
        assert "EXISTS" in delete_sql
        assert "AS stg" in delete_sql

    def test_standard_exists_correlates_on_all_keys(self):
        """EXISTS subquery should correlate on all unique key columns."""
        delete_sql, _ = self._build(
            "snowflake",
            unique_key=["a", "b", "c"],
            columns=["a", "b", "c", "val"],
        )
        assert 'stg."a"' in delete_sql
        assert 'stg."b"' in delete_sql
        assert 'stg."c"' in delete_sql

    # --- INSERT (all dialects) ---

    def test_insert_has_all_columns(self):
        """INSERT should include all columns."""
        _, insert_sql = self._build("postgres", columns=["id", "name", "value"])
        assert "INSERT INTO" in insert_sql
        assert '"id"' in insert_sql
        assert '"name"' in insert_sql
        assert '"value"' in insert_sql

    def test_insert_selects_from_staging(self):
        """INSERT should SELECT from staging table."""
        _, insert_sql = self._build("postgres")
        assert "SELECT" in insert_sql
        assert "_dvt_staging_orders" in insert_sql

    def test_returns_tuple(self):
        """_build_delete_insert_sql should return a tuple of (delete, insert)."""
        result = self._build("postgres")
        assert isinstance(result, tuple)
        assert len(result) == 2

    def test_delete_and_insert_are_separate(self):
        """DELETE and INSERT should be separate SQL statements (not joined by ;)."""
        delete_sql, insert_sql = self._build("postgres")
        assert ";" not in delete_sql
        assert ";" not in insert_sql


# =============================================================================
# D. load() Dispatcher Routing Tests
# =============================================================================


class TestLoadDispatcher:
    """Tests for load() dispatch logic."""

    def test_merge_dispatches_to_load_merge(self):
        """strategy='merge' + unique_key + adapter -> _load_merge."""
        from dvt.federation.loaders.base import FederationLoader, LoadConfig

        loader = FederationLoader()
        config = LoadConfig(
            table_name="public.orders",
            incremental_strategy="merge",
            unique_key=["id"],
            connection_config={"type": "postgres"},
        )
        adapter = _make_mock_adapter()

        with patch.object(loader, "_load_merge") as mock_merge:
            mock_merge.return_value = MagicMock(success=True)
            loader.load(MagicMock(), config, adapter)
            mock_merge.assert_called_once()

    def test_delete_insert_dispatches_to_load_delete_insert(self):
        """strategy='delete+insert' + unique_key + adapter -> _load_delete_insert."""
        from dvt.federation.loaders.base import FederationLoader, LoadConfig

        loader = FederationLoader()
        config = LoadConfig(
            table_name="public.orders",
            incremental_strategy="delete+insert",
            unique_key=["id"],
            connection_config={"type": "postgres"},
        )
        adapter = _make_mock_adapter()

        with patch.object(loader, "_load_delete_insert") as mock_di:
            mock_di.return_value = MagicMock(success=True)
            loader.load(MagicMock(), config, adapter)
            mock_di.assert_called_once()

    def test_append_dispatches_to_load_jdbc(self):
        """strategy='append' falls through to _load_jdbc (default)."""
        from dvt.federation.loaders.base import FederationLoader, LoadConfig

        loader = FederationLoader()
        config = LoadConfig(
            table_name="public.orders",
            incremental_strategy="append",
            unique_key=None,
            connection_config={"type": "postgres"},
        )
        adapter = _make_mock_adapter()

        with patch.object(loader, "_load_jdbc") as mock_jdbc:
            mock_jdbc.return_value = MagicMock(success=True)
            loader.load(MagicMock(), config, adapter)
            mock_jdbc.assert_called_once()

    def test_no_strategy_dispatches_to_load_jdbc(self):
        """No strategy -> _load_jdbc (default path)."""
        from dvt.federation.loaders.base import FederationLoader, LoadConfig

        loader = FederationLoader()
        config = LoadConfig(
            table_name="public.orders",
            incremental_strategy=None,
            unique_key=None,
            connection_config={"type": "postgres"},
        )
        adapter = _make_mock_adapter()

        with patch.object(loader, "_load_jdbc") as mock_jdbc:
            mock_jdbc.return_value = MagicMock(success=True)
            loader.load(MagicMock(), config, adapter)
            mock_jdbc.assert_called_once()

    def test_merge_without_unique_key_falls_to_jdbc(self):
        """strategy='merge' but no unique_key -> falls through to _load_jdbc."""
        from dvt.federation.loaders.base import FederationLoader, LoadConfig

        loader = FederationLoader()
        config = LoadConfig(
            table_name="public.orders",
            incremental_strategy="merge",
            unique_key=None,  # Missing!
            connection_config={"type": "postgres"},
        )
        adapter = _make_mock_adapter()

        with patch.object(loader, "_load_jdbc") as mock_jdbc:
            mock_jdbc.return_value = MagicMock(success=True)
            loader.load(MagicMock(), config, adapter)
            mock_jdbc.assert_called_once()

    def test_merge_without_adapter_falls_to_jdbc(self):
        """strategy='merge' but no adapter -> falls through to _load_jdbc."""
        from dvt.federation.loaders.base import FederationLoader, LoadConfig

        loader = FederationLoader()
        config = LoadConfig(
            table_name="public.orders",
            incremental_strategy="merge",
            unique_key=["id"],
            connection_config={"type": "postgres"},
        )

        with patch.object(loader, "_load_jdbc") as mock_jdbc:
            mock_jdbc.return_value = MagicMock(success=True)
            loader.load(MagicMock(), config, adapter=None)
            mock_jdbc.assert_called_once()

    def test_delete_insert_without_unique_key_falls_to_jdbc(self):
        """strategy='delete+insert' but no unique_key -> falls through to _load_jdbc."""
        from dvt.federation.loaders.base import FederationLoader, LoadConfig

        loader = FederationLoader()
        config = LoadConfig(
            table_name="public.orders",
            incremental_strategy="delete+insert",
            unique_key=None,
            connection_config={"type": "postgres"},
        )
        adapter = _make_mock_adapter()

        with patch.object(loader, "_load_jdbc") as mock_jdbc:
            mock_jdbc.return_value = MagicMock(success=True)
            loader.load(MagicMock(), config, adapter)
            mock_jdbc.assert_called_once()


# =============================================================================
# E. _load_merge() Code Structure Tests
# =============================================================================


class TestLoadMergeStructure:
    """Tests for _load_merge() code structure."""

    def _get_source(self):
        from dvt.federation.loaders.base import FederationLoader

        return inspect.getsource(FederationLoader._load_merge)

    def test_creates_target_table(self):
        """_load_merge should call _create_table_with_adapter for target."""
        source = self._get_source()
        assert "_create_table_with_adapter" in source

    def test_writes_to_staging_table(self):
        """_load_merge should call _write_to_staging_table."""
        source = self._get_source()
        assert "_write_to_staging_table" in source

    def test_builds_merge_sql(self):
        """_load_merge should call _build_merge_sql."""
        source = self._get_source()
        assert "_build_merge_sql" in source

    def test_executes_merge_via_adapter(self):
        """_load_merge should execute MERGE SQL via adapter."""
        source = self._get_source()
        assert "adapter.execute" in source

    def test_commits_after_merge(self):
        """_load_merge should commit after executing MERGE."""
        source = self._get_source()
        assert "_safe_commit" in source

    def test_has_finally_cleanup(self):
        """_load_merge should have finally block for staging cleanup."""
        source = self._get_source()
        assert "finally:" in source
        assert "_drop_staging_table" in source

    def test_returns_load_result_with_merge_method(self):
        """_load_merge should return LoadResult with load_method='merge'."""
        source = self._get_source()
        assert '"merge"' in source

    def test_has_error_handling(self):
        """_load_merge should catch exceptions and return failure result."""
        source = self._get_source()
        assert "except Exception" in source

    def test_staging_table_uses_get_staging_table_name(self):
        """Should use _get_staging_table_name to derive staging table."""
        source = self._get_source()
        assert "_get_staging_table_name" in source

    def test_gets_columns_from_df_schema(self):
        """Should extract column names from df.schema.fields."""
        source = self._get_source()
        assert "df.schema.fields" in source

    def test_order_create_write_merge(self):
        """Should: create target -> write staging -> execute merge."""
        source = self._get_source()
        create_pos = source.find("_create_table_with_adapter")
        write_pos = source.find("_write_to_staging_table")
        merge_pos = source.find("_build_merge_sql")
        assert 0 < create_pos < write_pos < merge_pos


# =============================================================================
# F. _load_delete_insert() Code Structure Tests
# =============================================================================


class TestLoadDeleteInsertStructure:
    """Tests for _load_delete_insert() code structure."""

    def _get_source(self):
        from dvt.federation.loaders.base import FederationLoader

        return inspect.getsource(FederationLoader._load_delete_insert)

    def test_creates_target_table(self):
        """_load_delete_insert should call _create_table_with_adapter."""
        source = self._get_source()
        assert "_create_table_with_adapter" in source

    def test_writes_to_staging_table(self):
        """_load_delete_insert should call _write_to_staging_table."""
        source = self._get_source()
        assert "_write_to_staging_table" in source

    def test_builds_delete_insert_sql(self):
        """_load_delete_insert should call _build_delete_insert_sql."""
        source = self._get_source()
        assert "_build_delete_insert_sql" in source

    def test_executes_both_statements(self):
        """Should execute both DELETE and INSERT via adapter."""
        source = self._get_source()
        # Should have two adapter.execute calls (delete + insert)
        assert source.count("adapter.execute") >= 2

    def test_commits_after_both_statements(self):
        """Should commit after both DELETE and INSERT."""
        source = self._get_source()
        assert "_safe_commit" in source

    def test_has_finally_cleanup(self):
        """Should have finally block for staging table cleanup."""
        source = self._get_source()
        assert "finally:" in source
        assert "_drop_staging_table" in source

    def test_returns_load_result_with_delete_insert_method(self):
        """Should return LoadResult with load_method='delete+insert'."""
        source = self._get_source()
        assert '"delete+insert"' in source

    def test_has_error_handling(self):
        """Should catch exceptions and return failure result."""
        source = self._get_source()
        assert "except Exception" in source

    def test_order_create_write_delete_insert(self):
        """Should: create target -> write staging -> delete -> insert."""
        source = self._get_source()
        create_pos = source.find("_create_table_with_adapter")
        write_pos = source.find("_write_to_staging_table")
        delete_pos = source.find("_build_delete_insert_sql")
        assert 0 < create_pos < write_pos < delete_pos


# =============================================================================
# G. Engine _write_to_target() Wiring Tests
# =============================================================================


class TestEngineWriteToTargetWiring:
    """Tests for incremental_strategy + unique_key propagation in engine."""

    def _get_source(self):
        from dvt.federation.engine import FederationEngine

        return inspect.getsource(FederationEngine._write_to_target)

    def test_reads_incremental_strategy(self):
        """_write_to_target should read model.config.incremental_strategy."""
        source = self._get_source()
        assert "incremental_strategy" in source

    def test_reads_unique_key(self):
        """_write_to_target should read model.config.unique_key."""
        source = self._get_source()
        assert "unique_key" in source

    def test_normalizes_unique_key_string_to_list(self):
        """Should normalize string unique_key to List[str]."""
        source = self._get_source()
        assert "isinstance(raw_unique_key, str)" in source

    def test_normalizes_unique_key_list(self):
        """Should pass through list unique_key."""
        source = self._get_source()
        assert "isinstance(raw_unique_key, list)" in source

    def test_only_reads_for_incremental(self):
        """Should only read strategy/unique_key for incremental materializations."""
        source = self._get_source()
        assert 'mat == "incremental"' in source

    def test_not_read_during_full_refresh(self):
        """Should not read strategy/unique_key during full_refresh."""
        source = self._get_source()
        assert "not full_refresh" in source

    def test_passes_strategy_to_load_config(self):
        """Should pass incremental_strategy to LoadConfig constructor."""
        source = self._get_source()
        assert "incremental_strategy=incremental_strategy" in source

    def test_passes_unique_key_to_load_config(self):
        """Should pass unique_key to LoadConfig constructor."""
        source = self._get_source()
        assert "unique_key=unique_key" in source

    def test_unique_key_none_when_not_set(self):
        """unique_key should default to None when not configured."""
        source = self._get_source()
        # The else branch should set unique_key = None
        assert "unique_key = None" in source


# =============================================================================
# H. LoadConfig Dataclass Tests
# =============================================================================


class TestLoadConfigPhase4Fields:
    """Tests for Phase 4 fields on LoadConfig dataclass."""

    def test_has_incremental_strategy_field(self):
        """LoadConfig should have incremental_strategy field."""
        from dvt.federation.loaders.base import LoadConfig

        config = LoadConfig(table_name="test")
        assert hasattr(config, "incremental_strategy")
        assert config.incremental_strategy is None

    def test_has_unique_key_field(self):
        """LoadConfig should have unique_key field."""
        from dvt.federation.loaders.base import LoadConfig

        config = LoadConfig(table_name="test")
        assert hasattr(config, "unique_key")
        assert config.unique_key is None

    def test_incremental_strategy_accepts_merge(self):
        """incremental_strategy should accept 'merge'."""
        from dvt.federation.loaders.base import LoadConfig

        config = LoadConfig(table_name="test", incremental_strategy="merge")
        assert config.incremental_strategy == "merge"

    def test_incremental_strategy_accepts_delete_insert(self):
        """incremental_strategy should accept 'delete+insert'."""
        from dvt.federation.loaders.base import LoadConfig

        config = LoadConfig(table_name="test", incremental_strategy="delete+insert")
        assert config.incremental_strategy == "delete+insert"

    def test_unique_key_accepts_list(self):
        """unique_key should accept List[str]."""
        from dvt.federation.loaders.base import LoadConfig

        config = LoadConfig(table_name="test", unique_key=["id", "code"])
        assert config.unique_key == ["id", "code"]

    def test_load_result_has_merge_method(self):
        """LoadResult should accept load_method='merge'."""
        from dvt.federation.loaders.base import LoadResult

        result = LoadResult(success=True, table_name="test", load_method="merge")
        assert result.load_method == "merge"

    def test_load_result_has_delete_insert_method(self):
        """LoadResult should accept load_method='delete+insert'."""
        from dvt.federation.loaders.base import LoadResult

        result = LoadResult(
            success=True, table_name="test", load_method="delete+insert"
        )
        assert result.load_method == "delete+insert"


# =============================================================================
# I. Helper Methods Tests
# =============================================================================


class TestHelperMethods:
    """Tests for staging helper methods."""

    def test_drop_staging_table_method_exists(self):
        """FederationLoader should have _drop_staging_table method."""
        from dvt.federation.loaders.base import FederationLoader

        assert hasattr(FederationLoader, "_drop_staging_table")

    def test_write_to_staging_table_method_exists(self):
        """FederationLoader should have _write_to_staging_table method."""
        from dvt.federation.loaders.base import FederationLoader

        assert hasattr(FederationLoader, "_write_to_staging_table")

    def test_drop_staging_uses_adapter(self):
        """_drop_staging_table should execute DROP via adapter."""
        source = inspect.getsource(
            __import__(
                "dvt.federation.loaders.base", fromlist=["FederationLoader"]
            ).FederationLoader._drop_staging_table
        )
        assert "DROP TABLE IF EXISTS" in source
        assert "adapter.execute" in source or "adapter" in source

    def test_drop_staging_has_error_handling(self):
        """_drop_staging_table should handle errors gracefully."""
        from dvt.federation.loaders.base import FederationLoader

        source = inspect.getsource(FederationLoader._drop_staging_table)
        assert "except Exception" in source

    def test_write_to_staging_uses_jdbc(self):
        """_write_to_staging_table should write via Spark JDBC."""
        from dvt.federation.loaders.base import FederationLoader

        source = inspect.getsource(FederationLoader._write_to_staging_table)
        assert "write.jdbc" in source

    def test_write_to_staging_uses_overwrite_mode(self):
        """_write_to_staging_table should use overwrite mode."""
        from dvt.federation.loaders.base import FederationLoader

        source = inspect.getsource(FederationLoader._write_to_staging_table)
        assert '"overwrite"' in source


# =============================================================================
# J. Integration: Dialect-Specific Quoting in MERGE SQL
# =============================================================================


class TestDialectQuotingInMergeSql:
    """Tests that _build_merge_sql uses quote_identifier for column names."""

    def test_merge_sql_calls_quote_identifier(self):
        """_build_merge_sql should use quote_identifier for column names."""
        from dvt.federation.loaders.base import FederationLoader

        source = inspect.getsource(FederationLoader._build_merge_sql)
        assert "quote_identifier" in source

    def test_delete_insert_sql_calls_quote_identifier(self):
        """_build_delete_insert_sql should use quote_identifier for columns."""
        from dvt.federation.loaders.base import FederationLoader

        source = inspect.getsource(FederationLoader._build_delete_insert_sql)
        assert "quote_identifier" in source

    def test_merge_sql_calls_get_quoted_table_name(self):
        """_build_merge_sql should use get_quoted_table_name for tables."""
        from dvt.federation.loaders.base import FederationLoader

        source = inspect.getsource(FederationLoader._build_merge_sql)
        assert "get_quoted_table_name" in source

    def test_delete_insert_sql_calls_get_quoted_table_name(self):
        """_build_delete_insert_sql should use get_quoted_table_name for tables."""
        from dvt.federation.loaders.base import FederationLoader

        source = inspect.getsource(FederationLoader._build_delete_insert_sql)
        assert "get_quoted_table_name" in source
