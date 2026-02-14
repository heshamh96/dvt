# Trial 15 - Comprehensive DVT Command Testing Plan

**Trial**: `trial_15_federation_e2e`  
**Project**: `Coke_DB`  
**Date**: February 11, 2026  
**DVT Version**: 1.12.103  
**Constraint**: No writes to Snowflake (reads allowed)

---

## Project Setup

### Targets Available

| Target | Adapter | Host/Account | Default |
|--------|---------|-------------|---------|
| `postgres` | postgres | localhost:5433 | Yes |
| `snowflake` | snowflake | SAUDIEXIM-EDWH | No |
| `databricks` | databricks | dbc-e991b0fe-3bbf.cloud.databricks.com | No |

### Sources Defined (16 total across 4 source groups)

| Source Group | Connection | Database/Schema | Tables |
|-------------|-----------|----------------|--------|
| `Postgres` | postgres | postgres / public | customers_db_1, customers_db_2, packs, transactions_a/b/c |
| `Exim` | snowflake | EXIM_EDWH_DEV / ods | cbs_closed_loans, cbs_f_ac_balance_type, cbs_f_country, cbs_f_dept_acct_officer |
| `postgres_source` | postgres | postgres / public | customers_db_1, employees, packs |
| `databricks_source` | databricks | demo / dvt_test | dim_regions, dim_categories |
| `snowflake_exim` | snowflake | EXIM_EDWH_DEV / ods | cbs_f_country |

### Seeds (13 total)

| Seed | Format | Target Config | Rows |
|------|--------|--------------|------|
| customers_db_1 | CSV | postgres / public | 469 |
| customers_db_2 | CSV | postgres / public | 146 |
| customers_parquet | Parquet | - | 469 |
| packs | CSV | postgres / public | 63 |
| packs_parquet | Parquet | - | 63 |
| employees | CSV | postgres / public | 9 |
| employees_json | JSON | - | 9 |
| dim_files | CSV | - | 3 |
| transactions_a | CSV | - | 1,046,572 |
| transactions_b | CSV | - | 1,046,572 |
| transactions_c | CSV | - | 1,046,572 |
| dim_regions | CSV | databricks / dvt_test | 5 |
| dim_categories | CSV | databricks / dvt_test | 7 |

### Models (24 total)

**Federation Test Models** (7 -- 3 existing + 4 new):

| Model | Sources Used | Target | Path |
|-------|-------------|--------|------|
| `pushdown_pg_only` | postgres_source.customers_db_1 + employees | postgres | Pushdown |
| `cross_pg_databricks` | postgres_source + databricks_source | postgres | Federation |
| `snowflake_to_pg` | snowflake_exim.cbs_f_country | postgres | Federation |
| `pushdown_databricks_only` (NEW) | databricks_source.dim_regions + dim_categories | databricks | Pushdown |
| `pg_to_databricks` (NEW) | postgres_source.customers_db_1 | databricks | Federation |
| `snowflake_to_databricks` (NEW) | snowflake_exim.cbs_f_country | databricks | Federation |
| `three_way_to_databricks` (NEW) | postgres_source + databricks_source + snowflake_exim | databricks | Federation |

**Cocacola Source Views** (6): customer_1, customer_2, packs_db, transactions_1/2/3  
**Exim Pipeline** (5): bv_cbs_f_dept_acct_officer -> a -> b -> c -> d_b_dim_officers  
**Staging** (1): stg_fact_transactions  
**Target** (4): dim_customers, dim_employees (incremental), dim_packs, fact_transactions

---

## Phase 1: Foundation & Diagnostics

Verify the environment is healthy before running data commands.

| # | Command | Purpose | Expected |
|---|---------|---------|----------|
| 1.1 | `dvt debug --profiles-dir ./Connections` | Verify all connections (PG, Snowflake, Databricks) | All 3 targets show OK |
| 1.2 | `dvt sync --profiles-dir ./Connections` | Install adapters, JDBC drivers, PySpark | Sync complete |
| 1.3 | `dvt parse --profiles-dir ./Connections` | Verify project parses cleanly | 24 models, 13 seeds, 4 tests, 16 sources |
| 1.4 | `dvt list --resource-type model` | List all models | 24 models |
| 1.5 | `dvt list --resource-type seed` | List all seeds | 13 seeds |
| 1.6 | `dvt list --resource-type source` | List all sources | 16 sources |
| 1.7 | `dvt list --resource-type test` | List all tests | 4 tests |
| 1.8 | `dvt compile -s pushdown_pg_only cross_pg_databricks snowflake_to_pg` | Verify SQL compilation for federation models | SQL compiled |

---

## Phase 2: Seed Diversification

Test seeding to different targets, including mixed-target behavior.

| # | Command | Purpose | Expected |
|---|---------|---------|----------|
| 2.1 | `dvt seed --full-refresh --target postgres` | Re-seed ALL to Postgres (baseline) | PASS=13 |
| 2.2 | `dvt seed --full-refresh -s dim_regions dim_categories --target databricks` | Seed small dims to Databricks | PASS=2 |
| 2.3 | `dvt seed --full-refresh -s employees --target databricks` | Seed employees to Databricks (diversify target) | PASS=1 |
| 2.4 | `dvt seed -s customers_db_1 packs` | Seed without --target (use per-seed config) | PASS=2 (uses default PG) |
| 2.5 | `dvt seed --show -s employees` | Seed with --show to preview loaded data | PASS=1 + data preview |

---

## Phase 3: Model Runs - Diversify Targets

### Phase 3 Setup: Create 4 New Federation Test Models

Create 4 new models that target **Databricks** as output (schema `dvt_test`):

1. **`pushdown_databricks_only.sql`** -- Databricks-only pushdown (CROSS JOIN dim_regions x dim_categories)
2. **`pg_to_databricks.sql`** -- Reads PG customers_db_1, writes to Databricks via federation
3. **`snowflake_to_databricks.sql`** -- Reads Snowflake cbs_f_country, writes to Databricks via federation
4. **`three_way_to_databricks.sql`** -- Joins PG + Snowflake + Databricks sources, writes to Databricks

Update `_models.yml` to add entries for all 4 new models.

### Run Commands

| # | Command | Tests | Expected |
|---|---------|-------|----------|
| 3.1 | `dvt run -s pushdown_pg_only --full-refresh` | PG-only pushdown (re-verify) | PASS, ~0.3s |
| 3.2 | `dvt run -s cross_pg_databricks --full-refresh` | PG+Databricks -> PG federation | PASS, ~15-35s |
| 3.3 | `dvt run -s snowflake_to_pg --full-refresh` | Snowflake -> PG federation | PASS, ~15-28s |
| 3.4 | `dvt run -s pushdown_databricks_only --full-refresh` | **NEW**: Databricks-only pushdown | TBD |
| 3.5 | `dvt run -s pg_to_databricks --full-refresh` | **NEW**: PG -> Databricks federation | PASS via JDBC |
| 3.6 | `dvt run -s snowflake_to_databricks --full-refresh` | **NEW**: Snowflake -> Databricks federation | PASS via JDBC |
| 3.7 | `dvt run -s three_way_to_databricks --full-refresh` | **NEW**: 3-way -> Databricks federation | PASS via JDBC |

**Federation Matrix Being Tested:**

| Source(s) | Target | Path |
|-----------|--------|------|
| PG only | PG | Pushdown |
| PG + Databricks | PG | Federation (Spark) |
| Snowflake | PG | Federation (Spark) |
| Databricks only | Databricks | Pushdown |
| PG | Databricks | Federation (Spark) |
| Snowflake | Databricks | Federation (Spark) |
| PG + Snowflake + Databricks | Databricks | Federation (Spark) |

---

## Phase 4: Full Pipeline & Incremental Testing

Test the existing Coca-Cola DWH pipeline and incremental models.

| # | Command | Purpose | Expected |
|---|---------|---------|----------|
| 4.1 | `dvt run -s customer_1 customer_2 packs_db transactions_1 transactions_2 transactions_3` | Run all Cocacola source views | 6 views created |
| 4.2 | `dvt run -s stg_fact_transactions` | Run staging layer | 1 view (unions transactions + joins packs) |
| 4.3 | `dvt run -s dim_customers dim_packs fact_transactions --full-refresh` | Run target tables | 3 tables |
| 4.4 | `dvt run -s dim_employees --full-refresh` | Run incremental model (first load) | SELECT N rows |
| 4.5 | `dvt run -s dim_employees` | Run incremental model (second run) | INSERT 0 0 (no new rows) |
| 4.6 | `dvt run -s bv_cbs_f_dept_acct_officer --full-refresh` | Run Exim Snowflake -> PG federation pipeline (base) | Table via federation |
| 4.7 | `dvt run -s d_b_dim_officers --full-refresh` | Run full Exim pipeline through to gold layer | Table in gold schema |
| 4.8 | `dvt run --select tag:TRGT` | Run all target-tagged models | 5 models selected |

---

## Phase 5: Testing, Show, Build & Other Commands

| # | Command | Purpose | Expected |
|---|---------|---------|----------|
| 5.1 | `dvt test` | Run all generic tests | PASS=4 |
| 5.2 | `dvt test -s dim_customers` | Run tests for specific model | PASS=2 (unique + not_null) |
| 5.3 | `dvt show -s pushdown_pg_only --limit 10` | Preview PG pushdown without materializing | Table preview |
| 5.4 | `dvt show -s cross_pg_databricks --limit 5` | Preview federation model output | Table preview |
| 5.5 | `dvt show --inline "select count(*) as cnt from postgres.public.customers_db_1"` | Inline query test | cnt = N |
| 5.6 | `dvt compile` | Compile entire project | All 24 models compiled |
| 5.7 | `dvt run-operation generate_schema_name --args '{...}'` | Test macro execution | Silent success |
| 5.8 | `dvt docs generate` | Generate documentation website | Catalog written |
| 5.9 | `dvt build -s pushdown_pg_only cross_pg_databricks snowflake_to_pg --full-refresh` | Build (seed+run+test) for selected models | PASS=3 |
| 5.10 | `dvt clean` | Clean artifacts | target/ and dvt_packages/ cleaned |

---

## Phase 6: Edge Cases & Retry

| # | Command | Purpose | Expected |
|---|---------|---------|----------|
| 6.1 | `dvt run -s nonexistent_model` | Test error handling for missing model | "does not match any enabled nodes" |
| 6.2 | `dvt retry` | Retry after any failures from previous phases | Retries only failed nodes |
| 6.3 | `dvt list --output json --resource-type model` | JSON output format | Full JSON with configs, deps |
| 6.4 | `dvt list -s tag:STG` | List by tag selection | 3 STG-tagged models |

---

## Complete DVT Commands Coverage

| # | Command | Tested In |
|---|---------|-----------|
| 1 | `dvt debug` | Phase 1 |
| 2 | `dvt sync` | Phase 1 |
| 3 | `dvt parse` | Phase 1 |
| 4 | `dvt deps` | Phase 6 (after clean) |
| 5 | `dvt list` / `dvt ls` | Phase 1, 6 |
| 6 | `dvt compile` | Phase 1, 5 |
| 7 | `dvt seed` | Phase 2 |
| 8 | `dvt run` | Phase 3, 4 |
| 9 | `dvt build` | Phase 5 |
| 10 | `dvt test` | Phase 5 |
| 11 | `dvt show` | Phase 5 |
| 12 | `dvt show --inline` | Phase 5 |
| 13 | `dvt run-operation` | Phase 5 |
| 14 | `dvt docs generate` | Phase 5 |
| 15 | `dvt clean` | Phase 5 |
| 16 | `dvt retry` | Phase 6 |
| 17 | `dvt init` | Not tested (project already exists) |
| 18 | `dvt snapshot` | Not tested (no snapshots defined) |
| 19 | `dvt clone` | Not tested (requires --state) |
| 20 | `dvt docs serve` | Not tested (interactive) |
| 21 | `dvt source freshness` | Not tested (no freshness config) |

**Coverage: 16 out of 21 commands tested** (5 excluded due to prerequisites or interactivity)

---

## Notes

- All commands run from `Coke_DB/` directory with `--profiles-dir ./Connections`
- Run via: `uv run --project /path/to/trial_15_federation_e2e dvt <command>`
- Seed operations use Spark JDBC for all targets (PG via COPY, Databricks via JDBC)
- Federation uses local Spark (spark-local) with filesystem bucket staging
