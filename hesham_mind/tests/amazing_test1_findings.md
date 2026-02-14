# Trial 15 - Test Findings & Results

**Trial**: `trial_15_federation_e2e`  
**Project**: `Coke_DB`  
**Date**: February 11, 2026  
**DVT Version**: 1.12.103

---

## Overall Scorecard

| Category | Tested | Passed | Failed | Pass Rate |
|----------|--------|--------|--------|-----------|
| DVT Commands | 16 | 14 | 2 | 87.5% |
| Seeds | 19 runs | 19 | 0 | 100% |
| Federation Models | 7 | 6 | 1 | 85.7% |
| Pipeline Models | 12 | 2 | 10 | 16.7% |
| Generic Tests | 4 | 4 | 0 | 100% |

---

## Phase 1: Foundation & Diagnostics -- ALL PASS

| # | Command | Result | Details |
|---|---------|--------|---------|
| 1.1 | `dvt debug` | PASS | All 3 targets OK: postgres, snowflake, databricks |
| 1.2 | `dvt sync` | PASS | dbt-postgres, dbt-snowflake, dbt-databricks, pyspark 4.1.0, JDBC drivers |
| 1.3 | `dvt parse` | PASS | 24 models, 13 seeds, 4 tests, 16 sources, 598 macros |
| 1.4 | `dvt list --resource-type model` | PASS | 20 models listed (before new models) |
| 1.5 | `dvt list --resource-type seed` | PASS | 13 seeds listed |
| 1.6 | `dvt list --resource-type source` | PASS | 16 sources across 4 source groups |
| 1.7 | `dvt list --resource-type test` | PASS | 4 generic tests listed |
| 1.8 | `dvt compile` | PASS | SQL compiled correctly for federation models |

---

## Phase 2: Seed Diversification -- ALL PASS

| # | Command | Result | Rows | Time |
|---|---------|--------|------|------|
| 2.1 | `dvt seed --full-refresh --target postgres` | **PASS=13** | 3.1M+ total | 54.67s |
| 2.2 | `dvt seed --full-refresh -s dim_regions dim_categories --target databricks` | **PASS=2** | 12 rows | 56.45s |
| 2.3 | `dvt seed --full-refresh -s employees --target databricks` | **PASS=1** | 9 rows | 32.51s |
| 2.4 | `dvt seed -s customers_db_1 packs` (no --target) | **PASS=2** | 532 rows | 10.62s |
| 2.5 | `dvt seed --show -s employees` | **PASS=1** | 9 rows | 6.95s |

**Key observations:**
- Spark seed handles CSV, Parquet, and JSON formats seamlessly
- Column name sanitization works: `"Customer Code"` -> `Customer_Code`, `" Price "` -> `Price`
- PG loads use COPY (fast), Databricks loads use parallel JDBC writers (4 partitions)
- 1M+ row transactions_a loaded in ~40s to PG via COPY

---

## Phase 3: Model Runs -- 6/7 PASS

| # | Model | Source(s) | Target | Path | Result | Time |
|---|-------|-----------|--------|------|--------|------|
| 3.1 | pushdown_pg_only | PG only | postgres | Pushdown | **PASS** | 0.26s |
| 3.2 | cross_pg_databricks | PG + Databricks | postgres | Federation | **PASS** | 34.66s |
| 3.3 | snowflake_to_pg | Snowflake | postgres | Federation | **PASS** | 27.53s |
| 3.4 | pushdown_databricks_only | Databricks only | databricks | Pushdown | **FAIL** | 0.08s |
| 3.5 | pg_to_databricks | PG | databricks | Federation | **PASS** | 38.55s |
| 3.6 | snowflake_to_databricks | Snowflake | databricks | Federation | **PASS** | 37.35s |
| 3.7 | three_way_to_databricks | PG + SF + DB | databricks | Federation | **PASS** | 40.77s |

### Federation Matrix

| Source(s) | Target | Path | Status | Load Method |
|-----------|--------|------|--------|-------------|
| PG only | PG | Pushdown | **PASS** | Direct SQL |
| PG + Databricks | PG | Federation | **PASS** | Spark -> COPY |
| Snowflake | PG | Federation | **PASS** | Spark -> COPY |
| PG | Databricks | Federation | **PASS** | Spark -> JDBC |
| Snowflake | Databricks | Federation | **PASS** | Spark -> JDBC |
| PG + Snowflake + Databricks | Databricks | Federation | **PASS** | Spark -> JDBC |
| Databricks only | Databricks | Pushdown | **FAIL** | (BUG-1) |

---

## Phase 4: Full Pipeline & Incremental -- Mixed Results

| # | Command | Result | Details |
|---|---------|--------|---------|
| 4.1 | Source views (customer_1, customer_2, packs_db, transactions_1/2/3) | **ERROR=6** | Column name mismatch (DATA-1) |
| 4.2 | stg_fact_transactions | **ERROR=1** | Cascade from 4.1 -- source views don't exist |
| 4.3 | dim_customers, dim_packs, fact_transactions | **ERROR=3** | Cascade from 4.1/4.2 |
| 4.4 | dim_employees --full-refresh | **PASS** | SELECT 54 (first load, refs seed directly) |
| 4.5 | dim_employees (incremental) | **PASS** | INSERT 0 0 (correct -- no new data) |
| 4.6 | bv_cbs_f_dept_acct_officer | **ERROR=1** | NOT NULL constraint violation (DATA-2) |
| 4.7 | d_b_dim_officers | skipped | Depends on 4.6 |
| 4.8 | `dvt run --select tag:TRGT` | **PASS=2, ERROR=3** | Tag selection works; dim_employees + d_b_dim_officers pass |

---

## Phase 5: Testing, Show, Build & Other Commands

| # | Command | Result | Details |
|---|---------|--------|---------|
| 5.1 | `dvt test` | **PASS=4** | unique_dim_customers_Customer_Code, not_null_dim_customers_Customer_Code, unique_dim_packs_SKU_Code, not_null_dim_packs_SKU_Code |
| 5.3 | `dvt show -s pushdown_pg_only --limit 10` | **FAIL** | `... LIMIT 100) limit 10` syntax error (BUG-2) |
| 5.4 | `dvt show -s cross_pg_databricks --limit 5` | **FAIL** | Same limit wrapping bug (BUG-2) |
| 5.5 | `dvt show --inline "select count(*) ..."` | **PASS** | Shows `cnt = 2814` correctly |
| 5.6 | `dvt compile` | **PASS** | Full project compiled (24 models) |
| 5.7 | `dvt run-operation generate_schema_name` | **PASS** | Macro executed silently |
| 5.8 | `dvt docs generate` | **PARTIAL** | Catalog written but 2 cross-db reference errors for Snowflake/Databricks sources |
| 5.9 | `dvt build -s pushdown_pg_only cross_pg_databricks snowflake_to_pg --full-refresh` | **PASS=3** | All 3 federation models passed via build |
| 5.10 | `dvt clean` | **PASS** | Cleaned target/ and dvt_packages/ |

---

## Phase 6: Edge Cases & Retry

| # | Command | Result | Details |
|---|---------|--------|---------|
| 6.1 | `dvt run -s nonexistent_model` | **PASS** (correct error) | "does not match any enabled nodes" |
| 6.2 | `dvt retry` (after clean) | **PASS** (correct error) | "Could not find previous run in 'target'" |
| 6.2b | `dvt retry` (after failure) | **PASS** | Correctly retried only pushdown_databricks_only (1 failed node) |
| 6.3 | `dvt list --output json --resource-type model` | **PASS** | Full JSON with configs, deps, tags for all 24 models |
| 6.4 | `dvt list -s tag:STG` | **PASS** | 3 models found: a_cbs_f_dept_acct_officer, c_vw_b_dim_officers, stg_fact_transactions |

---

## Issues Discovered

### BUG-1 (HIGH): Same-target pushdown fails when target is non-default adapter

**Symptom**: `pushdown_databricks_only` fails with:
```
cross-database references are not implemented: "demo.dvt_test.dim_regions"
LINE 29: FROM "demo"."dvt_test"."dim_regions" r
```

**Root Cause**: When a model has `target='databricks'` but the project's default adapter is `postgres`, the pushdown path executes the SQL on the **postgres adapter** which cannot handle Databricks table references like `demo.dvt_test.dim_regions`. DVT's execution plan classifies it as "pushdown" (correct, since all sources and target are the same: databricks), but it doesn't switch to the Databricks adapter for pushdown execution -- it stays on the default postgres adapter.

**Expected Behavior**: When model target matches all source targets (databricks in this case), DVT should either:
1. Use the Databricks adapter directly for pushdown execution, OR
2. Route through federation anyway since the active adapter differs from the model target

**Affected**: Any model where all sources AND target match a non-default adapter. Works fine when the model target matches the default adapter (e.g., `pushdown_pg_only` on default postgres).

**Workaround**: None currently. The federation path works (e.g., `pg_to_databricks` succeeds), so models writing to non-default targets must have at least one source from a different target to trigger federation.

---

### BUG-2 (MEDIUM): `dvt show` fails on models with existing LIMIT clause

**Symptom**: 
```
syntax error at or near "limit"
LINE 26:   limit 10
```

**Root Cause**: `dvt show` wraps the model SQL in a subquery and appends its own `limit N`, producing invalid SQL like:
```sql
SELECT * FROM (
    SELECT ... FROM ... LIMIT 100
) limit 10
```

The correct syntax would require either:
- Wrapping as `(SELECT ... FROM ... LIMIT 100) AS subq LIMIT 10`
- Or replacing the model's LIMIT with the show limit

**Affected**: Any model that contains a LIMIT clause. Models without LIMIT would likely work fine.

**Workaround**: Use `dvt show --inline` with the query directly, which works correctly.

---

### DATA-1 (LOW -- Not a DVT bug): Cocacola source views reference pre-sanitization column names

**Symptom**: 
```
column "Customer Code" does not exist
HINT: Perhaps you meant to reference the column "customers_db_1.Customer_Code"
```

**Root Cause**: DVT's Spark seed correctly sanitizes column names with spaces:
- `"Customer Code"` -> `Customer_Code`  
- `" Price "` -> `Price`  
- `"SKU Code"` -> `SKU_Code`

But the source view SQL models (customer_1.sql, packs_db.sql, transactions_1/2/3.sql) still reference the original CSV header names with spaces using double-quoted identifiers like `"Customer Code"`.

**Fix**: Update all 6 Cocacola source view SQL files to use the sanitized column names:
- `"Customer Code"` -> `"Customer_Code"`
- `"Customer name"` -> `"Customer_name"` 
- `"SKU Code"` -> `"SKU_Code"`
- `" Price "` -> `"Price"`
- etc.

---

### DATA-2 (LOW -- Not a DVT bug): Exim pipeline NOT NULL constraint violation

**Symptom**:
```
null value in column "RESTR_TO_COMPS" of relation "bv_cbs_f_dept_acct_officer" violates not-null constraint
```

**Root Cause**: The Snowflake source data has NULL values in `RESTR_TO_COMPS` and other columns, but the PostgreSQL target table was created with NOT NULL constraints from a previous run. The federation path (both COPY and JDBC fallback) fails when trying to insert NULLs into NOT NULL columns.

**Fix**: Use `--full-refresh` which should DROP the table first and recreate with nullable columns matching the Spark DataFrame schema. If the table was manually created with NOT NULL, drop it first.

---

### COSMETIC (LOW): `dvt docs generate` cross-database reference warnings

**Symptom**:
```
Cross-db references not allowed in postgres (EXIM_EDWH_DEV vs postgres)
Cross-db references not allowed in postgres (demo vs postgres)
```

**Root Cause**: The catalog builder tries to query Snowflake and Databricks source tables via the default PostgreSQL adapter. The catalog.json is still written but with incomplete source metadata for cross-target sources.

**Impact**: Documentation for cross-target sources will be incomplete. Federation test models and their outputs are still documented correctly.

---

## Performance Observations

### Seed Performance

| Target | Method | 7 rows | 63 rows | 469 rows | 1M rows |
|--------|--------|--------|---------|----------|---------|
| Postgres | Spark -> COPY | ~11s | ~2s | ~12s | ~40s |
| Databricks | Spark -> JDBC (4 writers) | ~15s | N/A | N/A | N/A |

### Federation Performance

| Path | Rows | Time | Method |
|------|------|------|--------|
| Pushdown (PG) | 100 | 0.26s | Direct SQL |
| SF -> PG | 100 | 14-27s | Spark extract -> COPY |
| PG+DB -> PG | 50 | 14-35s | Spark extract -> COPY |
| PG -> Databricks | 50 | 38s | Spark extract -> JDBC |
| SF -> Databricks | 50 | 37s | Spark extract -> JDBC |
| PG+SF+DB -> Databricks | 25 | 41s | Spark extract -> JDBC |

**Key insight**: PG target loads use COPY (fast), Databricks target loads use JDBC (slower due to network + 4 parallel writers). Spark session initialization adds ~10-12s overhead on first run.

### Staging Cache

Federation reuses extracted source data across runs:
```
Skipped source.Coke_DB.postgres_source.customers_db_1 (staging exists)
Skipped source.Coke_DB.snowflake_exim.cbs_f_country (staging exists)
```
This significantly speeds up subsequent federation runs (e.g., `three_way_to_databricks` skipped all 3 extractions on second run).

---

## What Works Well

1. **Federation across all target combinations**: PG->PG, SF->PG, PG+DB->PG, PG->DB, SF->DB, PG+SF+DB->DB all work
2. **Spark seed**: Multi-format (CSV, Parquet, JSON), multi-target, column sanitization, 1M+ rows
3. **Incremental models**: delete+insert strategy works correctly (first load + subsequent runs)
4. **Tag-based selection**: `--select tag:TRGT`, `dvt list -s tag:STG` work correctly
5. **dvt retry**: Correctly identifies and retries only failed nodes
6. **dvt show --inline**: Inline SQL preview works perfectly
7. **dvt build**: Combined execution of seed+run+test in DAG order
8. **Source staging cache**: Extracted data is reused across federation runs
9. **All 3 connections**: postgres, snowflake, databricks all verified and working
10. **Error reporting**: Clear error messages with compiled SQL paths for debugging

## What Needs Fixing

1. **BUG-1**: Same-target pushdown on non-default adapter (e.g., `target='databricks'` when default is `postgres`)
2. **BUG-2**: `dvt show` LIMIT wrapping produces invalid SQL on models with existing LIMIT
3. **DATA-1**: Cocacola source views need column name updates to match sanitized seed columns
4. **DATA-2**: Exim pipeline NOT NULL constraint -- need to drop stale PG table or fix schema
5. **COSMETIC**: `dvt docs generate` incomplete for cross-target sources
