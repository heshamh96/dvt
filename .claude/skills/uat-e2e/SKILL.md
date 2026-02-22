# Skill: uat-e2e

Comprehensive UAT (User Acceptance Test) E2E test for DVT across all CLI commands, targets, and incremental strategies. This is the final quality gate before shipping.

## Usage

- `/uat-e2e` - Run full UAT on the current trial (defaults to trial_16)
- `/uat-e2e trial_16` - Run on a specific trial

## Overview

This UAT validates every DVT CLI command against real databases (PostgreSQL, Databricks, Snowflake read-only, Snowflake writable) with 3 consecutive runs to exercise all incremental model states. It produces a detailed report with a ship/no-ship recommendation.

## Prerequisites

- Trial project exists in `~/Documents/My_Projects/DVT/Testing_Playground/trial_<N>_full_cli_e2e/Coke_DB/`
- PostgreSQL running locally (port 5433)
- Databricks workspace accessible (demo catalog, dvt_test schema)
- Snowflake EXIM accessible (EXIM_EDWH_DEV, read-only via `sf_dev`)
- Snowflake Coke_DB accessible (Coke_DB, writable via `disf_dev`, warehouse COMPUTE_WH)
- DVT installed: `cd core && uv sync`
- profiles.yml configured at `~/.dvt/profiles.yml` with targets: `pg_dev`, `dbx_dev`, `sf_dev`, `disf_dev`
- computes.yml configured at `~/.dvt/computes.yml` with `local_spark`

### Target Access Rules

| Target | Type | Access | Usage |
|--------|------|--------|-------|
| `pg_dev` | postgres | read/write | Default target, seeds, models |
| `dbx_dev` | databricks | read/write | Seeds, models, catalog=demo, schema=dvt_test |
| `sf_dev` | snowflake | **READ ONLY** | Source data (EXIM_EDWH_DEV), compile/show only |
| `disf_dev` | snowflake | read/write | Seeds, models, database=Coke_DB, schema=public |

**CRITICAL: Never write to `sf_dev`. Only `disf_dev` is the writable Snowflake target.**

## Test Phases

Execute phases sequentially. Record PASS/FAIL, timing, and row counts for every step.

### Phase 0: Debug (Connection Verification)

```bash
cd <trial_project_dir>

# 0a. dvt debug -- tests all targets
time uv run --project <dvt-core>/core dvt debug
```

**Expected**: All 4 targets show `✓ OK`: pg_dev, dbx_dev, sf_dev, disf_dev.
**Key check**: sf_dev and disf_dev must BOTH show OK (tests the adapter reset fix for same-type targets).

### Phase 1: Clean Slate

```bash
# 1a. dvt clean
time uv run --project <dvt-core>/core dvt clean

# Verify: target/ and dvt_packages/ directories removed
# Verify: .dvt/staging/ does NOT exist (or was cleared manually before test)
```

**Expected**: PASS. No errors. target/ and dvt_packages/ removed.

### Phase 2: Dependencies

```bash
# 2a. dvt deps
time uv run --project <dvt-core>/core dvt deps
```

**Expected**: PASS. dvt_packages/ populated.

### Phase 3: Seed -- All Targets, All Sizes, Timed

Seeds load raw data into target databases. Test EVERY seed on EVERY applicable target.

```bash
# 3a. Seed to PostgreSQL (all seeds)
time uv run --project <dvt-core>/core dvt seed --target pg_dev

# 3b. Seed to Databricks (all seeds)
time uv run --project <dvt-core>/core dvt seed --target dbx_dev

# 3c. Seed to Databricks with --full-refresh
time uv run --project <dvt-core>/core dvt seed --target dbx_dev --full-refresh

# 3d. Seed to Snowflake disf_dev (all seeds)
time uv run --project <dvt-core>/core dvt seed --target disf_dev

# 3e. Seed to Snowflake disf_dev with --full-refresh
time uv run --project <dvt-core>/core dvt seed --target disf_dev --full-refresh
```

**Record for each target**:
| Seed | Format | ~Rows | Status | Time (s) |
|------|--------|-------|--------|----------|
| transactions_a | CSV | ~1M | ? | ? |
| transactions_b | CSV | ~1M | ? | ? |
| transactions_c | CSV | ~1M | ? | ? |
| customers_db_1 | CSV | ~918 | ? | ? |
| customers_db_2 | CSV | 147 | ? | ? |
| packs | CSV | 64 | ? | ? |
| employees | CSV | 9 | ? | ? |
| dim_files | CSV | 3 | ? | ? |
| employees_json | JSON | 9 | ? | ? |
| dim_regions | CSV | 5 | ? | ? |
| dim_categories | CSV | 7 | ? | ? |
| customers_parquet | Parquet | ~918 | ? | ? |
| packs_parquet | Parquet | 64 | ? | ? |

**Verification**: After seeding, query target databases to confirm row counts match.

**Note on big seeds**: transactions_a/b/c (~1M rows each) are likely already seeded to pg_dev and dbx_dev from prior runs. Only re-seed if needed. Seeding to disf_dev is new and will take ~110s each.

### Phase 4: Compile & Show -- Multi-Target Including Snowflake

Test compilation without execution. Validate SQL generation for all dialects.

```bash
# 4a. Compile specific models for each target dialect
uv run --project <dvt-core>/core dvt compile -s pushdown_pg_only
uv run --project <dvt-core>/core dvt compile -s pushdown_databricks_only
uv run --project <dvt-core>/core dvt compile -s snowflake_to_pg
uv run --project <dvt-core>/core dvt compile -s pg_to_databricks
uv run --project <dvt-core>/core dvt compile -s three_way_to_databricks
uv run --project <dvt-core>/core dvt compile -s pg_to_snowflake
uv run --project <dvt-core>/core dvt compile -s three_way_to_snowflake

# 4b. dvt show (read-only query, good for Snowflake)
uv run --project <dvt-core>/core dvt show -s snowflake_to_pg --limit 5
uv run --project <dvt-core>/core dvt show -s pushdown_pg_only --limit 5

# 4c. Compile the full project (may be slow due to multi-adapter init)
time uv run --project <dvt-core>/core dvt compile
```

**Expected**: Each compile produces valid SQL in target/compiled/. Show returns preview rows.

### Phase 5: Run 1 -- Full Refresh (--full-refresh)

This is the first run. All models get DROP + CREATE + INSERT. Incremental models behave as tables.

```bash
# 5a. Full refresh all models
time uv run --project <dvt-core>/core dvt run --full-refresh
```

**Record for each model**:
| Model | Target | Path | Strategy | Rows | Status | Time (s) |
|-------|--------|------|----------|------|--------|----------|
| pushdown_pg_only | pg_dev | pushdown | - | 100 | ? | ? |
| pushdown_databricks_only | dbx_dev | pushdown | - | 35 | ? | ? |
| cross_pg_databricks | pg_dev | federation | - | 50 | ? | ? |
| pg_to_databricks | dbx_dev | federation | - | 50 | ? | ? |
| snowflake_to_pg | pg_dev | federation | - | 100 | ? | ? |
| snowflake_to_databricks | dbx_dev | federation | - | 50 | ? | ? |
| three_way_to_databricks | dbx_dev | federation | - | 25 | ? | ? |
| **pg_to_snowflake** | **disf_dev** | federation | - | 50 | ? | ? |
| **databricks_to_snowflake** | **disf_dev** | federation | - | 35 | ? | ? |
| **three_way_to_snowflake** | **disf_dev** | federation | - | 25 | ? | ? |
| transactions_pg_to_dbx | dbx_dev | federation | - | 100K | ? | ? |
| transactions_dbx_to_pg | pg_dev | federation | - | ~100K | ? | ? |
| incremental_limit_test | dbx_dev | federation | append | 50 | ? | ? |
| incremental_antijoin_test | dbx_dev | federation | append | 10 | ? | ? |
| incremental_merge_test | dbx_dev | federation | merge | 20 | ? | ? |
| incremental_delete_insert_test | dbx_dev | federation | del+ins | 20 | ? | ? |
| incr_merge_to_pg | pg_dev | federation | merge | 25 | ? | ? |
| incr_merge_to_dbx | dbx_dev | federation | merge | 25 | ? | ? |
| **incr_merge_to_sf** | **disf_dev** | federation | merge | 25 | ? | ? |
| incr_delete_insert_to_pg | pg_dev | federation | del+ins | 25 | ? | ? |
| incr_delete_insert_to_dbx | dbx_dev | federation | del+ins | 25 | ? | ? |
| **incr_delete_insert_to_sf** | **disf_dev** | federation | del+ins | 25 | ? | ? |
| incr_append_to_pg | pg_dev | federation | append | 25 | ? | ? |
| incr_append_to_dbx | dbx_dev | federation | append | 25 | ? | ? |
| **incr_append_to_sf** | **disf_dev** | federation | append | 25 | ? | ? |
| (Target layer models) | pg_dev | pushdown | - | varies | ? | ? |
| (Other_Source models) | pg_dev | federation | - | varies | ? | ? |

**Verification after Run 1**:
1. Check incremental model data values match full-refresh branch:
   - `incr_merge_to_pg`: all rows have region_name='North America', batch_label='full_refresh'
   - `incr_merge_to_dbx`: all rows have region_name='Asia Pacific', batch_label='full_refresh'
   - `incr_merge_to_sf`: all rows have region_name='Asia Pacific', batch_label='full_refresh'
   - `incr_delete_insert_to_pg`: all rows have category_name='Beverages', batch_label='full_refresh'
   - `incr_delete_insert_to_dbx`: all rows have category_name='Energy Drinks', batch_label='full_refresh'
   - `incr_delete_insert_to_sf`: all rows have category_name='Beverages', batch_label='full_refresh'
   - `incr_append_to_pg`: 25 rows, batch_label='full_refresh'
   - `incr_append_to_dbx`: 25 rows, batch_label='full_refresh'
   - `incr_append_to_sf`: 25 rows, batch_label='full_refresh'
2. Check no residual staging tables (`_dvt_staging_*`) left in target databases
3. Check .dvt/staging/ Delta tables exist for federation sources

### Phase 6: Run 2 -- Incremental (default, no flags)

This is the second run. Table models get TRUNCATE + INSERT. Incremental models exercise their `is_incremental()` branches.

```bash
# 6a. Default run (incremental)
time uv run --project <dvt-core>/core dvt run
```

**Verification after Run 2**:
1. Table models: re-created with same data (TRUNCATE + INSERT)
2. Incremental MERGE models: data VALUES changed (proves incremental branch ran):
   - `incr_merge_to_pg`: region_name changed to 'Europe', batch_label='incremental', count=25
   - `incr_merge_to_dbx`: region_name changed to 'Latin America', batch_label='incremental', count=25
   - `incr_merge_to_sf`: region_name changed to 'Middle East & Africa', batch_label='incremental', count=25
3. Incremental DELETE+INSERT models: data VALUES changed:
   - `incr_delete_insert_to_pg`: category_name changed to 'Soft Drinks', batch_label='incremental', count=25
   - `incr_delete_insert_to_dbx`: category_name changed to 'Water', batch_label='incremental', count=25
   - `incr_delete_insert_to_sf`: category_name changed to 'Energy Drinks', batch_label='incremental', count=25
4. Incremental APPEND models: NO new rows added (NOT IN filter catches all):
   - `incr_append_to_pg`: still 25 rows, batch_label='full_refresh' (no new rows)
   - `incr_append_to_dbx`: still 25 rows, batch_label='full_refresh' (no new rows)
   - `incr_append_to_sf`: still 25 rows, batch_label='full_refresh' (no new rows)
5. `incremental_limit_test`: Quantity filter > MAX(quantity) -> likely 0 new rows (range 1-6)
6. No residual staging tables left

### Phase 7: Run 3 -- Incremental Idempotency

Third run proves incremental models are stable and don't corrupt data on repeated execution.

```bash
# 7a. Another incremental run
time uv run --project <dvt-core>/core dvt run
```

**Verification after Run 3**:
1. MERGE models: same data as Run 2 (idempotent -- same keys, same values)
   - `incr_merge_to_pg`: still 25 rows, region_name='Europe', batch_label='incremental'
   - `incr_merge_to_dbx`: still 25 rows, region_name='Latin America', batch_label='incremental'
   - `incr_merge_to_sf`: still 25 rows, region_name='Middle East & Africa', batch_label='incremental'
2. DELETE+INSERT models: same data as Run 2 (idempotent)
   - `incr_delete_insert_to_sf`: still 25 rows, category_name='Energy Drinks', batch_label='incremental'
3. APPEND models: still 25 rows (NOT IN filter prevents duplicates)
   - `incr_append_to_sf`: still 25 rows
4. Table models: identical to Run 2
5. No residual staging tables

### Phase 8: --target Flag Tests

Test the `--target` CLI flag to redirect model output to different targets.

```bash
# 8a. Run a PG-targeted model with --target dbx_dev (should use federation)
time uv run --project <dvt-core>/core dvt run -s pushdown_pg_only --target dbx_dev

# 8b. Run a DBX-targeted model with --target pg_dev (should use federation)
time uv run --project <dvt-core>/core dvt run -s pushdown_databricks_only --target pg_dev

# 8c. Run a specific federation model with --full-refresh
time uv run --project <dvt-core>/core dvt run -s snowflake_to_pg --full-refresh

# 8d. Run a PG-targeted model with --target disf_dev (PG → Snowflake override)
time uv run --project <dvt-core>/core dvt run -s pushdown_pg_only --target disf_dev --full-refresh

# 8e. Run a DBX-targeted model with --target disf_dev (DBX → Snowflake override)
time uv run --project <dvt-core>/core dvt run -s pushdown_databricks_only --target disf_dev --full-refresh
```

**Expected**: Models execute with target override. Federation path activates when source != target.

### Phase 9: Data Integrity Verification

After all runs, verify data in target databases.

**PostgreSQL verification** (via psql or adapter):
```sql
-- Seed counts
SELECT count(*) FROM public.transactions_a;  -- ~1M
SELECT count(*) FROM public.customers_db_1;  -- ~918
SELECT count(*) FROM public.packs;           -- 64

-- Model counts
SELECT count(*) FROM public.pushdown_pg_only;      -- 100
SELECT count(*) FROM public.cross_pg_databricks;    -- 50
SELECT count(*) FROM public.snowflake_to_pg;        -- 100
SELECT count(*) FROM public.incr_merge_to_pg;       -- 25
SELECT count(*) FROM public.incr_delete_insert_to_pg; -- 25
SELECT count(*) FROM public.incr_append_to_pg;      -- 25

-- Incremental data values
SELECT DISTINCT region_name, batch_label FROM public.incr_merge_to_pg;
-- Expected: Europe, incremental
SELECT DISTINCT category_name, batch_label FROM public.incr_delete_insert_to_pg;
-- Expected: Soft Drinks, incremental

-- No residual staging tables
SELECT tablename FROM pg_tables WHERE tablename LIKE '_dvt_staging_%';
-- Expected: empty
```

**Databricks verification** (via adapter or JDBC):
```sql
-- Model counts
SELECT count(*) FROM dvt_test.pushdown_databricks_only;   -- 35
SELECT count(*) FROM dvt_test.pg_to_databricks;            -- 50
SELECT count(*) FROM dvt_test.transactions_pg_to_dbx;      -- 100000
SELECT count(*) FROM dvt_test.incr_merge_to_dbx;           -- 25

-- Incremental data values
SELECT DISTINCT region_name, batch_label FROM dvt_test.incr_merge_to_dbx;
-- Expected: Latin America, incremental
SELECT DISTINCT category_name, batch_label FROM dvt_test.incr_delete_insert_to_dbx;
-- Expected: Water, incremental

-- No residual staging tables
SHOW TABLES IN dvt_test LIKE '_dvt_staging_*';
-- Expected: empty
```

**Snowflake disf_dev verification** (via Snowflake UI or adapter):
```sql
-- Table model counts
SELECT count(*) FROM public.pg_to_snowflake;           -- 50
SELECT count(*) FROM public.databricks_to_snowflake;    -- 35
SELECT count(*) FROM public.three_way_to_snowflake;     -- 25

-- Incremental model counts
SELECT count(*) FROM public.incr_merge_to_sf;           -- 25
SELECT count(*) FROM public.incr_delete_insert_to_sf;   -- 25
SELECT count(*) FROM public.incr_append_to_sf;          -- 25

-- Incremental data values
SELECT DISTINCT region_name, batch_label FROM public.incr_merge_to_sf;
-- Expected: Middle East & Africa, incremental
SELECT DISTINCT category_name, batch_label FROM public.incr_delete_insert_to_sf;
-- Expected: Energy Drinks, incremental

-- No residual staging tables
SHOW TABLES LIKE '_dvt_staging_%' IN SCHEMA public;
-- Expected: empty
```

### Phase 10: Staging & Pushdown Verification

Check the local .dvt/staging/ directory to verify extraction optimization.

```bash
# List all staging directories
ls -la .dvt/staging/

# For each source staging, check column count (proves column pruning)
# Example: check transactions_a extraction columns
python3 -c "
from delta import DeltaTable
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName('verify').config('spark.jars.packages', 'io.delta:delta-spark_2.13:4.0.1').getOrCreate()
dt = DeltaTable.forPath(spark, '.dvt/staging/source.Coke_DB.postgres_source.transactions_a.delta')
print('Columns:', dt.toDF().columns)
print('Row count:', dt.toDF().count())
spark.stop()
"
```

**Verify**:
1. Column pruning: staging tables should have ONLY the columns needed by their consuming models, NOT all source columns (unless SELECT * is used)
2. Predicate pushdown: check extraction SQL in state files for WHERE clauses
3. LIMIT pushdown: check extraction SQL for LIMIT clauses

### Phase 10.5: Incremental Staging Trace Test

This phase exercises the full incremental Delta staging flow with **traceable data** to prove that `{{ this }}` resolution, NOT IN anti-join, Delta append mode, source re-extraction, and predicate pushdown safety all work correctly together.

The test uses a **modified append model** with small, deterministic row sets so you can verify exact customer codes at each step.

#### Step 1: Modify the test model

Pick an existing append model (e.g., `incr_append_to_pg.sql`). **Save the original** for restoration later. Replace its content with a trace-test version:

```sql
{{
    config(
        materialized='incremental',
        incremental_strategy='append',
        target='pg_dev'
    )
}}

/*
    STAGING TRACE TEST — APPEND strategy → PostgreSQL
    Run 1 (--full-refresh): LIMIT 5 ORDER BY ASC → first 5 customers, batch='full_refresh'
    Run 2 (incremental):    LIMIT 10 ORDER BY ASC → NOT IN {{ this }} filters 5 already loaded
                            → appends only NEW rows, batch='incremental'
    Expected: PG target = 5+N rows, model staging Delta = 5+N rows (2 commits)
*/

{% if is_incremental() %}

SELECT
    c."Customer Code" as customer_code,
    c."Customer name" as customer_name,
    r.region_name,
    'incremental' as batch_label
FROM {{ source('postgres_source', 'customers_db_1') }} c
LEFT JOIN {{ source('databricks_source', 'dim_regions') }} r
    ON r.region_code = 'MEA'
WHERE c."Customer Code" NOT IN (SELECT customer_code FROM {{ this }})
ORDER BY c."Customer Code" ASC
LIMIT 10

{% else %}

SELECT
    c."Customer Code" as customer_code,
    c."Customer name" as customer_name,
    r.region_name,
    'full_refresh' as batch_label
FROM {{ source('postgres_source', 'customers_db_1') }} c
LEFT JOIN {{ source('databricks_source', 'dim_regions') }} r
    ON r.region_code = 'MEA'
ORDER BY c."Customer Code" ASC
LIMIT 5

{% endif %}
```

Key design choices:
- **LIMIT 5** on full-refresh (small, traceable set)
- **LIMIT 10** on incremental (larger set, NOT IN filter will exclude the 5 from Run 1)
- **ORDER BY ASC** makes customer codes deterministic and verifiable
- **batch_label** distinguishes full_refresh vs incremental rows

#### Step 2: Run 1 — Full Refresh

```bash
time uv run --project <dvt-core>/core dvt run --select incr_append_to_pg --full-refresh
```

**Verify Run 1**:

```bash
# 1. Check PG target: exactly 5 rows
PGPASSWORD=postgres psql -h localhost -p 5433 -U postgres -d postgres \
  -c "SELECT * FROM public.incr_append_to_pg ORDER BY customer_code;"
# Expected: 5 rows, all batch_label='full_refresh', customer codes Cs XXXX-Cs YYYY

# 2. Check model staging Delta exists and has 1 commit with 5 rows
cat .dvt/staging/model.Coke_DB.incr_append_to_pg.delta/_delta_log/00000000000000000000.json
# Look for: "numOutputRows":"5", "mode":"Append"
# Look for: min/max customer_code values matching PG
```

| Check | Expected |
|-------|----------|
| PG row count | 5 |
| PG batch_label | all 'full_refresh' |
| Delta commit 0 exists | yes |
| Delta commit 0 numOutputRows | 5 |
| Delta commit 0 mode | Append |

#### Step 3: Run 2 — Incremental

```bash
time uv run --project <dvt-core>/core dvt run --select incr_append_to_pg
```

**Watch the log output for these key lines**:
- `Registered { this } view: _dvt_XXXX__this (5 rows)` — model staging loaded from Delta
- `Optimizer: column pushdown on N/N sources, 0 predicates pushed` — NOT IN predicate NOT pushed (Bug 6 fix)
- `Extracting source.Coke_DB.postgres_source.customers_db_1...` — fresh extraction (Bug 5 fix)
- `Loaded N rows via JDBC` — new rows written to PG

**Verify Run 2**:

```bash
# 1. Check PG target: original 5 + new rows
PGPASSWORD=postgres psql -h localhost -p 5433 -U postgres -d postgres \
  -c "SELECT * FROM public.incr_append_to_pg ORDER BY customer_code;"
# Expected: 5 full_refresh + N incremental rows

# 2. Check Delta has 2 commits
cat .dvt/staging/model.Coke_DB.incr_append_to_pg.delta/_delta_log/00000000000000000001.json
# Look for: "numOutputRows":"N", "mode":"Append", batch_label min/max = "incremental"

# 3. Verify NO overlap: incremental customer codes must NOT appear in full_refresh set
PGPASSWORD=postgres psql -h localhost -p 5433 -U postgres -d postgres \
  -c "SELECT customer_code, batch_label FROM public.incr_append_to_pg ORDER BY customer_code;"
# Each customer_code should appear exactly once
```

| Check | Expected |
|-------|----------|
| PG total rows | 5 + N (N = new rows from incremental branch) |
| PG full_refresh rows | 5 (preserved from Run 1) |
| PG incremental rows | N (new, no overlap with full_refresh) |
| `{{ this }}` view loaded | 5 rows |
| Predicates pushed | 0 |
| Sources re-extracted | fresh (not stale) |
| Delta commit 1 exists | yes |
| Delta commit 1 mode | Append |
| No duplicate customer_codes | confirmed |

#### Step 4: Restore Original Model

After the trace test, restore the original `incr_append_to_pg.sql` content. **Make sure to include `incremental_strategy='append'`** in the config block (it may not have been in the original).

#### What This Phase Proves

1. **Source staging cleared between runs** — `before_run()` clears `source.*` entries, forcing fresh extraction
2. **Model staging preserved between runs** — `model.*` entries survive, enabling `{{ this }}` resolution
3. **`{{ this }}` resolves to Delta staging** — Spark temp view loaded with correct historical data
4. **NOT IN anti-join works** — existing customer codes excluded from incremental batch
5. **Predicate pushdown safety** — subqueries referencing Spark temp views are NOT pushed to remote DBs
6. **Delta append mode** — model staging accumulates rows across commits (correct for append strategy)
7. **Data integrity** — no duplicate rows, correct batch labels, deterministic ordering

## Report Template

Generate findings in `<trial_dir>/findings/uat_e2e_results.md` with:

```markdown
# DVT UAT E2E Results

**Date**: YYYY-MM-DD
**Branch**: `branch_name` (commit `hash`)
**Trial**: trial_N
**Targets**: pg_dev (Postgres), dbx_dev (Databricks), sf_dev (Snowflake read-only), disf_dev (Snowflake writable)

## Executive Summary
- Total tests: N
- Passed: N
- Failed: N
- **Ship recommendation**: YES/NO

## Phase Results
(tables for each phase with PASS/FAIL/time)

## Seed Performance
(table with all seeds x targets, row counts, times)

## Incremental Model Verification
(table showing data values after each of the 3 runs, including SF models)

## Optimizer Verification
(column pruning, predicate pushdown, LIMIT pushdown evidence)

## Residual Table Check
(confirmation no _dvt_staging_* tables left in PG, DBX, and SF)

## dbt Compatibility Assessment
(which native dbt commands work, which differ, gaps)

## Known Issues
(list any failures with root cause)

## Recommendation
(detailed ship/no-ship with reasoning)
```
