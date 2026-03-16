# Skill: uat-e2e

Comprehensive UAT (User Acceptance Test) E2E test for DVT across all CLI commands, targets, and incremental strategies. This is the final quality gate before shipping.

## Usage

- `/uat-e2e` - Run full UAT on the current trial (defaults to trial_16)
- `/uat-e2e trial_16` - Run on a specific trial

## Overview

This UAT validates every DVT CLI command against real databases (PostgreSQL, Databricks, Snowflake read-only, Snowflake writable) with 3 consecutive runs to exercise all incremental model states. It produces a detailed report with a ship/no-ship recommendation.

## Prerequisites

- Trial project exists in `~/Documents/My_Projects/DVT/Testing_Factory/Testing_Playground/trial_<N>/Coke_DB/`
- Trial uses pyproject.toml with uv sources pointing to dvt-ce and dvt-adapters
- PostgreSQL running locally (port 5433)
- Databricks workspace accessible (demo catalog, dvt_test schema)
- Snowflake EXIM accessible (EXIM_EDWH_DEV, read-only via `sf_dev`)
- Snowflake Coke_DB accessible (Coke_DB, writable via `disf_dev`, warehouse COMPUTE_WH)
- DVT installed: `cd core && uv sync`
- profiles.yml configured at `~/.dvt/profiles.yml` with targets: `pg_dev`, `dbx_dev`, `sf_dev`, `disf_dev`
- Sling installed and accessible (`sling --version`)
- DuckDB available as Python package

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
time uv run --project <dvt-ce>/core dvt debug
```

**Expected**: All 4 targets show OK: pg_dev, dbx_dev, sf_dev, disf_dev.
**Key check**: sf_dev and disf_dev must BOTH show OK (tests the adapter reset fix for same-type targets).

### Phase 1: Clean Slate

```bash
# 1a. dvt clean
time uv run --project <dvt-ce>/core dvt clean

# Verify: target/ and dvt_packages/ directories removed
```

**Expected**: PASS. No errors. target/ and dvt_packages/ removed.

### Phase 2: Dependencies

```bash
# 2a. dvt deps
time uv run --project <dvt-ce>/core dvt deps
```

**Expected**: PASS. dvt_packages/ populated.

### Phase 3: Seed -- All Targets, All Sizes, Timed

Seeds load raw data into target databases. Test EVERY seed on EVERY applicable target.

```bash
# 3a. Seed to PostgreSQL (all seeds)
time uv run --project <dvt-ce>/core dvt seed --target pg_dev

# 3b. Seed to Databricks (all seeds)
time uv run --project <dvt-ce>/core dvt seed --target dbx_dev

# 3c. Seed to Databricks with --full-refresh
time uv run --project <dvt-ce>/core dvt seed --target dbx_dev --full-refresh

# 3d. Seed to Snowflake disf_dev (all seeds)
time uv run --project <dvt-ce>/core dvt seed --target disf_dev

# 3e. Seed to Snowflake disf_dev with --full-refresh
time uv run --project <dvt-ce>/core dvt seed --target disf_dev --full-refresh
```

### Phase 4: Compile & Show -- Multi-Target Including Snowflake

Test compilation without execution. Validate SQL generation for all dialects.

```bash
# 4a. Compile specific models for each target dialect
uv run --project <dvt-ce>/core dvt compile -s pushdown_pg_only
uv run --project <dvt-ce>/core dvt compile -s pushdown_databricks_only

# 4b. dvt show (read-only query)
uv run --project <dvt-ce>/core dvt show -s pushdown_pg_only --limit 5

# 4c. Compile the full project
time uv run --project <dvt-ce>/core dvt compile
```

**Expected**: Each compile produces valid SQL in target/compiled/. Show returns preview rows.

### Phase 5: Run 1 -- Full Refresh (--full-refresh)

This is the first run. All models get DROP + CREATE + INSERT. Incremental models behave as tables.

```bash
# 5a. Full refresh all models
time uv run --project <dvt-ce>/core dvt run --full-refresh
```

**Verification after Run 1**:
1. Check model data values match expectations
2. Check no residual staging tables (`_dvt_staging_*`) left in target databases
3. Check DuckDB cache at `~/.dvt/cache/` has extraction data for federation sources

### Phase 6: Run 2 -- Incremental (default, no flags)

This is the second run. Table models get TRUNCATE + INSERT. Incremental models exercise their `is_incremental()` branches.

```bash
# 6a. Default run (incremental)
time uv run --project <dvt-ce>/core dvt run
```

**Verification after Run 2**:
1. Table models: re-created with same data (TRUNCATE + INSERT)
2. Incremental MERGE models: data VALUES changed (proves incremental branch ran)
3. Incremental APPEND models: verify correct behavior
4. No residual staging tables left

### Phase 7: Run 3 -- Incremental Idempotency

Third run proves incremental models are stable and don't corrupt data on repeated execution.

```bash
# 7a. Another incremental run
time uv run --project <dvt-ce>/core dvt run
```

**Verification after Run 3**:
1. MERGE models: same data as Run 2 (idempotent -- same keys, same values)
2. APPEND models: no duplicate rows
3. Table models: identical to Run 2
4. No residual staging tables

### Phase 8: --target Flag Tests

Test the `--target` CLI flag to redirect model output to different targets.

```bash
# 8a. Run a PG-targeted model with --target dbx_dev (should use federation via Sling+DuckDB)
time uv run --project <dvt-ce>/core dvt run -s pushdown_pg_only --target dbx_dev

# 8b. Run a DBX-targeted model with --target pg_dev (should use federation via Sling+DuckDB)
time uv run --project <dvt-ce>/core dvt run -s pushdown_databricks_only --target pg_dev
```

**Expected**: Models execute with target override. Federation path (Sling extraction -> DuckDB -> Sling load) activates when source != target.

### Phase 9: Data Integrity Verification

After all runs, verify data in target databases via SQL queries against PG, Databricks, and Snowflake to confirm row counts and data values match expectations.

### Phase 10: Extraction & DuckDB Cache Verification

Check the local DuckDB cache to verify extraction optimization.

```bash
# List cache contents
ls -la ~/.dvt/cache/

# Verify DuckDB cache has extracted tables
python3 -c "
import duckdb
conn = duckdb.connect('~/.dvt/cache/dvt_cache.duckdb', read_only=True)
print(conn.execute('SHOW TABLES').fetchall())
conn.close()
"
```

**Verify**:
1. Column pruning: extracted tables should have ONLY the columns needed by their consuming models
2. Predicate pushdown: extraction queries should include WHERE clauses where applicable
3. Sling extraction logs show correct source connections

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
(table showing data values after each of the 3 runs)

## Extraction & DuckDB Cache Verification
(column pruning, predicate pushdown evidence, Sling extraction logs)

## Residual Table Check
(confirmation no _dvt_staging_* tables left in PG, DBX, and SF)

## dbt Compatibility Assessment
(which native dbt commands work, which differ, gaps)

## Known Issues
(list any failures with root cause)

## Recommendation
(detailed ship/no-ship with reasoning)
```
