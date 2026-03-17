# DVT Implementation Plan — Shared Memory

> **Last Updated:** March 2026  
> **Status:** Living document — updated after each phase

---

## Foundation

- **dvt-ce**: dbt-core 1.9.10 renamed to `dvt-ce`. Provides `dbt.*` + `dvt.*`.
- **dvt-adapters**: 13 engine adapters in one package. Provides `dbt.adapters.*` + `dbt.include.*`.
- **Test projects:**
  - `trial_19_dvt_ce_pypi/Coke_DB` — original 12-model test (Docker DBs)
  - `trial_20_full_coverage/Coke_DB` — full 68-model test (Docker + remote SF/PG/DBX)
- **Both packages:** local editable via `pyproject.toml` + `[tool.uv.sources]`
- **Profiles:** `~/.dvt/profiles.yml` (DVT-first, `~/.dbt` fallback)

---

## COMPLETED

### Phase 0: Scaffold + Sync [DONE]
### Phase 1: Core Extraction Pipeline [DONE]
### Phase 2: Seeds, Debug, Show [DONE]
### Phase 3: Package Consolidation [DONE]
### Phase 4: Persistent DuckDB Cache + Incremental [DONE]
### Phase 5: Federation Optimizer [DONE]
### Phase 6: Non-Default Pushdown + Polish [DONE]
### Phase 7: DuckDB Connectivity [DONE]
### Phase 8.5: dvt show --select [DONE]

### E2E Test Results

**Trial 19 (scoped — 12 models):** 12/12 PASS (both run 1 + run 2)
**Trial 20 (full DAG — 68 models):** Many failures exposed (see Feedback section below)

---

## USER FEEDBACK (v1) — CRITICAL ITEMS

From `hesham_tests/Feedback_v1/design_and_branding_feedback.md`:

### F1: dvt --version (BRANDING)
**Problem:** Shows "dbt-core", "Your version of dbt-core is out of date", links to getdbt.com. Lists adapter versions from PyPI, not from dvt-adapters.
**Fix needed:**
- Replace all "dbt-core" branding with "dvt-ce"
- Show DVT version only, no "update available" against PyPI dbt-core
- Adapter versions should come from dvt-adapters (one version for all)
- Link to `https://github.com/heshamh96/dvt-ce`
- Suppress oracle adapter "thin mode" warning and thrift SSL warning

### F2: dvt debug (UX)
**Problem:** Sling output is noisy, connection URLs exposed, inconsistent formatting.
**Fix needed:**
- Use 🟩 `[OK]` / 🟥 `[FAIL]` symbols
- Suppress ALL Sling output (connection URLs, temp file paths, CLI banners)
- User should NOT know Sling is involved — just show adapter connectivity status
- Clean, consistent formatting per connection

### F3: dvt sync (UX)
**Problem:** Functional but poorly formatted. Adapter warnings leak through.
**Fix needed:**
- Use 🟩/🟥 symbols for status
- Suppress oracle "thin mode" and thrift SSL warnings
- Clean formatting

### F4: dvt seed (FUNCTIONAL + UX)
**Problem:** Sling output is noisy. 3/11 seeds FAIL on type inference (dirty CSV data: "1.25%", "_4").
**Fix needed:**
- Suppress ALL Sling output during seeds — show only dbt-style "OK loaded seed file" / "ERROR loading seed file"
- Fix type inference: either pass `adjust_column_type: true` to Sling, or default columns to text/varchar (like dbt's agate loader does), or use `SLING_SAMPLE_SIZE` env var
- On error: show clean DVT error message, not raw Sling/pq errors
- The user should feel at home (dbt-like output)

### F5: dvt run (FUNCTIONAL + UX)
**Problem:** Sling output floods the terminal. Many models fail. Output is not dbt-like.
**Fix needed:**
- **UX:** Suppress ALL Sling/DuckDB output during model execution. The extraction path (source→Sling→DuckDB→Sling→target) is a BLACK BOX. Show only dbt-style model output ("OK created sql table model", "ERROR creating sql table model").
- **Functional:** Many failures in trial_20 are from:
  - Seeds that failed to load (cascading failures for models that depend on them)
  - Source tables not existing on `pg_dev` (the default target is `pg_dev` port 5433, not `pg_docker` port 5432)
  - Models with syntax errors for their target engines
- Need to fix Coke_DB models + seeds before re-testing

### F6: --target Philosophy (ARCHITECTURE)
**Problem:** `--target` can switch to a different adapter type, causing pushdown models to fail with syntax errors.
**Rule (added to DVT_RULES.md):**
- `--target` switches environments (same engine), NOT engines
- Pushdown models use target dialect — work on same engine type only
- Extraction models use DuckDB dialect — work on any engine
- DVT warns (DVT007) when `--target` changes adapter type, doesn't block
- This is the DVT philosophy: two dialects coexist in one project

---

## UPCOMING PHASES

### Phase 10: UX/Branding Overhaul [NEXT — HIGH PRIORITY]

Sling is an implementation detail. The user should NEVER see it.

| Item | Priority | Details |
|------|----------|---------|
| P10.1: Suppress Sling stdout/stderr | CRITICAL | Redirect all Sling subprocess output to /dev/null or capture and log at DEBUG level only. ALL dvt commands (run, seed, debug, show) should produce clean, dbt-like output. |
| P10.2: dvt --version rebranding | HIGH | Replace dbt-core version check with DVT-only output. Show dvt-ce version + dvt-adapters version. No PyPI update check against dbt-core. Link to github.com/heshamh96/dvt-ce. |
| P10.3: dvt debug clean output | HIGH | 🟩 `[OK]` / 🟥 `[FAIL]` per connection. No Sling URLs, no temp paths, no CLI banners. Suppress adapter import warnings (oracle thin mode, thrift SSL). |
| P10.4: dvt sync clean output | HIGH | 🟩/🟥 symbols. Suppress adapter warnings. |
| P10.5: dvt seed clean output | HIGH | dbt-style output only. Suppress Sling. On error: clean DVT error message. |
| P10.6: dvt run clean output | CRITICAL | Black-box extraction path. Suppress ALL Sling/DuckDB output. dbt-style model output only. |
| P10.7: --target adapter type warning | MEDIUM | DVT007 warning when --target changes adapter type. |

### Phase 11: Seed Robustness

| Item | Priority | Details |
|------|----------|---------|
| P11.1: Type inference fix | HIGH | Default to text/varchar for all CSV columns (like dbt's agate). Or pass `adjust_column_type: true` to Sling. Dirty CSV data ("1.25%", "_4") must not crash seeding. |
| P11.2: Seed --target cross-engine | HIGH | Verify seeding works across all engine targets. Tested: pg, mysql, oracle, mssql, mariadb, snowflake, databricks. |
| P11.3: Large seed performance | MEDIUM | Verify streaming + low memory footprint for 1M+ row CSVs across all targets. |

### Phase 12: Full DAG Testing (Trial 20 Fixes)

| Item | Priority | Details |
|------|----------|---------|
| P12.1: Fix Coke_DB seeds | HIGH | Fix dirty CSV data (customers_db_1/2 Discount column, transactions_a Day column). |
| P12.2: Fix Coke_DB models | HIGH | Review all 68 models. Fix syntax errors for their intended targets. Ensure sources match. |
| P12.3: Verify pg_dev vs pg_docker | HIGH | Default target is `pg_dev` (port 5433). Many models use `pg_docker` (port 5432) sources. Ensure seeds are loaded to the right target. |
| P12.4: Full DAG run with clean data | HIGH | Re-run all 68 models after fixing seeds and models. Target: 0 errors. |

### Phase 13: README + Documentation

| Item | Priority | Details |
|------|----------|---------|
| P13.1: GitHub README (master branch) | HIGH | Comprehensive README for the Sling+DuckDB architecture. Not the old Spark stuff. Getting started, installation, examples, philosophy. |
| P13.2: PyPI description | MEDIUM | Update setup.py long_description for PyPI listing. |
| P13.3: Create README update skill | LOW | Skill that auto-generates README from hesham_mind/ docs when merging to master. |

### Phase 14: Advanced Features

| Item | Priority | Details |
|------|----------|---------|
| P14.1: Bucket materialization | MEDIUM | `config(target='s3_bucket', format='delta')` |
| P14.2: CDC extraction | LOW | Sling `change-capture` mode |
| P14.3: Virtual federation | LOW | `materialized='virtual'` via DuckDB ATTACH |
| P14.4: `dvt docs` lineage enhancement | LOW | Show extraction paths in lineage graph |

### Phase 15: Testing + Release

| Item | Priority | Details |
|------|----------|---------|
| P15.1: Unit tests | MEDIUM | Watermark formatter, connection mapper, target resolver, optimizer |
| P15.2: Integration test suite | MEDIUM | Automated E2E tests for all execution paths |
| P15.3: PyPI publish (clean release) | MEDIUM | dvt-ce + dvt-adapters with all UX fixes |
| P15.4: GitHub release notes | LOW | Changelog, migration guide from dbt |

---

## DVT Philosophy

### Two Dialects, One Project

DVT projects have two types of SQL:

1. **Target dialect** (pushdown models) — written for the target engine. Uses engine-specific features (Snowflake QUALIFY, Postgres JSONB, etc.). Runs directly on the target via adapter.

2. **DuckDB dialect** (extraction models) — written in DuckDB SQL. Engine-agnostic. Runs in DuckDB cache. Result loaded to any target via Sling.

### `--target` Switches Environments, Not Engines

- `dvt run --target prod_snowflake` → switches from dev to prod Snowflake. Same dialect. Works.
- `dvt run --target mssql_docker` → switches from Postgres to SQL Server. Pushdown models break. Expected.
- DVT warns (DVT007) but doesn't block — the user might know what they're doing.

### Sling is Invisible

The user should NEVER see Sling output, URLs, or errors. The extraction path (source→Sling→DuckDB→Sling→target) is a black box. DVT shows only dbt-like output. Sling details are logged at DEBUG level for troubleshooting.

### Seeds Must Be Robust

Sling seed loading must handle dirty CSV data as gracefully as dbt's agate loader. Default to text/varchar. Never crash on type inference.

---

## Architecture Summary

```
Execution flow:
  1. dbt compiles model (Jinja → SQL in target dialect)
  2. DVT detects remote sources (source.connection != model.target)
  3. If pushdown: adapter executes SQL on target (standard dbt)
  4. If extraction:
     a. Federation optimizer decomposes → per-source queries
     b. Sling extracts sources → .dvt/cache.duckdb (INVISIBLE)
     c. DuckDB executes model SQL (INVISIBLE)
     d. Sling loads result → target (INVISIBLE)
     e. User sees only: "OK created sql table model" (like dbt)

Paths:
  Default pushdown:     source.connection == default target → adapter SQL
  Non-default pushdown: target override, same adapter type → DuckDB cache → target
  Extraction:           source.connection != model target → DuckDB cache → target

Cache: .dvt/cache.duckdb (persistent, per-project, thread-safe)
  Sources: {source}__{table} (SELECT *, shared across models)
  Results: __model__{name} (for incremental {{ this }})
```

---

*Updated after each development session.*
