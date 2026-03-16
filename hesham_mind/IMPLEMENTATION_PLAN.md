# DVT Implementation Plan — Shared Memory

> **Last Updated:** March 2026  
> **Status:** Living document — updated after each phase

---

## Foundation

- **dvt-ce**: dbt-core 1.9.10 renamed to `dvt-ce`. Provides `dbt.*` + `dvt.*`.
- **dvt-adapters**: 13 engine adapters in one package. Provides `dbt.adapters.*` + `dbt.include.*`.
- **Test project:** `Testing_Playground/trial_19_dvt_ce_pypi/Coke_DB` (5 Docker DBs + remote SF/PG/DBX).
- **Both packages:** local editable in trial_19 `.venv`.

---

## COMPLETED

### Phase 0: Scaffold + Sync [DONE]
- DVT CLI (all dbt commands + `dvt sync`). Resilient fallback to sync-only CLI.
- `dvt sync`: self-healing — purges dbt-core, installs drivers, DuckDB extensions, verifies Sling. Favors uv.

### Phase 1: Core Extraction Pipeline [DONE]
- Connection mapper (16 adapters → Sling URLs)
- Watermark formatter (13 dialects × 4 types)
- Source connection parsing from sources.yml
- Target resolver (execution path classification per model)
- Sling client (lazy import, 5 operations)
- DvtRunTask + DvtModelRunner + DvtBuildTask

### Phase 2: Seeds, Debug, Show [DONE]
- `dvt seed` via Sling (bulk CSV loading, 10-100x faster)
- `dvt show` via DuckDB (local queries, ATTACH to PG/MySQL)
- `dvt debug` (all connections or `--target X`, Sling ping)

### Phase 3: Package Consolidation [DONE]
- dvt-adapters: 13 engines from upstream + community repos, import fixes
- dvt-ce depends on dvt-adapters (zero dbt-core/dbt-adapters from PyPI)
- Sync installs drivers only (no dbt-* packages)

### Phase 4: Persistent DuckDB Cache + Incremental [DONE]
- `.dvt/cache.duckdb` persistent cache. Thread-safe file lock.
- Unified extraction: ALL cross-engine goes through DuckDB cache (killed Sling Direct)
- Cache-per-source (shared across models). Model results cached for `{{ this }}`.
- Incremental: `is_incremental()` via cache, watermark from TARGET, delta extraction, `{{ this }}` rewriting
- `--full-refresh` destroys cache. View/ephemeral coerced to table (DVT001).
- Fix: stale `is_incremental()` stripped when cache is empty.

### Phase 5: Federation Optimizer [DONE]
- Query decomposition via SQLGlot AST → per-source extraction queries
- Predicate pushdown (safe comparisons only, function calls stay in DuckDB)
- LIMIT pushdown (transpiled: LIMIT→TOP for TSQL, LIMIT→FETCH FIRST for Oracle)
- Full extraction query transpiled DuckDB → source dialect via SQLGlot
- Column pruning disabled at extraction (cache shared); DuckDB handles it internally

### Phase 6: Non-Default Pushdown + Polish [DONE]
- **Non-default pushdown**: models targeting non-default engines (MySQL, Oracle, MSSQL, MariaDB) correctly route through DuckDB cache and load to the right target with the right schema (dbo for MSSQL, SYSTEM for Oracle, devdb for MySQL/MariaDB)
- Target resolver reads `config.target` from `_extra` (AdditionalPropertiesAllowed)
- `_model_table_name` uses target's schema from profiles.yml for non-default targets
- `~/.dvt` profiles directory fallback (before `~/.dbt`)
- `dvt clean` deletes `.dvt/` cache directory
- `.gitignore` template includes `.dvt/`
- DVT error classes (DVT001-DVT109) in `dvt/exceptions.py`

### E2E Test Results (Trial 19 — Latest)

**12/12 models PASS on BOTH run 1 (fresh) and run 2 (incremental):**

| Test | Path | Target | Result |
|------|------|--------|--------|
| `pushdown_pg` | Default pushdown | pg_docker | PASS |
| `pushdown_mysql` | Non-default pushdown | mysql_docker | PASS |
| `pushdown_oracle` | Non-default pushdown | oracle_docker | PASS |
| `pushdown_mssql` | Non-default pushdown | mssql_docker | PASS |
| `pushdown_mariadb` | Non-default pushdown | mariadb_docker | PASS |
| `mysql_to_pg` | Extraction | MySQL→PG | PASS |
| `mariadb_to_pg` | Extraction | MariaDB→PG | PASS |
| `mssql_to_pg` | Extraction | MSSQL→PG | PASS |
| `oracle_to_pg` | Extraction | Oracle→PG | PASS |
| `cross_pg_mysql` | Extraction | PG+MySQL→PG | PASS |
| `cross_all_docker_to_pg` | Extraction | 5-engine JOIN→PG | PASS |
| `incremental_mysql_to_pg` | Incremental extraction | MySQL→PG (delta) | PASS |

Also: `dvt sync` 6/6, `dvt debug` 9/9, `dvt seed` PASS, `dvt build` 27/27, `dvt show` PASS.

**No known issues.**

---

## UPCOMING

### Phase 7: DuckDB Connectivity to All Engines [NEXT]

| Item | Priority | Details |
|------|----------|---------|
| P7.1: DuckDB ATTACH research | HIGH | postgres ✓, mysql ✓, sqlite ✓. Research: Snowflake, BigQuery, MSSQL, Oracle via extensions or community scanners. |
| P7.2: `dvt show` for all engines | HIGH | ATTACH where DuckDB supports it natively. For unsupported engines, use Sling to extract into DuckDB temp, then query. Make `dvt show` work with ALL 13 engines. |
| P7.3: `dvt show --select model_name` | MEDIUM | Compile model SQL, resolve sources, run in DuckDB locally. |

### Phase 8: Advanced Features

| Item | Priority | Details |
|------|----------|---------|
| P8.1: Bucket materialization | MEDIUM | `config(target='s3_bucket', format='delta')`. SQL on default target → Sling → bucket. |
| P8.2: CDC extraction | LOW | Sling `change-capture` mode for transaction log reading. |
| P8.3: Virtual federation | LOW | `materialized='virtual'` via DuckDB ATTACH — ephemeral cross-source queries. |
| P8.4: `dvt docs` lineage enhancement | LOW | Show extraction paths in lineage graph. |

### Phase 9: Testing + Release

| Item | Priority | Details |
|------|----------|---------|
| P9.1: Unit tests | MEDIUM | Watermark formatter, connection mapper, target resolver, optimizer. |
| P9.2: Integration tests | MEDIUM | Full E2E test suite as automated tests. |
| P9.3: PyPI publish | MEDIUM | dvt-ce + dvt-adapters with all fixes. |
| P9.4: Documentation | LOW | README, getting started, migration from dbt guide. |

---

## Architecture Summary

```
Execution flow:
  1. dbt compiles model (Jinja → SQL in target dialect)
  2. DVT rewrites source refs → DuckDB cache table names
  3. Federation optimizer decomposes → per-source extraction queries
     (predicate pushdown + LIMIT, transpiled to source dialect)
  4. Sling extracts each source → .dvt/cache.duckdb
  5. DuckDB executes rewritten model SQL
  6. Sling loads result → target (default or non-default)
     (incremental: watermark from target, delta-only)

Paths:
  Default pushdown:     source.connection == default target → adapter SQL
  Non-default pushdown: source.connection == non-default target → DuckDB cache → non-default target
  Extraction:           source.connection != model target → DuckDB cache → model target

Cache: .dvt/cache.duckdb (persistent, per-project, thread-safe)
  Sources: {source}__{table} (SELECT *, shared across models)
  Results: __model__{name} (for incremental {{ this }})
```

---

*Updated after each development session.*
