# DVT Implementation Plan — Shared Memory

> **Last Updated:** March 2026  
> **Status:** Living document — updated after each phase

This file tracks everything that has been done and what is upcoming.
It acts as shared memory between sessions.

---

## Foundation

- **dvt-ce**: dbt-core 1.9.10 with package renamed to `dvt-ce`. Provides `dbt.*` + `dvt.*` namespaces.
- **dvt-adapters**: All 13 engine adapters in one package. Provides `dbt.adapters.*` + `dbt.include.*`.
- **Branch:** `new_dev` on dvt-ce repo, `dev` on dvt-adapters repo.
- **Test project:** `Testing_Playground/trial_19_dvt_ce_pypi/Coke_DB` with 5 Docker databases.
- **Both packages installed as local editable** in trial_19 `.venv`.

---

## COMPLETED

### Phase 0: Scaffold + Sync [DONE]

| Item | Status | Details |
|------|--------|---------|
| P0.1: DVT CLI scaffold | DONE | `dvt` entry point with all dbt commands + `dvt sync`. Resilient `dvt.cli.__init__` falls back to sync-only CLI when dbt imports fail. |
| P0.2: `dvt sync` | DONE | Self-healing: purges dbt-core conflicts, installs database drivers (not dbt-* packages), installs DuckDB extensions, verifies Sling. Favors `uv` over `pip`. |

### Phase 1: Core Extraction Pipeline [DONE]

| Item | Status | Details |
|------|--------|---------|
| P1.1: Connection mapper | DONE | 16 adapter types → Sling URLs. Fixed: postgres sslmode, sqlserver TLS, oracle service_name, databricks token/port/http_path format. |
| P1.2: Watermark formatter | DONE | 13 dialects × 4 types (timestamp, date, integer, string). |
| P1.3: Source connection parsing | DONE | Reads `connection:` from sources.yml + added field to `UnparsedSourceDefinition`. |
| P1.4: Target resolver | DONE | `resolve_all_models()` classifies models into execution paths. |
| P1.5: Sling client | DONE | `SlingClient` with lazy import. 5 methods. |
| P1.6: DvtRunTask + DvtModelRunner | DONE | Two-way dispatch: pushdown vs extraction. Source ref rewriting (longest-first). |
| P1.7: DvtBuildTask | DONE | Inherits extraction paths for `dvt build`. |

### Phase 2: Seeds, Debug, Show [DONE]

| Item | Status | Details |
|------|--------|---------|
| P2.1: `dvt seed` via Sling | DONE | Sling bulk loads CSV → target. 10-100x faster than dbt. |
| P2.2: `dvt show` via DuckDB | DONE | DuckDB in-memory, ATTACHes to Postgres/MySQL. Cross-engine JOINs locally. |
| P2.3: `dvt debug` | DONE | Tests all connections (or `--target X`) via Sling ping. |

### Phase 3: Package Consolidation [DONE]

| Item | Status | Details |
|------|--------|---------|
| P3.1: dvt-adapters rebuild | DONE | 13 engines. Import fixes for mysql, databricks. |
| P3.2: dvt-ce depends on dvt-adapters | DONE | Clean stack: zero dbt-core/dbt-adapters from PyPI. |
| P3.3: Sync installs drivers only | DONE | No `dbt-*` packages from PyPI. |

### Phase 4: Persistent DuckDB Cache + Incremental [DONE]

| Item | Status | Details |
|------|--------|---------|
| P4.1: `.dvt/` cache directory | DONE | Persistent at `.dvt/cache.duckdb`. Thread-safe file lock. |
| P4.2: Unified extraction path | DONE | Killed Sling Direct. ALL extraction: Sling→DuckDB cache→Sling→target. |
| P4.3: Cache-per-source | DONE | Sources cached as `{source}__{table}`. Shared across models. |
| P4.4: Incremental extraction | DONE | `is_incremental()` via cache. Watermark from TARGET. Delta-only extraction. `{{ this }}` rewriting. |
| P4.5: `--full-refresh` | DONE | Destroys cache, re-extracts. |
| P4.6: View/ephemeral coercion | DONE | DVT001 warning, coerced to table. |

### Phase 5: Federation Optimizer [DONE]

| Item | Status | Details |
|------|--------|---------|
| P5.1: Query decomposition | DONE | Parses model DuckDB SQL via SQLGlot AST. Decomposes into per-source extraction queries. |
| P5.2: Predicate pushdown | DONE | Safe predicates (comparisons, IN, BETWEEN, LIKE) pushed to source. Function calls stay in DuckDB. |
| P5.3: LIMIT pushdown | DONE | LIMIT pushed for single-source models. SQLGlot transpiles to TOP (TSQL) / FETCH FIRST (Oracle). |
| P5.4: Dialect transpilation | DONE | Full extraction query transpiled from DuckDB → source dialect via SQLGlot. Fallback to DuckDB SQL on failure. |
| P5.5: Column pruning decision | DONE | Disabled at extraction level (cache shared across models). DuckDB handles column pruning internally. |

### E2E Test Results (Trial 19 — Latest)

| Test | Result |
|------|--------|
| `dvt sync` | 6/6 adapters, DuckDB, Sling |
| `dvt debug` | 9/9 connections OK |
| `dvt seed` | PASS (Sling bulk load) |
| `dvt run` 7 table models | 7/7 PASS (pushdown + DuckDB cache) |
| `dvt run` incremental | PASS (run 1: full 5 rows, run 2: delta 0 rows) |
| `dvt run` 8 models parallel (run 1 + run 2) | 8/8 PASS both runs |
| `dvt build` (27 nodes) | 27/27 PASS |
| `dvt show` cross-engine | PASS (DuckDB ATTACH) |
| `.dvt/cache.duckdb` | Persistent, shared sources, model results |
| Federation optimizer | Predicate pushdown + LIMIT transpilation active |

### No Known Issues

All edge cases resolved. Incremental models work in parallel with table models on both fresh and subsequent runs.

---

## UPCOMING

### Phase 6: Non-Default Pushdown + Polish [NEXT]

| Item | Priority | Details |
|------|----------|---------|
| P6.1: Non-default pushdown | HIGH | Model targets non-default adapter, all sources on that adapter → compile and execute using non-default adapter (not DuckDB). |
| P6.2: `~/.dvt` profiles directory | MEDIUM | Add `~/.dvt` as fallback alongside `~/.dbt`. VS Code dbt extension compatibility. |
| P6.3: DVT error codes | MEDIUM | Proper DVT error classes (DVT100-DVT111). |
| P6.4: DVT event logging | MEDIUM | Sling/DuckDB operations via dbt's event system. |
| P6.5: `dvt init` template | MEDIUM | DVT-specific starter project. `.dvt/` in .gitignore. |
| P6.6: `dvt clean` deletes `.dvt/` | MEDIUM | Wire DvtCleanTask to also remove cache directory. |

### Phase 7: DuckDB Connectivity to All Engines

| Item | Priority | Details |
|------|----------|---------|
| P7.1: DuckDB ATTACH research | HIGH | postgres ✓, mysql ✓, sqlite ✓. Research: Snowflake, BigQuery, MSSQL, Oracle. |
| P7.2: `dvt show` for all engines | MEDIUM | ATTACH where possible, Sling-extract-to-DuckDB where not. |

### Phase 8: Advanced Features

| Item | Priority | Details |
|------|----------|---------|
| P8.1: Bucket materialization | MEDIUM | `config(target='s3_bucket', format='delta')` |
| P8.2: CDC extraction | LOW | Sling `change-capture` mode |
| P8.3: Virtual federation | LOW | `materialized='virtual'` via DuckDB ATTACH |

### Phase 9: Testing + Release

| Item | Priority | Details |
|------|----------|---------|
| P9.1: Unit tests | MEDIUM | All core modules |
| P9.2: Integration tests | MEDIUM | Full E2E test suite |
| P9.3: PyPI publish | MEDIUM | dvt-ce + dvt-adapters matching versions |
| P9.4: Documentation | LOW | README, getting started, migration guide |

---

## Architecture Summary (Current)

```
dvt-ce (provides dbt.* + dvt.*)
├── dbt/                           # Upstream dbt-core 1.9.10
├── dvt/
│   ├── cli/                       # Resilient CLI entry point
│   ├── tasks/                     # DvtRunTask, DvtBuildTask, DvtSeedTask,
│   │                              # DvtShowTask, DvtDebugTask, DvtSyncTask
│   ├── config/                    # source_connections, target_resolver
│   ├── extraction/                # connection_mapper, watermark_formatter, sling_client
│   ├── runners/                   # DvtModelRunner, DvtSeedRunner
│   ├── federation/                # dvt_cache (persistent), optimizer (decompose+transpile)
│   └── sync/                      # profiles_reader, adapter_installer, duckdb_extensions,
│                                  # cloud_deps, sling_checker

dvt-adapters (provides dbt.adapters.* + dbt.include.*)
├── Framework + 13 engines

.dvt/cache.duckdb (persistent, per-project)
├── Source tables: {source}__{table} (SELECT *, shared across models)
├── Model results: __model__{name} (for incremental {{ this }})
└── Lifecycle: persists between runs, --full-refresh destroys, dvt clean destroys
```

## Execution Flow

```
1. dbt compiles model SQL (Jinja → target dialect SQL)
2. DVT rewrites source refs → DuckDB cache table names
3. Federation optimizer:
   a. Decomposes model SQL into per-source extraction queries
   b. Identifies pushable predicates (safe comparisons only)
   c. Transpiles each extraction query to source dialect (SQLGlot)
4. Sling extracts each source (optimized query) → .dvt/cache.duckdb
5. DuckDB executes rewritten model SQL (JOINs, aggregations, etc.)
6. Sling loads result from DuckDB → target (full-refresh or incremental)
```

---

*This document is updated after each development session.*
