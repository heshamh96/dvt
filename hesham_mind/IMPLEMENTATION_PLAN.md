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
| P1.2: Watermark formatter | DONE | 13 dialects × 4 types (timestamp, date, integer, string). Oracle TO_TIMESTAMP, SQL Server CONVERT, BigQuery TIMESTAMP prefix, etc. |
| P1.3: Source connection parsing | DONE | Reads `connection:` directly from sources.yml (parallel to dbt parsing). Also added `connection` field to dbt's `UnparsedSourceDefinition`. |
| P1.4: Target resolver | DONE | `resolve_all_models()` classifies each model into execution paths. Reads source connections, compares vs model target. |
| P1.5: Sling client | DONE | `SlingClient` with lazy import. Methods: `extract_to_target`, `extract_to_duckdb`, `load_from_duckdb`, `load_seed`, `load_cross_target`. |
| P1.6: DuckDB compute engine | DONE | `DuckDBCompute` — ephemeral DuckDB instance (temp file). Extract sources → DuckDB, run SQL, load result → target. |
| P1.7: DvtRunTask + DvtModelRunner | DONE | `DvtRunTask(RunTask)` resolves models, returns `DvtModelRunner` for extraction. `DvtModelRunner(ModelRunner)` dispatches: pushdown vs extraction. Source ref rewriting (longest-first replacement). |
| P1.8: DvtBuildTask | DONE | `DvtBuildTask(DvtRunTask, BuildTask)` — inherits extraction paths for `dvt build`. |

### Phase 2: Seeds, Debug, Show [DONE]

| Item | Status | Details |
|------|--------|---------|
| P2.1: `dvt seed` via Sling | DONE | `DvtSeedTask` + `DvtSeedRunner`. Sling bulk loads CSV → target (COPY, bcp). 10-100x faster than dbt's agate INSERT. Supports `--full-refresh` and `--target`. |
| P2.2: `dvt show` via DuckDB | DONE | `DvtShowTask`. Starts DuckDB in-memory, ATTACHes to Postgres/MySQL sources, runs inline SQL. Cross-engine JOINs locally. |
| P2.3: `dvt debug` | DONE | `DvtDebugTask`. Tests all connections (or `--target X`) via Sling replication ping. Oracle uses `SELECT 1 FROM DUAL`. Reports Sling/DuckDB availability. |
| P2.4: Broad testing | DONE | 7/7 federation models PASS. 27/27 nodes in `dvt build` PASS. 9/9 connections OK. |

### Phase 3: Package Consolidation [DONE]

| Item | Status | Details |
|------|--------|---------|
| P3.1: dvt-adapters rebuild | DONE | Fresh from upstream dbt-adapters monorepo + community repos. 13 engines: postgres, snowflake, bigquery, redshift, spark, databricks, duckdb, sqlserver, mysql, mysql5, mariadb, oracle, fabric. |
| P3.2: Import fixes | DONE | MySQL adapters: `dbt.contracts.connection` → `dbt.adapters.contracts.connection`, `dbt.events.AdapterLogger` → `dbt.adapters.events.logging.AdapterLogger`, etc. Databricks: `metadata.version("dbt-core")` → fallback to `dvt-ce`. |
| P3.3: dvt-ce depends on dvt-adapters | DONE | Replaced `dbt-adapters` dependency with `dvt-adapters>=0.2.1`. Clean stack: zero dbt-core, zero dbt-adapters from PyPI. |
| P3.4: Sync installs drivers only | DONE | No longer installs `dbt-*` packages. Only installs driver deps (psycopg2, oracledb, etc.) that dvt-adapters needs. |

### E2E Test Results (Trial 19)

| Test | Result |
|------|--------|
| `dvt sync` | 6/6 adapters, DuckDB, Sling |
| `dvt debug` | 9/9 connections OK (pg, mysql, mssql, oracle, mariadb, pg-dev, sf x2, databricks) |
| `dvt seed test_seed` | PASS (Sling bulk load, 5 rows) |
| `dvt run pushdown_pg` | PASS (adapter pushdown) |
| `dvt run mysql_to_pg` | PASS (extraction: MySQL→PG) |
| `dvt run mariadb_to_pg` | PASS (extraction: MariaDB→PG) |
| `dvt run mssql_to_pg` | PASS (extraction: MSSQL→PG) |
| `dvt run oracle_to_pg` | PASS (extraction: Oracle→PG) |
| `dvt run cross_pg_mysql` | PASS (extraction: PG+MySQL JOIN→PG) |
| `dvt run cross_all_docker_to_pg` | PASS (extraction: 5-engine JOIN→PG via DuckDB, 4 rows) |
| `dvt build` (27 nodes) | 27/27 PASS |
| `dvt show` cross-engine JOIN | PASS (PG+MySQL via DuckDB ATTACH) |

---

## UPCOMING

### Phase 4: Persistent DuckDB Cache + Incremental Extraction [NEXT]

This is the big architectural change: kill Sling Direct sub-path, ALL extraction goes through DuckDB with persistent cache.

**Core concept:** DuckDB cache at `.dvt/cache.duckdb` persists between runs. Sources are cached per-source (shared across models). Incremental models use the cache for `is_incremental()` detection and the target for watermark values.

| Item | Priority | Details |
|------|----------|---------|
| P4.1: `.dvt/` cache directory management | HIGH | Create `.dvt/` in project dir. Manage `cache.duckdb` lifecycle. `--full-refresh` deletes it. `dvt clean` deletes it. |
| P4.2: Persistent DuckDB cache engine | HIGH | Replace ephemeral `DuckDBCompute` with persistent `DvtCache`. Opens `.dvt/cache.duckdb` (creates if missing). Manages source tables and model result tables. |
| P4.3: Unified extraction path | HIGH | Remove Sling Direct sub-path from DvtModelRunner. All extraction: Sling→DuckDB cache→Sling→target. Two-way dispatch (pushdown vs extraction). |
| P4.4: Cache-per-source extraction | HIGH | Extract each remote source once into DuckDB cache as `{source_name}__{table_name}`. Multiple models share the same cached source. |
| P4.5: Incremental extraction | HIGH | `is_incremental()` checks DuckDB cache (fast). Watermark from TARGET (accurate). Format watermark in source dialect. Sling extracts delta only. DuckDB merges delta into cache. Model SQL runs with incremental WHERE. Sling loads delta to target. |
| P4.6: `--full-refresh` cache cleanup | HIGH | Delete `.dvt/cache.duckdb` (or drop relevant tables). Re-extract everything. |
| P4.7: View/ephemeral coercion | HIGH | If extraction model is `view` or `ephemeral`, coerce to `table` with DVT001 warning. |
| P4.8: Test incremental extraction E2E | HIGH | Create cross-engine incremental model in Coke_DB. Test first run (full) + subsequent run (delta) + `--full-refresh`. |

### Phase 5: Federation Optimizer [HIGH PRIORITY]

Reduce data movement — critical for single-machine operation.

| Item | Priority | Details |
|------|----------|---------|
| P5.1: Column pruning | HIGH | Analyze model SQL (SQLGlot AST) to determine which columns are actually used from each source. Extract only those columns. |
| P5.2: Predicate pushdown | HIGH | Identify WHERE predicates that can be pushed to the source extraction query. Reduce rows extracted. |
| P5.3: LIMIT pushdown | MEDIUM | If model has LIMIT, propagate to extraction query. |
| P5.4: Source-side aggregation | LOW | If model only uses aggregated data from a source, push the GROUP BY to the source. |

### Phase 6: Non-Default Pushdown + Polish

| Item | Priority | Details |
|------|----------|---------|
| P6.1: Non-default pushdown path | HIGH | When model targets non-default adapter and all sources match, compile and execute using non-default adapter (not DuckDB). |
| P6.2: `~/.dvt` profiles directory | MEDIUM | Add `~/.dvt` as fallback alongside `~/.dbt`. VS Code dbt extension compatibility. |
| P6.3: DVT error codes | MEDIUM | Replace RuntimeError strings with proper DVT error classes (DVT100-DVT111). |
| P6.4: DVT event logging | MEDIUM | Log Sling extraction, DuckDB compute, watermark resolution via dbt's event system. |
| P6.5: `dvt init` template | MEDIUM | DVT-specific starter project with multi-adapter profiles.yml example. |

### Phase 7: DuckDB Connectivity to All Engines

| Item | Priority | Details |
|------|----------|---------|
| P7.1: Research DuckDB extensions | HIGH | Inventory which engines DuckDB can ATTACH to natively (postgres, mysql, sqlite confirmed). Research: Snowflake (via httpfs?), BigQuery, MSSQL, Oracle. |
| P7.2: Sling-as-bridge for non-ATTACHable engines | MEDIUM | For engines DuckDB can't ATTACH to, use Sling to extract into DuckDB. This is the current approach — formalize it. |
| P7.3: `dvt show` for all engines | MEDIUM | Make `dvt show` work with all 13 engines. ATTACH where possible, Sling-extract where not. |

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
| P9.1: Unit tests | MEDIUM | Watermark formatter, connection mapper, target resolver, source connection parser. |
| P9.2: Integration tests | MEDIUM | Cross-engine incremental, multi-source DuckDB, pushdown, seed, show, debug. |
| P9.3: Publish to PyPI | MEDIUM | dvt-ce + dvt-adapters with matching versions. |
| P9.4: Documentation | LOW | README, getting started guide, migration from dbt guide. |

---

## Architecture Summary (Current)

```
dvt-ce (provides dbt.* + dvt.*)
├── dbt/                           # Upstream dbt-core 1.9.10 (1 field added to UnparsedSourceDefinition)
├── dvt/
│   ├── cli/
│   │   ├── __init__.py            # Resilient entry point (falls back to sync-only)
│   │   └── main.py                # Full CLI: all dbt commands + dvt sync
│   ├── tasks/
│   │   ├── run.py                 # DvtRunTask — resolves models, dispatches runners
│   │   ├── build.py               # DvtBuildTask — DvtRunTask + BuildTask
│   │   ├── seed.py                # DvtSeedTask — Sling-based CSV loading
│   │   ├── show.py                # DvtShowTask — DuckDB local queries
│   │   ├── debug.py               # DvtDebugTask — multi-connection health check
│   │   └── sync.py                # DvtSyncTask — env bootstrap + self-healing
│   ├── config/
│   │   ├── source_connections.py  # Read connection: from sources.yml
│   │   └── target_resolver.py     # Execution path resolution per model
│   ├── extraction/
│   │   ├── connection_mapper.py   # 16 adapters → Sling URLs
│   │   ├── watermark_formatter.py # 13 dialects × 4 types
│   │   └── sling_client.py        # Sling Python wrapper (lazy import)
│   ├── runners/
│   │   ├── model_runner.py        # DvtModelRunner — pushdown vs extraction
│   │   └── seed_runner.py         # DvtSeedRunner — Sling CSV loading
│   ├── federation/
│   │   └── duckdb_compute.py      # Ephemeral DuckDB (TODO: replace with persistent cache)
│   └── sync/
│       ├── profiles_reader.py     # Read profiles.yml
│       ├── adapter_installer.py   # Install database drivers
│       ├── duckdb_extensions.py   # Install DuckDB extensions
│       ├── cloud_deps.py          # Install cloud SDKs
│       └── sling_checker.py       # Verify/bootstrap Sling binary

dvt-adapters (provides dbt.adapters.* + dbt.include.*)
├── Framework: base, sql, contracts, catalogs, events, exceptions, record, etc.
├── Engines: postgres, snowflake, bigquery, redshift, spark, databricks,
│            duckdb, sqlserver, mysql, mysql5, mariadb, oracle, fabric
└── Each engine: Python adapter + SQL macros in dbt.include.<engine>/
```

## Execution Paths

```
For each model:
  1. Resolve target (CLI --target > model config > profiles.yml default)
  2. Check all source().connection vs model.target
  3. If ALL local → PUSHDOWN (adapter SQL on target, standard dbt)
  4. If ANY remote → EXTRACTION (Sling → DuckDB cache → Sling → target)
```

## DuckDB Cache (Phase 4 — upcoming)

```
my_project/
  .dvt/
    cache.duckdb           ← persistent DuckDB database
      crm__customers       ← cached source table (extracted via Sling)
      crm__orders          ← cached source table
      stg_orders           ← model result (for incremental {{ this }})

Flow: Source → Sling → .dvt/cache.duckdb → model SQL in DuckDB → Sling → Target

Incremental: watermark from TARGET, delta extraction via Sling,
             merge into cache, model SQL with is_incremental(),
             delta load to target via Sling
```

## Key Design Decisions

| Decision | Choice | Rationale |
|----------|--------|-----------|
| Internal namespace | `dbt.*` (not renamed) | Compatible with all dbt adapters and extensions |
| Adapter consolidation | `dvt-adapters` (one package, all engines) | No dependency hell, no dbt-core conflicts |
| Extraction engine | DuckDB (persistent cache) | Handles JOINs, incremental, auto-spills to disk |
| Data movement | Sling | Go-based streaming, 30+ connectors, low memory |
| Extraction model dialect | DuckDB SQL | One dialect for all cross-engine models |
| Incremental detection | DuckDB cache (fast) | Avoids querying target for existence check |
| Watermark value | From TARGET (accurate) | Reflects what actually landed |
| Cache strategy | Per-source (shared) | Extract once, multiple models reuse |
| View/ephemeral extraction | Coerce to table (DVT001) | Can't create view from remote extraction |
| Profiles directory | `~/.dvt` then `~/.dbt` | VS Code dbt extension compatibility |

---

*This document is updated after each development session.*
