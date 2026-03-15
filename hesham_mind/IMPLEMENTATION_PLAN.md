# DVT Implementation Plan

## Foundation

- **Base:** dbt-core 1.9.10 (branch `new_dev` from `upstream/1.9.latest`)
- **Package:** `dvt-ce` v0.1.2
- **Internals:** `dbt.*` namespace (untouched upstream code)
- **DVT layer:** `core/dvt/` (new, extends dbt via subclassing)
- **CLI:** `dvt` entry point → `dvt.cli.main:cli`; `dbt` entry point → `dbt.cli.main:cli`

## Phase 0: Project Scaffold + dvt sync (Weeks 1-2)

### P0.1: DVT project structure

Create `core/dvt/` with the module skeleton:

```
core/dvt/
  __init__.py
  cli/
    __init__.py
    main.py              # Click CLI group mirroring dbt commands
  tasks/
    __init__.py
    sync.py              # DvtSyncTask
  config/
    __init__.py
  extraction/
    __init__.py
    connection_mapper.py
  sync/
    __init__.py
    adapter_installer.py
    duckdb_extensions.py
    cloud_deps.py
    sling_checker.py
```

**Deliverable:** `dvt --help` shows the command list. All commands initially
route to stock dbt behavior except `dvt sync`.

### P0.2: dvt sync

Implement environment bootstrap:

1. Read profiles.yml (respect `--profiles-dir`)
2. Map adapter types to pip packages (`postgres` → `dbt-postgres`, etc.)
3. Map bucket types to cloud SDKs (`s3` → `boto3`, etc.)
4. Map adapter types to DuckDB extensions (`postgres` → `postgres_scanner`, etc.)
5. Always install: `delta` extension (default bucket format)
6. Check Sling binary (`which sling` or `sling --version`)
7. Optionally test all connections
8. Report status to terminal

**Deliverable:** `dvt sync` installs all dependencies for a project.

### P0.3: Update setup.py

Add `sling` to install_requires. Ensure `deltalake` is included.
Update entry_points to include `dvt = dvt.cli.main:cli` (already done).

---

## Phase 1: Core Pipeline — Extraction + Pushdown (Weeks 3-7)

### P1.1: Connection mapper (Week 3)

`core/dvt/extraction/connection_mapper.py`

- Map profiles.yml adapter configs → Sling connection URL strings
- Handle: postgres, snowflake, bigquery, redshift, mysql, sqlserver,
  databricks, oracle, duckdb, clickhouse, trino, sqlite
- Handle: s3, gcs, azure bucket configs
- Handle special auth: key-pair, OAuth, service accounts
- Unit tests: one test per adapter type

### P1.2: Watermark formatter (Week 3)

`core/dvt/extraction/watermark_formatter.py`

- Dialect-specific literal formatting for watermark values
- Support: postgres, mysql, sqlserver, oracle, snowflake, bigquery,
  redshift, databricks, clickhouse, trino, duckdb, sqlite
- Types: timestamp, date, integer, string
- Unit tests: one test per dialect × type combination

### P1.3: Source-target mismatch detection (Week 4)

`core/dvt/config/target_resolver.py`

- For each model, check if any `source()` reference has a `connection` (from sources.yml) that differs from the model's target (from profiles.yml default or model config)
- Validate that remote source connections exist in profiles.yml
- Build extraction plan: which source tables need Sling extraction to `_dvt` staging
- Determine staging table names: `_dvt.{source_name}__{table_name}`

### P1.4: Sling client wrapper (Week 4)

`core/dvt/extraction/sling_client.py`

- Wrapper around `sling` Python package
- Methods: `extract_source()`, `load_seed()`, `load_to_target()`
- Error handling: capture Sling errors, surface in DVT run results
- Logging: integrate Sling output with DVT's event system

### P1.5: DvtRunTask + DvtModelRunner (Weeks 5-6)

`core/dvt/tasks/run.py`
`core/dvt/runners/model_runner.py`
`core/dvt/runners/extraction_runner.py`

- `DvtRunTask(RunTask)`:
  - Override `get_runner()` to return DvtModelRunner
  - Reuse ALL dbt lifecycle decorators

- `DvtModelRunner(ModelRunner)`:
  - Detect source-target mismatch → auto-extract remote sources via Sling to `_dvt` staging
  - For models with remote sources: extract source tables to staging, then run model SQL on target via adapter
  - For pushdown models (all sources local): delegates to `super().execute()` (stock dbt)
  - For cross-target models: execute on default target, then Sling to model target

### P1.6: Cross-engine incremental wiring (Weeks 5-6)

- Pre-resolve `{{ this }}` watermark queries from target before compilation
- Format watermark as dialect-specific literal for source engine
- Substitute literal into compiled SQL before sending to Sling
- Map dbt incremental strategy → Sling merge strategy
- Test: incremental extraction model across Postgres → Snowflake

### P1.7: Integration testing (Week 7)

- End-to-end test: model with remote source (Postgres source → Postgres target via _dvt staging)
- End-to-end test: remote source extraction + pushdown model (Postgres → Snowflake)
- Verify incremental models with remote sources work across runs with dialect-specific watermarks
- Verify `--full-refresh` forces full extraction + full rebuild
- Verify `--select` only runs selected models

**Phase 1 Deliverable:** `dvt run` with automatic source extraction via Sling
(transparent to user) and pushdown models via adapter. Cross-engine incremental works.

---

## Phase 2: Seeds, Cross-Target, Buckets (Weeks 8-12)

### P2.1: Sling-based seed loading (Week 8)

`core/dvt/tasks/seed.py`
`core/dvt/runners/seed_runner.py`

- `DvtSeedTask(SeedTask)`:
  - Override to use Sling instead of dbt's agate-based loader
  - Support `--full-refresh` (drop + create) and default (truncate + load)
  - Support `--target <target_name>` to redirect seeds

- `DvtSeedRunner`:
  - Calls Sling client: `sling.run(src_stream=csv_path, tgt_conn=target, ...)`
  - Uses Sling's native bulk loading for performance
  - Reports: rows loaded, duration

### P2.2: Cross-target model materialization (Weeks 9-10)

`core/dvt/loading/sling_loader.py`

- When model target != default target:
  1. Execute model SQL on default target → temp result table
  2. Sling streams temp result → model's target
  3. Drop temp result table
- Handle both database targets and bucket targets
- For databases: standard Sling DB-to-DB streaming
- For buckets: Sling DB-to-file streaming

### P2.3: Bucket materialization (Weeks 10-11)

`core/dvt/loading/bucket_materializer.py`

- Handle `config(target='s3_bucket', format='delta')`
- Supported formats: delta (default), parquet, csv, json, jsonlines
- Delta Lake integration via Sling's DuckDB target + delta extension,
  or via Sling's native file writing
- Path construction: `{bucket_prefix}/{path}/{model_name}/`

### P2.4: Source-side view pushdown (Weeks 11-12)

- Support models that target a source connection for source-side filtering
- These are standard dbt models with `config(target='source_xxx')`
- DVT detects when a model targets a source connection and pushes SQL there
- Test: filter on SQL Server via T-SQL, DVT auto-extracts filtered result to target

### P2.5: DvtBuildTask (Week 12)

`core/dvt/tasks/build.py`

- `DvtBuildTask(BuildTask)`: combines extraction, models, tests, seeds, snapshots
- Uses DVT runners for all node types

**Phase 2 Deliverable:** Full ELT pipeline with seeds via Sling,
cross-target materialization, bucket outputs in Delta format,
and incremental extraction + transformation.

---

## Phase 3: Local Dev, Debug, Polish (Weeks 13-16)

### P3.1: dvt show (Weeks 13-14)

`core/dvt/tasks/show.py`
`core/dvt/federation/engine.py`

- DuckDB in-process engine
- ATTACH to source databases where supported
- SQLGlot transpilation for target-dialect SQL → DuckDB SQL
- Source resolution: `{{ source() }}` → ATTACH'd table or scanner view
- Display results in terminal (table, csv, json formats)

### P3.2: dvt debug (Week 14)

`core/dvt/tasks/debug.py`

- Test all connections from profiles.yml
- Check Sling binary
- Check DuckDB extensions
- Check dbt adapter installations
- Report per-connection: type, version, latency, status

### P3.3: dvt compile enhancements (Week 15)

- Show extraction plan alongside compiled SQL
- Show execution path per model (pushdown vs cross-target)
- Show target per model

### P3.4: dvt init template (Week 15)

- Create DVT-specific starter project template
- Includes: multi-adapter profiles.yml, sources.yml with connection metadata,
  example cross-engine model

### P3.5: Documentation and lineage (Week 16)

- Extraction nodes appear in `dvt docs` lineage graph
- Cross-target models show their target in the docs
- Bucket targets shown in lineage

**Phase 3 Deliverable:** Complete developer experience with local query,
debugging, and documentation.

---

## Phase 4: Advanced Features (Weeks 17-24)

### P4.1: Federation optimizer (Weeks 17-19)

`core/dvt/federation/optimizer.py`

- Analyze model SQL (via SQLGlot AST) to determine:
  - Which columns are actually needed from each source (column pruning)
  - Which WHERE predicates can be pushed to source extraction queries
  - Which LIMIT clauses can be pushed down
- Generate optimized extraction queries instead of `SELECT *`
- This reduces data movement significantly for large sources

### P4.2: CDC integration (Weeks 19-20)

- Wire Sling's `change-capture` mode into DVT extraction nodes
- Handle CDC metadata columns (_sling_synced_at, _sling_synced_op)
- Support soft deletes in downstream models
- Requires SLING_STATE configuration

### P4.3: Virtual federation via DuckDB (Weeks 21-23)

- New materialization: `materialized='virtual'`
- Model SQL runs in DuckDB against ATTACH'd sources
- No materialization — result computed on-demand
- Useful for real-time cross-source queries
- DuckDB ATTACH supports: Postgres, MySQL, SQLite

### P4.4: Snapshot models with Sling (Week 24)

- Enhance snapshot handling with Sling extraction
- Snapshot sources across engines

**Phase 4 Deliverable:** Advanced federation, CDC, and virtual queries.

---

## Summary Timeline

| Phase | Weeks | Deliverable |
|-------|-------|-------------|
| P0: Scaffold + Sync | 1-2 | `dvt sync`, project structure, CLI |
| P1: Core Pipeline | 3-7 | `dvt run` with extraction + pushdown |
| P2: Seeds, Cross-Target, Buckets | 8-12 | Full ELT, Sling seeds, Delta buckets |
| P3: Local Dev, Debug, Polish | 13-16 | `dvt show`, `dvt debug`, docs |
| P4: Advanced | 17-24 | Federation optimizer, CDC, virtual |

**MVP (P0-P2): ~3 months**
**Full product (P0-P4): ~6 months**

## Technical Risks

| Risk | Mitigation |
|------|-----------|
| Sling Python wrapper limitations | Fall back to subprocess calls to sling CLI |
| SQLGlot transpilation gaps (dvt show only) | Maintain dialect_patches.py for manual overrides |
| dbt adapter config parsing for Sling URLs | Start with top 6 adapters, add others incrementally |
| Watermark formatting for exotic dialects | Start with top 12 dialects, add others as needed |
| Cross-engine incremental watermark resolution | Pre-resolve from target, substitute literal. Test extensively. |
| Source extraction performance for large tables | Use incremental watermarks to limit extraction volume. Monitor staging table sizes. |
| Sling CDC requires Pro license | Document as optional feature, ensure free-tier works without CDC |
| DuckDB ATTACH limitations (read-only, limited dialect support) | Use as dev tool only, not production path |
