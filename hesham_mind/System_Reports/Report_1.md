# DVT Architecture & Implementation Report

**Generated:** 2026-02-14
**Scope:** Full codebase analysis of dvt-core

---

## 1. What Is DVT?

DVT (Data Virtualization Tool) is a fork of **dbt-core** that adds **federated query capabilities** across multiple data sources. While dbt operates against a single database, DVT can orchestrate SQL models that span **multiple databases** -- using adapter pushdown for same-target queries and **Apache Spark** for cross-target federation.

**Key difference from dbt:** DVT supports multi-datasource federation using Spark JDBC connectors when models reference sources from different targets.

---

## 2. High-Level Architecture

```
                         +------------------------------+
                         |          dvt CLI              |
                         |   (Click-based commands)      |
                         +--------------+---------------+
                                        |
                    +-------------------+-------------------+
                    |                   |                   |
              +-----v-----+      +-----v------+   +-------v---------+
              | Standard   |     | DVT Tasks  |   | Infrastructure  |
              | dbt Tasks  |     | (dvt_tasks)|   | (sync/init/     |
              |            |     |            |   |  debug/deps)    |
              +-----+------+     +-----+------+   +-----------------+
                    |                   |
                    |        +----------v----------+
                    |        | Federation Resolver  |
                    |        |  (per-model routing) |
                    |        +----------+----------+
                    |                   |
          +---------+------+-------+---+------------------+
          |                |                              |
    +-----v------+  +-----v--------+          +-----------v-----------+
    |  Standard   |  | Non-Default  |          | Federation Runner    |
    | ModelRunner |  |  Pushdown    |          | (Spark JDBC)         |
    | (same tgt)  |  |  Runner      |          |                      |
    +-----+------+  +------+-------+          +----------+-----------+
          |                |                              |
          v                v                              v
    +----------+   +--------------+           +-----------+-----------+
    | Default  |   | Target DB    |           |  Federation Engine    |
    | Adapter  |   | Adapter      |           |                       |
    | (dbt)    |   | (pushdown)   |           |  +----+ +----+ +--+  |
    +----------+   +--------------+           |  | EL | |Sprk| |QO|  |
                                              |  +----+ +----+ +--+  |
                                              +----------+------------+
                                                         |
                                            +------------+------------+
                                            |            |            |
                                      +-----v---+  +----v----+  +---v----+
                                      |Extractors|  |  Spark  |  |Loaders |
                                      |(20+ DBs) |  | Engine  |  |(6 DBs) |
                                      +-----+---+  +----+----+  +---+----+
                                            |            |            |
                                      +-----v---+  +----v----+  +---v----+
                                      | Cloud   |  | SQLGlot |  | Cloud  |
                                      | Storage |  |Transpile|  | Storage|
                                      | (S3/GCS |  +---------+  | (S3/GCS|
                                      |  /Azure)|               |  /Azure)|
                                      +---------+               +--------+
```

---

## 3. Core Components

### 3.1 CLI Layer (`dvt/cli/`) -- 13 files

| Command | DVT Extension? | Description |
|---------|:-:|-------------|
| `dvt run` | Yes | Execute models with federation routing |
| `dvt build` | Yes | Seeds + models + snapshots + tests |
| `dvt compile` | Yes | Target-aware SQL compilation |
| `dvt seed` | Yes | Spark-based seed loading (CSV/Parquet/JSON/ORC) |
| `dvt show` | Yes | Target-aware preview |
| `dvt retry` | Yes | Federation-aware retry |
| `dvt sync` | **DVT-only** | Install adapters, PySpark, JDBC drivers, cloud JARs |
| `dvt init` | Extended | Project init + computes/buckets.yml templates |
| `dvt debug` | Extended | Inspect targets, computes, buckets |
| `dvt test` | No | Run tests (inherited) |
| `dvt snapshot` | No | Execute snapshots (inherited) |
| `dvt docs` | Yes | Generate/serve docs |
| `dvt deps` | No | Install packages (inherited) |
| `dvt clean` | Extended | Clean staging + cloud bucket files |
| `dvt list` | No | List resources (inherited) |

### 3.2 Task Layer (`dvt/task/` + `dvt/dvt_tasks/`) -- 31 standard + 7 DVT-specific

**Task hierarchy:**

```
BaseTask
 +-- CleanTask
 +-- ConfiguredTask (RuntimeConfig + Compiler)
 |    +-- GraphRunnableTask (DAG traversal + threading)
 |    |    +-- CloneTask
 |    |    +-- CompileTask ---> DvtCompileTask
 |    |    |    +-- RunTask ---> DvtRunTask (three-way runner routing)
 |    |    |    |    +-- BuildTask ---> DvtBuildTask
 |    |    |    |    +-- SeedTask ---> DvtSeedTask (Spark seeds)
 |    |    |    |    +-- FreshnessTask
 |    |    |    |    +-- SnapshotTask
 |    |    |    |    +-- TestTask
 |    |    |    +-- ShowTask ---> DvtShowTask
 |    |    |    +-- GenerateTask ---> DvtDocsGenerateTask
 |    |    +-- ListTask
 |    +-- RetryTask ---> DvtRetryTask
 |    +-- RunOperationTask
 |    +-- ServeTask
 +-- DebugTask
 +-- DepsTask
 +-- InitTask
```

### 3.3 Runner Layer (`dvt/dvt_runners/`) -- 4 files

Three execution paths per model:

| Runner | When Used | Mechanism |
|--------|-----------|-----------|
| **Standard ModelRunner** | All deps on default target | Standard dbt adapter execution |
| **NonDefaultPushdownRunner** | All deps on same non-default target | Execute DDL + SQL on target adapter |
| **FederationModelRunner** | Cross-target dependencies | Extract -> Spark transform -> Load |

**Runner hierarchy:**

```
BaseRunner
 +-- CompileRunner
 |    +-- ModelRunner (standard dbt)
 |    |    +-- SeedRunner
 |    |    +-- SnapshotRunner
 |    +-- ShowRunner
 |    +-- TestRunner
 +-- FreshnessRunner
 +-- SavedQueryRunner

DVT Extensions:
 DvtRunnerMixin (shared DVT behavior)
   +-- FederationModelRunner (cross-target via Spark)
   +-- NonDefaultPushdownRunner (non-default adapter pushdown)
```

### 3.4 Federation Engine (`dvt/federation/`) -- 63 files

The heart of DVT. Handles cross-database query execution.

| Component | Files | Lines | Purpose |
|-----------|:-----:|:-----:|---------|
| **Resolver** | 1 | 476 | Determines PUSHDOWN vs FEDERATION per model |
| **Engine** | 1 | 802 | 9-step pipeline: extract -> stage -> register -> translate -> execute -> load |
| **EL Layer** | 1 | 457 | Orchestrates extraction with skip/refresh logic |
| **Query Optimizer** | 3 | 595 | Predicate pushdown + column pruning via SQLGlot |
| **Spark Manager** | 1 | 463 | Singleton Spark session, JDBC URL building for 20+ DBs |
| **Cloud Storage** | 1 | 470 | S3/GCS/Azure abstraction with multi-auth |
| **State Manager** | 1 | 626 | Extraction state, schema change detection, row hashing |
| **Adapter Manager** | 1 | 377 | Thread-safe cached adapter instances per (profile, target) |
| **Credentials** | 1 | -- | Dynamic credential extraction from adapter dataclasses |
| **Dialect Fallbacks** | 1 | 564 | 30+ dialect configs for LIMIT/OFFSET/SAMPLE/quoting |
| **Auth Handlers** | 22 | -- | Per-DB authentication (password, OAuth, IAM, keypair, etc.) |
| **Extractors** | 23 | -- | Per-DB extraction with native export optimization |
| **Loaders** | 7 | -- | Per-DB loading with bulk COPY optimization |

**Federation Engine 9-Step Pipeline:**

```
1. Get SparkManager instance
2. Create EL layer for staging
3. Build source mappings (source_id -> SQL alias via SQLGlot)
4. Extract pushable operations from compiled SQL
5. Extract sources to staging bucket (with predicate pushdown)
6. Register staged Parquet as Spark temp views
7. Translate SQL: model dialect -> Spark SQL (via SQLGlot)
8. Execute in Spark
9. Write results to target (bucket COPY or JDBC fallback)
```

### 3.5 DVT Compiler (`dvt/dvt_compilation/`) -- 583 lines

Extends dbt's compiler with:
- Source-dialect Jinja rendering (`ref()`/`source()` resolved in source dialect)
- Cross-dialect SQL transpilation via **SQLGlot**
- CTE handling with adapter passthrough

### 3.6 Configuration System (`dvt/config/`)

| File | Location | Purpose |
|------|----------|---------|
| `profiles.yml` | `~/.dvt/` | Database connections (dbt-compatible) |
| `computes.yml` | `~/.dvt/` | Spark engine configs per profile |
| `buckets.yml` | `~/.dvt/` | Staging bucket configs (S3/GCS/Azure/filesystem) |
| `dbt_project.yml` | Project root | Project config (dbt-compatible + `require-adapters`) |
| `mdm.duckdb` | `~/.dvt/data/` | MDM database (DuckDB) |

**Resolution hierarchy** (consistent across target, compute, and bucket):
```
CLI flag  >  model config  >  yml default
```

**`computes.yml` structure:**
```yaml
<profile_name>:
  target: <compute_name>       # Default active compute
  computes:
    <compute_name>:
      type: spark
      version: "4.0.0"        # PySpark version
      master: "local[*]"
      config:
        spark.driver.memory: "2g"
        spark.executor.memory: "2g"
      jdbc_extraction:
        num_partitions: 8
        fetch_size: 10000
      jdbc_load:
        num_partitions: 4
        batch_size: 10000
```

**`buckets.yml` structure:**
```yaml
<profile_name>:
  target: local
  buckets:
    local:
      type: filesystem
    s3_staging:
      type: s3
      bucket: my-bucket
      prefix: staging/
      region: us-east-1
    gcs_staging:
      type: gcs
      bucket: my-bucket
      keyfile: /path/to/sa.json
    azure_staging:
      type: azure
      container: dvt-staging
      storage_account: myaccount
```

### 3.7 Sync Infrastructure (`dvt/task/sync.py`) -- 734 lines

`dvt sync` performs 8 sequential steps:

```
Step 1: Resolve Python venv (.venv / venv / env)
Step 2: Detect package manager (uv preferred, pip fallback)
Step 3: Install dbt adapters from profiles.yml + require-adapters
Step 4: Install PySpark version from computes.yml (Java compat check)
Step 5: Download JDBC drivers (25+ adapters, Maven Central)
Step 6: Download native connectors (Snowflake, BigQuery, Redshift)
Step 7: Install cloud storage Python packages (boto3, gcs, azure)
Step 8: Download Hadoop cloud connector JARs (version-matched)
```

Supporting modules:
- `jdbc_drivers.py` (200 lines) -- Maven coordinate registry for 25 adapter types
- `native_connectors.py` (225 lines) -- Spark native connector JARs (Snowflake, BigQuery, Redshift)
- `cloud_connectors.py` (359 lines) -- Hadoop cloud connector JARs (S3/GCS/Azure)

### 3.8 Contracts & Artifacts

**Core Contracts (`dvt/contracts/`):**
- `graph/nodes.py` -- 35+ node classes (ModelNode, SeedNode, SnapshotNode, etc.)
- `graph/manifest.py` -- Manifest (central DAG data structure) + 10 lookup classes
- `graph/unparsed.py` -- 40+ raw/unparsed YAML types
- `files.py`, `project.py`, `results.py`, `selection.py`, `sql.py`, `state.py`

**Artifacts (`dvt/artifacts/`):**
- `resources/v1/` -- 24 resource definition files (~90+ classes)
- Versioned schemas: Manifest v12, Run Results v5, Catalog v1, Freshness v3

### 3.9 Other Inherited Modules

| Module | Files | Purpose |
|--------|:-----:|---------|
| `parser/` | 28 | YAML/SQL parsing, validation, Python object construction |
| `graph/` | 10 | DAG construction via networkx, node selection logic |
| `context/` | 15 | Jinja context building and exposure |
| `events/` | 7 | Structured logging / protobuf event system |
| `deps/` | 9 | Package dependency management (git, registry, tarball) |
| `clients/` | 9 | External clients (git, Jinja, YAML, registry) |
| `plugins/` | 6 | Plugin system |
| `include/` | 5 | Bundled project templates |

---

## 4. Database Support Matrix

### Tier 1: Full Optimized (Native Export + Bulk Load)

| Database | Extractor | Loader | Native Export Path |
|----------|:---------:|:------:|-------------------|
| PostgreSQL (+6 compat*) | COPY TO STDOUT | COPY FROM | -- |
| Snowflake | COPY INTO | COPY INTO | S3/GCS/Azure |
| BigQuery | EXPORT DATA | LOAD DATA | GCS |
| Redshift | UNLOAD | COPY | S3 |
| Databricks | COPY INTO | COPY INTO | S3/GCS/Azure/HDFS |

*PostgreSQL-compatible: CockroachDB, Greenplum, AlloyDB, Materialize, Citus, TimescaleDB, Neon

### Tier 2: Full Support (DB-Specific Extractor + JDBC Loader)

| Database | Extractor | Auth Handler | Compatible DBs |
|----------|:---------:|:------------:|----------------|
| MySQL | Yes | Yes | MariaDB, TiDB, SingleStore, PlanetScale, Vitess, Aurora MySQL |
| Oracle | Yes | Yes | -- |
| SQL Server | Yes | Yes | Synapse, Fabric |
| DuckDB | Yes (COPY TO) | Yes | -- |
| Trino/Starburst | Yes | Yes | -- |
| Athena | Yes (CTAS) | Yes | -- |
| ClickHouse | Yes | Yes | -- |
| Vertica | Yes | Yes | -- |
| Exasol | Yes | Yes | -- |
| DB2 | Yes | Yes | -- |
| Teradata | Yes | Yes | -- |
| Hive/Impala | Yes | Yes | -- |
| Firebolt | Yes | Yes | -- |
| Spark | Yes | Yes | -- |

### Tier 3: Generic (JDBC Only)

Any database with a JDBC driver works through GenericExtractor + GenericLoader.

**Totals:**
- **34+ named adapter types** mapped to extractors
- **20+ JDBC URL builders** in SparkManager
- **30+ dialects** in dialect_fallbacks

---

## 5. Implementation Status

### 5.1 Fully Implemented and Working

| Feature | Component | Detail |
|---------|-----------|--------|
| Federation resolver | `federation/resolver.py` | PUSHDOWN vs FEDERATION routing per model |
| Federation engine | `federation/engine.py` | 9-step pipeline (extract -> Spark -> load) |
| DVT compiler | `dvt_compilation/dvt_compiler.py` | Cross-dialect transpilation via SQLGlot |
| Three-way runner routing | `dvt_tasks/dvt_run.py` | Federation / NonDefault Pushdown / Standard |
| 20+ DB extractors | `federation/extractors/` | With native export for Tier 1 DBs |
| 6 bulk loaders | `federation/loaders/` | PostgreSQL, Snowflake, BigQuery, Redshift, Databricks, Generic |
| 22 auth handlers | `federation/auth/` | All major auth methods per DB |
| Cloud storage | `federation/cloud_storage.py` | S3, GCS, Azure with multi-auth |
| Spark session mgmt | `federation/spark_manager.py` | Singleton, JDBC URL building for 20+ DBs |
| State manager | `federation/state_manager.py` | Schema change detection, consecutive run skip |
| Predicate pushdown | `federation/query_optimizer.py` | Predicate extraction & cross-dialect transpilation |
| Spark-based seeding | `task/spark_seed.py` | Multi-format (CSV/Parquet/JSON/ORC), 12+ DB targets |
| `dvt sync` | `task/sync.py` | 8-step env setup (adapters, PySpark, JDBC, cloud JARs) |
| `dvt init` | `task/init.py` | Project init + computes/buckets templates |
| `dvt debug` | `task/debug.py` | Extended with target/compute/bucket inspection |
| Materialization coercion | resolver | Views -> tables for cross-target (DVT001 warning) |
| Dialect fallbacks | `federation/dialect_fallbacks.py` | 30+ dialects for LIMIT/OFFSET/SAMPLE/quoting |
| Adapter manager | `federation/adapter_manager.py` | Thread-safe cached adapters per (profile, target) |
| Credential extraction | `federation/credentials.py` | Dynamic extraction from adapter dataclasses |

### 5.2 Partially Implemented

| Feature | Status | Gap |
|---------|--------|-----|
| **Column projection pushdown** | Code exists, **disabled** | Case sensitivity bug with PostgreSQL. SQLGlot lowercases unquoted columns but PostgreSQL preserves case for quoted identifiers. TODO in `engine.py:397`. |
| **Incremental delta extraction** | Framework built, **not wired** | StateManager stores row hashes, extractors have `extract_hashes()`, `get_changed_pks()` returns new/changed/deleted PKs. But EL layer only handles "skip" and full extraction -- delta path not connected. |

### 5.3 Not Implemented (Stubs / Empty)

| Feature | Location | Status |
|---------|----------|--------|
| `virtualization/` | `dvt/virtualization/` | Empty `__init__.py` only. Likely planned for virtual table/view abstraction layer. |
| `execution/` | `dvt/execution/` | Empty `__init__.py` only. Likely planned for execution engine abstraction. |

---

## 6. Test Coverage

### 6.1 DVT-Specific Tests (8 files)

| Test File | Lines | What It Tests |
|-----------|:-----:|---------------|
| `tests/unit/federation/test_resolver.py` | ~200 | Federation path determination (pushdown vs federation) |
| `tests/unit/federation/test_query_optimizer.py` | ~200 | SQLGlot predicate pushdown, column projection, limit optimization |
| `tests/unit/federation/test_state_manager.py` | ~200 | State persistence, row-level change detection, schema hashing |
| `tests/unit/test_federation_auth.py` | 930 | Auth handlers for 28+ adapters (all auth methods) |
| `tests/unit/test_native_sql_execution.py` | 670 | Native SQL execution for 9+ DB families |
| `tests/unit/test_credentials.py` | ~100 | Dynamic credential extraction |
| `tests/unit/task/test_jdbc_drivers.py` | ~80 | JDBC driver registry, deduplication, Maven coordinates |
| `tests/unit/task/test_sync.py` | ~80 | SyncTask mocked flow |

### 6.2 Inherited dbt-core Tests

- **Unit tests** (`tests/unit/`): ~90+ files across `artifacts/`, `cli/`, `clients/`, `config/`, `context/`, `contracts/`, `deps/`, `events/`, `graph/`, `parser/`, `task/`, `utils/`
- **Functional tests** (`tests/functional/`): ~60+ files, end-to-end against Postgres

### 6.3 Coverage Gaps (No Automated Tests)

| Component | File(s) | Risk |
|-----------|---------|------|
| **Federation engine** | `federation/engine.py` (802 lines) | HIGH -- main execution pipeline |
| **EL layer** | `federation/el_layer.py` (457 lines) | HIGH -- extraction orchestration |
| **Spark manager** | `federation/spark_manager.py` (463 lines) | MEDIUM -- session lifecycle |
| **All extractors** | `federation/extractors/` (23 files) | HIGH -- per-DB extraction logic |
| **All loaders** | `federation/loaders/` (7 files) | HIGH -- per-DB loading logic |
| **Cloud storage** | `federation/cloud_storage.py` (470 lines) | MEDIUM -- S3/GCS/Azure abstraction |
| **SparkSeedRunner** | `task/spark_seed.py` (1,080 lines) | HIGH -- complex multi-DB seed loading |
| **DVT tasks** | `dvt_tasks/` (7 files) | MEDIUM -- task wrappers |
| **dvt init** | `task/init.py` | LOW -- project initialization |
| **dvt debug** | `task/debug.py` | LOW -- diagnostics |
| **Dialect fallbacks** | `federation/dialect_fallbacks.py` (564 lines) | MEDIUM -- 30+ dialect configs |
| **Native connectors** | `task/native_connectors.py` | LOW -- download logic |
| **Cloud connectors** | `task/cloud_connectors.py` | LOW -- download logic |

---

## 7. Dependencies

### Core Dependencies

**From dbt-core (pinned):**
- `agate`, `Jinja2`, `mashumaro[msgpack]`, `click`, `jsonschema`, `networkx`, `protobuf`, `requests`, `snowplow-tracker`, `pathspec`, `sqlparse`

**dbt ecosystem:**
- `dbt-extractor`, `dbt-semantic-interfaces`, `dbt-common`, `dbt-adapters`, `dbt-protos`

**DVT-specific additions:**
- `sqlglot>=20.0.0` -- SQL dialect translation
- `duckdb>=0.9.0` -- MDM database
- `pyarrow>=14.0.0` -- columnar data format
- `pydantic<3` -- data validation
- `daff>=1.3.46` -- data diff

**Deliberately excluded:** PySpark (installed per-project by `dvt sync`)

### Build System
- **Build tool:** Hatch (`hatchling` backend)
- **Package name:** `dvt-core`
- **Python:** `>=3.10` (supports 3.10-3.13, CPython + PyPy)
- **Entry point:** `dvt = "dvt.cli.main:cli"`

---

## 8. Key Design Decisions

1. **PySpark is NOT a package dependency** -- installed per-project via `dvt sync` + `computes.yml` version pinning, allowing different projects to use different Spark versions.

2. **Three config files** (`profiles.yml`, `computes.yml`, `buckets.yml`) with consistent resolution hierarchy: CLI > model config > yml default.

3. **SQLGlot for transpilation** with manual fallback when SQLGlot cannot handle edge cases.

4. **Staging via cloud storage** (S3/GCS/Azure) or local filesystem for data transfer between databases during federation.

5. **Rebase compatibility** -- DVT additions are isolated in `dvt_tasks/`, `dvt_runners/`, `dvt_compilation/`, `federation/` to minimize conflicts when rebasing onto upstream dbt-core.

6. **Three-way execution routing** per model: Standard dbt (same default target) / Non-default pushdown (same non-default target) / Spark federation (cross-target).

7. **Materialization coercion** -- Cross-target views are automatically promoted to tables (cannot create a view across databases), with DVT001 warning.

---

## 9. Module File Counts Summary

| Module | Files | DVT-Specific? |
|--------|:-----:|:---:|
| `cli/` | 13 | Extended |
| `task/` | 31 | Inherited |
| `dvt_tasks/` | 9 | Yes |
| `dvt_runners/` | 6 | Yes |
| `dvt_compilation/` | 3 | Yes |
| `federation/` | 17 core + 22 auth + 23 extractors + 7 loaders = **69** | Yes |
| `config/` | 11 | Extended |
| `contracts/` | 20 | Inherited |
| `artifacts/` | ~47 | Inherited |
| `parser/` | 28 | Inherited |
| `graph/` | 10 | Inherited |
| `context/` | 15 | Inherited |
| `events/` | 7 | Inherited |
| `deps/` | 9 | Inherited |
| `clients/` | 9 | Inherited |
| `plugins/` | 6 | Inherited |
| `virtualization/` | 1 (empty) | Stub |
| `execution/` | 1 (empty) | Stub |
| **Total** | **~300+** | **~87 DVT-specific** |
