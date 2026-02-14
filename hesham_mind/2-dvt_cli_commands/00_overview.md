# DVT CLI Commands Implementation Overview

## Purpose

This document provides a master overview of all DVT CLI command implementations required to support federation capabilities. It defines implementation phases, command dependencies, and priority order.

## Reference Documents

All implementation details are derived from these planning documents in `hesham_mind/`:

| Document | Purpose |
|----------|---------|
| `dvt_rules.md` | Authoritative rules for compute/target selection, materialization coercion |
| `federation_execution_plan.md` | Core federation engine: query decomposition, SQLGlot, Spark execution |
| `spark_seed_plan.md` | Enhanced seeding using Spark instead of agate |
| `connection_optimizations.md` | JDBC/native connectors, `buckets.yml`, CLI enhancements |
| `spark_optimizations.md` | Partitioned reading, retry strategies, compression |

---

## Implementation Phases

### Phase 1: Foundation (connection_optimizations.md)

**Goal**: Establish infrastructure for native connectors and bucket-based staging.

| Command | Type | Changes |
|---------|------|---------|
| `init` | Delta | Add `buckets.yml` template to `~/.dvt/` |
| `sync` | Delta | Download native connector JARs (Snowflake, BigQuery, Redshift) |
| `debug` | Delta | Add bucket configuration status, compute validation |
| `parse` | Delta | Validate all sources have `connection` property |

**Dependencies**: None (foundation layer)

### Phase 2: Spark Seeds (spark_seed_plan.md)

**Goal**: Replace agate-based seeding with Spark for better performance and cross-target support.

| Command | Type | Changes |
|---------|------|---------|
| `seed` | Full | Spark-based CSV loading, cross-target seeding support |

**Dependencies**: Phase 1 (buckets.yml for staging, native connectors)

### Phase 3: Core Rules Integration (dvt_rules.md)

**Goal**: Integrate DVT rules for target/compute resolution into execution commands.

| Command | Type | Changes |
|---------|------|---------|
| `run` | Full | Federation resolver, execution path selection (pushdown vs federation) |
| `test` | Modification | Ensure tests always execute on target adapter |
| `compile` | Full | Always compile using target adapter; fail on ephemeral without target |

**Dependencies**: Phase 1 (parse validation ensures sources have connections)

### Phase 4: Federation Execution (federation_execution_plan.md)

**Goal**: Full federation engine integration for cross-target query execution.

| Command | Type | Changes |
|---------|------|---------|
| `run` | Enhancement | Full federation engine (SQLGlot translation, Spark JDBC execution) |
| `build` | Full | Coordinate run + seed + test with federation awareness |
| `snapshot` | Modification | Explicit error when federation path detected (not supported) |

**Dependencies**: Phase 2 (seeds), Phase 3 (core rules)

### Phase 5: Optimizations (spark_optimizations.md) - Optional

**Goal**: Performance optimizations for federation execution.

| Command | Type | Changes |
|---------|------|---------|
| All federation commands | Enhancement | Partitioned reading, retry with backoff, compression |

**Dependencies**: Phase 4 (full federation working)

---

## Command Dependency Graph

```
                    ┌─────────────────────────────────────────────┐
                    │           Phase 1: Foundation               │
                    │  ┌──────┐ ┌──────┐ ┌───────┐ ┌───────┐     │
                    │  │ init │ │ sync │ │ debug │ │ parse │     │
                    │  └──┬───┘ └──┬───┘ └───┬───┘ └───┬───┘     │
                    └─────┼────────┼─────────┼─────────┼──────────┘
                          │        │         │         │
                          └────────┴────┬────┴─────────┘
                                        │
                    ┌───────────────────▼─────────────────────────┐
                    │           Phase 2: Spark Seeds              │
                    │              ┌──────┐                       │
                    │              │ seed │                       │
                    │              └──┬───┘                       │
                    └─────────────────┼───────────────────────────┘
                                      │
          ┌───────────────────────────┴───────────────────────────┐
          │                                                       │
          ▼                                                       ▼
┌─────────────────────────────────┐         ┌─────────────────────────────────┐
│     Phase 3: Core Rules         │         │     Phase 4: Federation         │
│  ┌─────┐ ┌──────┐ ┌─────────┐   │         │  ┌─────┐ ┌───────┐ ┌──────────┐ │
│  │ run │ │ test │ │ compile │   │────────▶│  │ run │ │ build │ │ snapshot │ │
│  └─────┘ └──────┘ └─────────┘   │         │  └─────┘ └───────┘ └──────────┘ │
└─────────────────────────────────┘         └─────────────────────────────────┘
                                                          │
                                                          ▼
                                            ┌─────────────────────────────────┐
                                            │   Phase 5: Optimizations        │
                                            │   (All federation commands)     │
                                            └─────────────────────────────────┘
```

---

## Implementation Priority Order

Based on dependencies and user requirements:

| Priority | Command | Phase | Rationale |
|----------|---------|-------|-----------|
| 1 | `init` | 1 | Foundation: creates `buckets.yml` template |
| 2 | `sync` | 1 | Foundation: downloads native connector JARs |
| 3 | `debug` | 1 | Foundation: validates configuration |
| 4 | `parse` | 1 | Foundation: enforces `connection` on sources |
| 5 | `seed` | 2 | Simpler than federation, validates Spark integration |
| 6 | `compile` | 3 | Core rule: target adapter compilation |
| 7 | `run` | 3+4 | Core execution with federation |
| 8 | `test` | 3 | Ensures tests use correct adapter |
| 9 | `build` | 4 | Orchestrates all commands |
| 10 | `snapshot` | 4 | Error handling for unsupported federation |

---

## File Index

| File | Command | Type | Phase |
|------|---------|------|-------|
| `01_init.md` | `dvt init` | Delta | 1 |
| `02_sync.md` | `dvt sync` | Delta | 1 |
| `03_debug.md` | `dvt debug` | Delta | 1 |
| `04_parse.md` | `dvt parse` | Delta | 1 |
| `05_seed.md` | `dvt seed` | Full | 2 |
| `06_run.md` | `dvt run` | Full | 3+4 |
| `07_test.md` | `dvt test` | Modification | 3 |
| `08_build.md` | `dvt build` | Full | 4 |
| `09_compile.md` | `dvt compile` | Full | 3 |
| `10_snapshot.md` | `dvt snapshot` | Modification | 4 |

---

## Key DVT Concepts (Quick Reference)

### Execution Paths

1. **Adapter Pushdown** (preferred): Execute SQL directly on target database when all dependencies share the same target
2. **Federation Path** (fallback): Use Spark JDBC when cross-engine queries are needed

### Resolution Priorities

**Target Resolution**:
```
CLI --target > model config target: > profiles.yml default
```

**Compute Resolution**:
```
CLI --compute > model config compute: > computes.yml default
```

### Materialization Coercion

When federation path is triggered:
- `view` → `table` (cannot create view across databases)
- `ephemeral` → Error (must materialize or set explicit target)

### Source Connection Requirement

Every source in DVT must have an explicit `connection` property:
```yaml
sources:
  - name: external_db
    connection: postgres_prod  # Required: specifies target
    tables:
      - name: users
```

---

## Success Criteria

Phase completion requires:

1. **Phase 1**: `dvt init` creates valid `buckets.yml`, `dvt sync` downloads all configured JARs, `dvt debug` shows complete status, `dvt parse` validates connections
2. **Phase 2**: `dvt seed` successfully loads CSVs via Spark to any target
3. **Phase 3**: `dvt run` correctly selects execution path, `dvt compile` uses target adapter, `dvt test` executes on correct adapter
4. **Phase 4**: Full cross-target queries work via federation, `dvt build` orchestrates correctly, `dvt snapshot` errors appropriately
5. **Phase 5**: Measurable performance improvements in federation queries
