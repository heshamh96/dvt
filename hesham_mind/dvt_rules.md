# DVT Rules & Specifications

> **Document Version:** 2.0  
> **Last Updated:** February 2026  
> **Status:** Authoritative Reference

This document defines the complete ruleset for DVT (Data Virtualization Tool) behavior. All implementation decisions must conform to these rules.

---

## Table of Contents

1. [Core Principles](#1-core-principles)
2. [Compute Rules](#2-compute-rules)
3. [Target Rules](#3-target-rules)
4. [Materialization Rules](#4-materialization-rules)
5. [DAG & Execution Rules](#5-dag--execution-rules)
6. [Writing Rules](#6-writing-rules)
7. [Seed Rules](#7-seed-rules)
8. [Test Rules](#8-test-rules)
9. [Hook Rules](#9-hook-rules)
10. [Edge Cases & Validation](#10-edge-cases--validation)
11. [Backward Compatibility](#11-backward-compatibility)

---

## 1. Core Principles

### 1.1 Pushdown Preference

DVT **always prioritizes Adapter Pushdown** over Federation:

```
IF model.target == ALL upstream.targets THEN
    USE Adapter Pushdown (SQL execution on target database)
ELSE
    USE Federation Path (Spark compute engine)
```

**Rationale:** Adapter pushdown is faster, requires less infrastructure, and maintains native database optimizations.

### 1.2 Federation as Fallback

Federation (Spark compute) is **only used when necessary**:
- When a model's target differs from at least one upstream source/model target
- When CLI `--target` forces a global target override

### 1.3 Filter Optimization

When using Federation Path, DVT **always applies Predicate Pushdown**:
- WHERE clauses are pushed to source databases
- Minimizes data transfer over the network
- Reduces Spark processing overhead

### 1.4 Backward Compatibility

When using a single adapter (single target), DVT behaves **identically to dbt**:
- All models use adapter pushdown
- No Spark infrastructure required
- Existing dbt projects work without modification

---

## 2. Compute Rules

### 2.1 Compute Selection Hierarchy

```
Priority (Highest to Lowest):
1. CLI: --compute <name>           (overrides everything)
2. Model Config: compute: <name>   (overrides default)
3. computes.yml: default compute   (base default)
```

### 2.2 computes.yml Requirements

**RULE 2.2.1:** `computes.yml` is **REQUIRED** for DVT operation.

```yaml
# ~/.dvt/computes.yml (REQUIRED)
default:                    # Profile name
  target: default           # Active compute for this profile
  computes:
    default:                # Compute name
      type: spark
      version: "3.5.0"
      master: "local[*]"
      config:
        spark.driver.memory: "2g"
        spark.sql.adaptive.enabled: "true"
```

**RULE 2.2.2:** If `computes.yml` does not exist or has no default compute:
- DVT raises a **compilation error**: `"computes.yml not found or has no default compute. Run 'dvt init' to create one."`
- DVT does **NOT** fall back to a built-in default

### 2.3 Compute Validation

**RULE 2.3.1:** Compute validation happens at **parse time** (compilation).

**RULE 2.3.1.1 :** Compute activeness happens at **debug time** (compilation).

**RULE 2.3.2:** If a specified compute (via model config or CLI) does not exist in `computes.yml`:
- DVT raises a **compilation error**: `"Compute '<name>' not found in computes.yml"`
- DVT does **NOT** fall back to default
- Execution stops immediately

### 2.4 Compute Scope

**RULE 2.4.1:** Compute engines are **only used for Federation Path models**.

**RULE 2.4.2:** Models eligible for Adapter Pushdown **ignore** the compute setting entirely.

**RULE 2.4.3:** Different models **MAY** use different compute engines in the same run:
- Each model's compute is resolved independently
- Multiple Spark sessions may be created if needed
- This allows per-model compute optimization

### 2.5 Compute Selection Summary

| Scenario | Compute Used |
|----------|--------------|
| Same-target model (pushdown) | None (adapter only) |
| Cross-target model, no config | Default from computes.yml |
| Cross-target model, config specified | Model's config compute |
| Any model, CLI --compute | CLI compute (for federation models only) |

---

## 3. Target Rules

### 3.1 Target Selection Hierarchy

```
Priority (Highest to Lowest):
1. CLI: --target <name>            (FORCES global override)
2. Model Config: target: <name>    (model-specific)
3. profiles.yml: default target    (base default)
```

### 3.2 Source Connection Requirements

**RULE 3.2.1:** All sources **MUST** have an explicit `connection` property.

```yaml
# schema.yml
sources:
  - name: postgres_app
    connection: postgres_prod    # REQUIRED: target name from profiles.yml
    tables:
      - name: users
      - name: orders
```

**RULE 3.2.2:** If a source does not have a `connection` property:
- DVT raises a **compilation error**: `"Source '<name>' must have a 'connection' property specifying its target"`
- DVT does **NOT** assume a default

**Rationale:** Explicit connections prevent ambiguity and ensure correct federation decisions.

### 3.3 Cross-Target References (ref)

**RULE 3.3.1:** When a model refs another model with a different target:
1. The ref'd model is **executed first** (respecting DAG order)
2. The ref'd model is **materialized to its target**
3. The referencing model reads from the ref'd model's target via Federation

```
Model A (target: postgres)
    |
    | refs
    v
Model B (target: snowflake)
    |
    | Federation reads Model A from postgres
    | Transforms in Spark
    | Writes to snowflake
    v
Result in Snowflake
```

### 3.4 Global Target Override (CLI)

**RULE 3.4.1:** When `--target` is specified via CLI:
- **ALL** models are forced to materialize in the specified target
- Any source not in this target triggers Federation Path
- Model-level target configs are **overridden**

**RULE 3.4.2:** Global target override implications:
```
dvt run --target snowflake_prod

Result:
- All models materialize to snowflake_prod
- Sources with connection != snowflake_prod → Federation
- Sources with connection == snowflake_prod → Pushdown
```

### 3.5 Target Resolution Summary

| Scenario | Model Target | Execution Path |
|----------|--------------|----------------|
| No config, no CLI | profiles.yml default | Depends on upstreams |
| Model config: target_x | target_x | Depends on upstreams |
| CLI: --target target_y | target_y (forced) | Likely Federation |
| Model config + CLI | CLI wins | Likely Federation |

---

## 4. Materialization Rules

### 4.1 Defaults & Validation

**RULE 4.1.1:** If materialization is unspecified:
- Use project default from `dbt_project.yml`
- If no project default, use `view`

**RULE 4.1.2:** If materialization is invalid/unknown:
- DVT raises a **compilation error**: `"Unknown materialization '<name>'"`
- DVT does **NOT** fall back to a default

### 4.2 Same-Target Execution (Adapter Pushdown)

**Condition:** `model.target == ALL upstream.targets`

| Materialization | Behavior |
|-----------------|----------|
| `table` | Create/replace table via adapter SQL |
| `view` | Create/replace view via adapter SQL |
| `incremental` | Apply incremental strategy via adapter SQL |
| `ephemeral` | Compile as CTE, inject into downstream queries |

### 4.3 Cross-Target Execution (Federation Path)

**Condition:** `model.target != at least one upstream.target`

| Materialization | Behavior |
|-----------------|----------|
| `table` | Spark reads sources via JDBC, transforms, writes table to target via JDBC |
| `view` | **COERCED TO TABLE** with warning (DVT001) |
| `incremental` | Spark reads sources, applies incremental strategy, writes to target via JDBC |
| `ephemeral` | Computed in Spark memory, passed as DataFrame to downstream |

### 4.4 View Coercion (DVT001)

**RULE 4.4.1:** Views **cannot** be created across databases.

**RULE 4.4.2:** When a view-materialized model requires Federation:
- DVT emits warning: `"DVT001: Model '<name>' is configured as view but requires federation. Materializing as table."`
- Model is materialized as **table**
- This is automatic coercion, not an error

### 4.5 Ephemeral on Federation Path

**RULE 4.5.1:** Ephemeral models on Federation Path are computed in Spark memory.

**RULE 4.5.2:** Ephemeral DataFrame caching:
- If an ephemeral model is referenced by **multiple** downstream models:
  - Compute **every time**
  - warn the user of the ephemeral model being used multiple times , advice him to materialize first

### 4.6 Incremental on Federation Path

**RULE 4.6.1:** Incremental models on Federation Path **honor the model's incremental strategy**.

**RULE 4.6.2:** Strategy translation to Spark:

| dbt Strategy | Spark Implementation |
|--------------|---------------------|
| `append` | DataFrame write with mode="append" |
| `merge` | MERGE INTO via Spark SQL (if target supports) or delete+insert |
| `delete+insert` | Delete matching rows, then insert new rows |
| `insert_overwrite` | Overwrite partitions |

**RULE 4.6.3:** If strategy cannot be translated:
- DVT emits warning: `"DVT003: Incremental strategy '<strategy>' may behave differently on federation path"`
- DVT proceeds with best-effort translation

### 4.7 Snapshot Materialization

**RULE 4.7.1:** Snapshots are **NOT supported** on Federation Path.

**RULE 4.7.2:** If a snapshot model requires Federation:
- DVT raises a **compilation error**: `"Snapshots require all upstream sources in the same target. Model '<name>' cannot use federation."`

**Rationale:** SCD Type 2 logic requires direct database operations that cannot be reliably federated.

### 4.8 Materialization Summary

| Materialization | Same-Target | Cross-Target (Federation) |
|-----------------|-------------|---------------------------|
| table | Adapter SQL | Spark + JDBC write |
| view | Adapter SQL | **Coerced to table** (DVT001) |
| incremental | Adapter SQL | Spark + strategy translation |
| ephemeral | CTE injection | DataFrame in memory (cached) |
| snapshot | Adapter SQL | **NOT SUPPORTED** (error) |

---

## 5. DAG & Execution Rules

### 5.1 Resolution Phase

DVT resolves the DAG in this order:

```
Step 1: Parse all models, sources, seeds, tests
Step 2: Resolve TARGET for every node
        - Apply CLI --target override (if specified)
        - Apply model config target (if specified)
        - Fall back to profiles.yml default
Step 3: Resolve EXECUTION PATH for every node
        - Compare node target vs all upstream targets
        - If match → Pushdown
        - If mismatch → Federation
Step 4: Resolve COMPUTE for federation nodes
        - Apply CLI --compute override (if specified)
        - Apply model config compute (if specified)
        - Fall back to computes.yml default
Step 5: Validate all configurations
        - Check compute exists
        - Check source connections exist
        - Check for circular dependencies
```

### 5.2 Execution Phase

**RULE 5.2.1:** DVT respects DAG execution order regardless of execution path.

```
Model A (pushdown) → Model B (federation) → Model C (pushdown)

Execution:
1. Execute Model A via adapter
2. Execute Model B via Spark (reads A via JDBC)
3. Execute Model C via adapter (reads B directly)
```

**RULE 5.2.2:** Execution path switching is seamless:
- Pushdown → Federation: Spark reads from adapter-created table via JDBC
- Federation → Pushdown: Adapter reads from Spark-created table directly

### 5.3 Parallel Execution

**RULE 5.3.1:** Parallel execution is controlled by `--threads` (like dbt).

**RULE 5.3.2:** Independent models (no DAG dependencies) **MAY** run in parallel:
- Pushdown models: Parallel via adapter connections
- Federation models: Parallel Spark jobs (if resources allow)

**RULE 5.3.3:** Default thread count: 4 (matches dbt default)

### 5.4 Error Handling

**RULE 5.4.1:** If a model fails during execution:
- Mark the model as **Error**
- Mark all downstream models as **Skipped**
- Continue executing independent models

**RULE 5.4.2:** `--fail-fast` flag:
- If specified: Stop entire run on first failure
- If not specified: Continue with independent models (default)

**RULE 5.4.3:** Error reporting includes:
- Which model failed
- Execution path used (pushdown/federation)
- Underlying error message
- Skipped downstream models

---

## 6. Writing Rules

### 6.1 Table Materialization Write Behavior

**RULE 6.1.1:** Default behavior (no `--full-refresh`):
- **Truncate** existing table
- **Insert** new data
- Preserves table structure, grants, etc.

**RULE 6.1.2:** With `--full-refresh`:
- **Drop** existing table (DROP CASCADE if needed)
- **Create** new table
- **Insert** data

**RULE 6.1.3:** This applies to both Pushdown and Federation paths.

### 6.2 Schema Evolution

**RULE 6.2.1:** If column types change between runs (without `--full-refresh`):
- DVT raises an **error**: `"Schema change detected for model '<name>'. Column '<col>' type changed from '<old>' to '<new>'. Use --full-refresh to apply schema changes."`
- Execution stops for this model

**RULE 6.2.2:** With `--full-refresh`:
- Table is dropped and recreated with new schema
- No error raised

**RULE 6.2.3:** Schema changes include:
- Column type changes
- Column additions (may work without full-refresh on some adapters)
- Column removals
- Column renames

### 6.3 Incremental Write Behavior

**RULE 6.3.1:** Incremental models follow their configured strategy.

**RULE 6.3.2:** With `--full-refresh`:
- Incremental model is treated as `table`
- Full data reload occurs

### 6.4 Seed Write Behavior

**RULE 6.4.1:** Seeds always use **full refresh** semantics:
- Drop existing table
- Create new table
- Load all data from CSV

**RULE 6.4.2:** No `--full-refresh` flag needed for seeds (always full).

---

## 7. Seed Rules

### 7.1 Spark-Based Loading

**RULE 7.1.1:** All seeds use **Spark** for CSV loading.

**RULE 7.1.2:** Spark seed loading provides:
- No memory limits (distributed processing)
- Better type inference
- Parallel JDBC writes
- Cross-target support

### 7.2 Cross-Target Seeding

**RULE 7.2.1:** Seeds can be loaded to any target via CLI:
```bash
dvt seed --target snowflake_prod
```

**RULE 7.2.2:** Target resolution for seeds:
```
Priority (Highest to Lowest):
1. CLI: --target <name>
2. profiles.yml: default target
```

### 7.3 Type Handling

**RULE 7.3.1:** Type inference:
- Spark infers types from CSV content
- Types are mapped to target database types

**RULE 7.3.2:** Type overrides via `column_types`:
```yaml
seeds:
  my_seed:
    +column_types:
      id: integer
      amount: numeric(10,2)
```

**RULE 7.3.3:** Unknown types default to STRING/VARCHAR.

### 7.4 Seed Configuration

```yaml
seeds:
  my_project:
    +delimiter: ","           # CSV delimiter
    +quote_columns: true      # Quote column names in DDL
    +column_types:            # Explicit type overrides
      id: integer
```

---

## 8. Test Rules

### 8.1 Test Execution Path

**RULE 8.1.1:** Tests **always** use the target adapter.

**RULE 8.1.2:** Tests **never** use Federation, even if the tested model was created via Federation.

**RULE 8.1.3:** Rationale:
- Tests verify data in the target database
- The model has already been materialized to target
- No cross-database queries needed for testing

### 8.2 Test Configuration

**RULE 8.2.1:** Test severity and thresholds work identically to dbt.

**RULE 8.2.2:** Tests run after their dependent models complete.

### 8.3 Cross-Target Test Limitations

**RULE 8.3.1:** Tests that would require cross-target queries are **NOT supported**.

**RULE 8.3.2:** If a test references models/sources in different targets:
- DVT raises a **compilation error**: `"Test '<name>' references nodes in different targets. Tests must reference nodes in the same target."`

**Guidance:** Test models only on their target database. If you need to verify cross-database consistency, create a model that joins the data and test that model.

---

## 9. Hook Rules

### 9.1 Hook Execution on Federation Path

**RULE 9.1.1:** `pre-hook` execution:
- Runs on **source adapter** (before Federation)
- Executes before Spark reads from sources
- Use for: source table preparation, logging, locks

**RULE 9.1.2:** `post-hook` execution:
- Runs on **target adapter** (after Federation)
- Executes after Spark writes to target
- Use for: grants, indexes, statistics, notifications

### 9.2 Hook Execution on Pushdown Path

**RULE 9.2.1:** Standard dbt behavior:
- `pre-hook` runs before model SQL
- `post-hook` runs after model SQL
- Both on the same adapter

### 9.3 Hook Configuration

```yaml
models:
  my_model:
    +pre-hook: "ANALYZE {{ this }}"
    +post-hook: 
      - "GRANT SELECT ON {{ this }} TO reporting_role"
      - "CALL refresh_cache('{{ this }}')"
```

### 9.4 Hook Limitations

**RULE 9.4.1:** Hooks are SQL statements, not Spark operations.

**RULE 9.4.2:** For Federation models with multiple sources in different targets:
- `pre-hook` runs on the **first source's** adapter
- Consider if this is the intended behavior

---

## 10. Edge Cases & Validation

### 10.1 No Upstream Dependencies

**RULE 10.1.1:** Models with no upstream dependencies (sources or refs):
- Use **target adapter** (Pushdown path)
- No Federation needed
- Example: `SELECT 1 AS id, 'test' AS name`

### 10.2 Duplicate Source Definitions

**RULE 10.2.1:** If the same source is defined with different `connection` values in different schema.yml files:
- DVT raises a **compilation error**: `"Source '<name>' has conflicting connection values: '<conn1>' and '<conn2>'"`

### 10.3 Self-Referencing Models

**RULE 10.3.1:** Models that read from and write to the same table:
- **Allowed only** for `incremental` materialization
- Required for incremental logic (reading existing data)

**RULE 10.3.2:** For non-incremental models:
- DVT raises a **compilation error**: `"Model '<name>' cannot read from itself unless using incremental materialization"`

### 10.4 Circular Target Dependencies

**RULE 10.4.1:** Circular dependencies across targets are **prevented by DAG resolution**.

**RULE 10.4.2:** The DAG ensures:
- Model A must complete before Model B (if B refs A)
- Even across targets, execution order is maintained

### 10.5 Empty Results

**RULE 10.5.1:** If a model produces zero rows:
- Table/view is still created (empty)
- Not treated as an error
- Downstream models may have zero rows as well

### 10.6 NULL Handling

**RULE 10.6.1:** NULL handling follows standard SQL semantics.

**RULE 10.6.2:** When federating between databases:
- NULL values are preserved
- Type coercion rules apply

---

## 11. Backward Compatibility

### 11.1 Single-Adapter Projects

**RULE 11.1.1:** Projects using only one adapter work **identically to dbt**:
- All models use Pushdown path
- No Spark infrastructure needed
- `computes.yml` still required but compute is never used
- All existing dbt functionality preserved

### 11.2 Migration from dbt

**RULE 11.2.1:** Existing dbt projects can migrate to DVT:
1. Install DVT
2. Run `dvt init` to create `computes.yml`
3. Add `connection` property to sources (if using federation)
4. Run `dvt run` - works like `dbt run` for single-adapter

### 11.3 dbt Compatibility Matrix

| dbt Feature | DVT Single-Adapter | DVT Multi-Adapter |
|-------------|-------------------|-------------------|
| Models | Identical | Federation when cross-target |
| Seeds | Spark-based (enhanced) | Spark-based + cross-target |
| Tests | Identical | Target adapter only |
| Snapshots | Identical | Same-target only |
| Hooks | Identical | Pre=source, Post=target |
| Macros | Identical | Identical |
| Packages | Identical | Identical |

---

## Appendix A: Warning & Error Codes

### Warnings

| Code | Message | Trigger |
|------|---------|---------|
| DVT001 | Materialization coerced | View → Table on federation |
| DVT002 | Type precision loss | Type mapping loses precision |
| DVT003 | Function translation | SQL function translated with potential differences |
| DVT004 | JSON handling | JSON semantics may differ |
| DVT005 | Predicate not pushed | WHERE clause executed in Spark |
| DVT010 | Column not found | column_types references missing column |
| DVT011 | Unknown type | Type string not recognized |

### Errors

| Code | Message | Trigger |
|------|---------|---------|
| DVT100 | computes.yml not found | Missing configuration file |
| DVT101 | Compute not found | Invalid compute name |
| DVT102 | Connection required | Source missing connection property |
| DVT103 | Conflicting connections | Same source, different connections |
| DVT104 | Snapshot federation | Snapshot requires federation |
| DVT105 | Cross-target test | Test references multiple targets |
| DVT106 | Self-reference | Non-incremental self-referencing model |
| DVT107 | Schema change | Column types changed without --full-refresh |

---

## Appendix B: Configuration Reference

### profiles.yml (Database Connections)

```yaml
my_project:
  target: dev
  outputs:
    dev:
      type: postgres
      host: localhost
      port: 5432
      user: dev_user
      password: "{{ env_var('DEV_PASSWORD') }}"
      database: dev_db
      schema: analytics
    
    prod:
      type: snowflake
      account: my_account
      user: prod_user
      password: "{{ env_var('PROD_PASSWORD') }}"
      database: PROD_DB
      warehouse: COMPUTE_WH
      schema: ANALYTICS
```

### computes.yml (Spark Configurations)

```yaml
my_project:
  target: default
  computes:
    default:
      type: spark
      version: "3.5.0"
      master: "local[*]"
      config:
        spark.driver.memory: "2g"
        spark.executor.memory: "4g"
        spark.sql.adaptive.enabled: "true"
    
    large:
      type: spark
      version: "3.5.0"
      master: "spark://cluster:7077"
      config:
        spark.driver.memory: "8g"
        spark.executor.memory: "16g"
        spark.executor.instances: "10"
```

### Model Config (Federation-Aware)

```yaml
# models/schema.yml
models:
  - name: cross_db_model
    config:
      target: snowflake_prod      # Override target
      compute: large              # Override compute (for federation)
      materialized: table
```

### Source Config (Connection Required)

```yaml
# models/sources.yml
sources:
  - name: postgres_app
    connection: postgres_prod     # REQUIRED
    database: app_db
    schema: public
    tables:
      - name: users
      - name: orders
  
  - name: mysql_legacy
    connection: mysql_prod        # REQUIRED
    database: legacy
    tables:
      - name: customers
```

---

## Appendix C: Decision Summary

| Topic | Decision |
|-------|----------|
| Missing computes.yml | Error (required) |
| Multiple computes per run | Yes, per-model |
| Source without connection | Error (required) |
| Cross-target refs | Execute ref'd first, then federate |
| Incremental on federation | Honor model's strategy |
| Ephemeral caching | Compute once, cache, clear after run |
| Snapshots on federation | Not supported (error) |
| Failed model handling | Skip downstream (dbt behavior) |
| Parallel execution | Configurable via --threads |
| Federation write mode | Truncate+Insert by default |
| Schema evolution | Error without --full-refresh |
| Tests on federation models | Always use target adapter |
| Pre-hook on federation | Run on source adapter |
| Post-hook on federation | Run on target adapter |
| No upstream model | Use target adapter (pushdown) |
| Duplicate source connections | Error |
| Self-referencing models | Only for incremental |

---

*End of DVT Rules Document*
