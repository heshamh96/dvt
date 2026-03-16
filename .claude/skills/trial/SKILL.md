---
name: trial
description: Create a new test trial in Testing_Playground
arguments:
  - name: name
    description: Name for the trial (e.g., "federation_test")
    required: true
---

Create a new trial environment for testing DVT features.

## Steps

1. Create trial directory at `/Users/hex/Documents/My_Projects/DVT/Testing_Factory/Testing_Playground/trial_$ARGUMENTS`
2. Initialize with `uv init`
3. Edit `pyproject.toml` to add dvt-ce and dvt-adapters as editable path dependencies:

```toml
[project]
name = "trial-$ARGUMENTS"
version = "0.1.0"
requires-python = ">=3.12"
dependencies = [
    "dvt-ce",
    "dvt-adapters",
    "sling>=1.5.12",
    "dbt-semantic-interfaces>=0.7.4,<0.8",
    "snowplow-tracker>=1.0.2,<2.0",
]

[tool.uv.sources]
dvt-ce = { path = "../../../dvt-ce/core", editable = true }
dvt-adapters = { path = "../../../dvt-adapters", editable = true }
```

4. Run `uv sync`
5. Test `dvt init` to generate the project skeleton:

```bash
cd trial_$ARGUMENTS
dvt init <ProjectName>
```

This creates:
- `<ProjectName>/dbt_project.yml`
- `<ProjectName>/models/example/`
- `<ProjectName>/.gitignore` (includes `.dvt/`)
- Prompts for profile setup

6. Copy the real project content (models, seeds, sources.yml, profiles.yml) from an existing trial on top of the generated skeleton. The reference project is:
   `Testing_Factory/Testing_Playground/trial_19_dvt_ce_pypi/Coke_DB/`

7. Set up profiles.yml — either:
   - Copy to `~/.dvt/profiles.yml` (system-wide, no `--profiles-dir` needed)
   - Keep in `<ProjectName>/Connections/profiles.yml` (use `--profiles-dir`)

8. Run `dvt sync` to bootstrap the environment (installs drivers, DuckDB extensions, verifies Sling)

## Full Test Sequence (after project setup)

```bash
cd <ProjectName>

# Environment
dvt sync
dvt debug

# Parse + Compile
dvt parse
dvt compile

# List resources
dvt list
dvt list --resource-type model
dvt list --resource-type source

# Seeds (default + cross-target)
dvt seed --select test_seed
dvt seed --select test_seed --target mysql_docker
dvt seed --select test_seed --full-refresh

# Run models (all paths)
dvt run --select pushdown_pg                           # default pushdown
dvt run --select "pushdown_mysql pushdown_oracle"      # non-default pushdown
dvt run --select "mysql_to_pg cross_pg_mysql"          # extraction
dvt run --select incremental_mysql_to_pg               # incremental (run 1)
dvt run --select incremental_mysql_to_pg               # incremental (run 2, delta)
dvt run --select incremental_mysql_to_pg --full-refresh # full refresh

# Tests
dvt test

# Build (DAG order: seeds → models → snapshots → tests)
dvt build --select "test_seed+ pushdown_pg"

# Show (local DuckDB queries)
dvt compile --select pushdown_pg cross_pg_mysql
dvt show --select pushdown_pg
dvt show --inline "SELECT * FROM pg_docker.public.test_seed LIMIT 5"

# Snapshot
dvt snapshot

# Docs
dvt docs generate

# Source freshness (needs loaded_at_field + freshness in sources.yml)
dvt source freshness

# Retry (after a failed run)
dvt retry

# Clean
dvt clean
```

### Adding More Adapters

Add engine extras to dvt-adapters as needed:
```toml
dependencies = [
    "dvt-ce",
    "dvt-adapters[postgres,snowflake,databricks]",
]
```

Available extras: `postgres`, `snowflake`, `bigquery`, `redshift`, `databricks`, `duckdb`, `spark`, `sqlserver`, `fabric`, `mysql`, `oracle`, `all`

## Example

`/trial sling_extraction` creates:
- `/Users/hex/Documents/My_Projects/DVT/Testing_Factory/Testing_Playground/trial_sling_extraction/`
- With dvt-ce and dvt-adapters installed via uv editable path sources
- Ready for `dvt init <project_name>` then copy real project content
