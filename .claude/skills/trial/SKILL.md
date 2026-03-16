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
3. Edit `pyproject.toml` to add dvt-ce and dvt-adapters as path dependencies with uv sources:

```toml
[project]
name = "trial-$ARGUMENTS"
version = "0.1.0"
requires-python = ">=3.9"
dependencies = [
    "dvt-ce",
    "dvt-adapters[postgres]",
]

[tool.uv.sources]
dvt-ce = { path = "/Users/hex/Documents/My_Projects/DVT/dvt-ce/core" }
dvt-adapters = { path = "/Users/hex/Documents/My_Projects/DVT/dvt-adapters" }
```

4. Run `uv sync`
5. Create `findings/` directory for notes

### Adding More Adapters

Add engine extras to dvt-adapters as needed:
```toml
dependencies = [
    "dvt-ce",
    "dvt-adapters[postgres,snowflake,databricks]",
]
```

Available extras: `postgres`, `snowflake`, `bigquery`, `redshift`, `databricks`, `duckdb`, `spark`, `sqlserver`, `fabric`, `mysql`, `oracle`, `all`

## After Creation

The trial is ready for:
```bash
cd /Users/hex/Documents/My_Projects/DVT/Testing_Factory/Testing_Playground/trial_$ARGUMENTS
uv run dvt init <project_name>
```

## Example

`/trial sling_extraction` creates:
- `/Users/hex/Documents/My_Projects/DVT/Testing_Factory/Testing_Playground/trial_sling_extraction/`
- With dvt-ce and dvt-adapters installed via uv path sources
- Ready for DVT project initialization
