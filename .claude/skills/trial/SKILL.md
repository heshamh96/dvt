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

1. Create trial directory at `/Users/hex/Documents/My_Projects/DVT/Testing_Playground/trial_$ARGUMENTS`
2. Initialize with `uv init`
3. Add dvt-core as path dependency: `uv add dvt-core --path /Users/hex/Documents/My_Projects/DVT/dvt-core/core`
4. Add dbt-postgres adapter: `uv add dbt-postgres`
5. Run `uv sync`
6. Create `findings/` directory for notes

## After Creation

The trial is ready for:
```bash
cd /Users/hex/Documents/My_Projects/DVT/Testing_Playground/trial_$ARGUMENTS
uv run dvt init <project_name>
```

## Example

`/trial spark_jdbc` creates:
- `/Users/hex/Documents/My_Projects/DVT/Testing_Playground/trial_spark_jdbc/`
- With dvt-core and dbt-postgres installed
- Ready for DVT project initialization
