---
name: debug-dvt
description: Debug DVT configuration and environment
---

Diagnose DVT configuration issues by checking all relevant files and settings.

## Checks to Perform

### 1. DVT Installation
```bash
cd /Users/hex/Documents/My_Projects/DVT/dvt-core/core
uv run dvt --version
```

### 2. User Configuration Files
Check these files exist and are valid:
- `~/.dvt/profiles.yml` - Database connections
- `~/.dvt/computes.yml` - Spark compute configs

### 3. Project Configuration
If in a DVT project directory:
- `dbt_project.yml` - Project settings
- `packages.yml` - Dependencies (if exists)

### 4. Environment
```bash
echo "Python: $(python --version)"
echo "DVT_DEV: $DVT_DEV"
echo "HATCH_PYTHON: $HATCH_PYTHON"
```

### 5. Common Issues

**"Profile not found"**
- Check `~/.dvt/profiles.yml` exists
- Verify profile name matches `dbt_project.yml`

**"Adapter not found"**
- Check `require-adapters` in `dbt_project.yml`
- Install adapter: `uv add dbt-<adapter>`

**"Compute engine not found"**
- Check `~/.dvt/computes.yml` has the compute defined
- Verify compute name spelling in model config

## Output

Report findings with:
- Configuration file contents (sanitized - no credentials)
- Any errors or warnings found
- Suggested fixes
