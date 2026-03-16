---
name: debug-dvt
description: Debug DVT configuration and environment
---

Diagnose DVT configuration issues by checking all relevant files and settings.

## Checks to Perform

### 1. DVT Installation
```bash
cd /Users/hex/Documents/My_Projects/DVT/dvt-ce/core
uv run dvt --version
```

### 2. User Configuration Files
Check these files exist and are valid:
- `~/.dvt/profiles.yml` - Database connections
- `~/.dvt/data/mdm.duckdb` - MDM database
- `~/.dvt/cache/dvt_cache.duckdb` - DuckDB federation cache (created on first federation run)

### 3. Sling Connectivity
```bash
# Verify Sling is available
sling --version

# Test connection via Sling
sling conns test <connection_name>
```

### 4. DuckDB Cache
```bash
# Verify DuckDB is importable
python -c "import duckdb; print(duckdb.__version__)"

# Check DuckDB extensions installed
python -c "import duckdb; c=duckdb.connect(); c.execute('SELECT * FROM duckdb_extensions()').fetchdf()"
```

### 5. Project Configuration
If in a DVT project directory:
- `dbt_project.yml` - Project settings
- `packages.yml` - Dependencies (if exists)

### 6. Environment
```bash
echo "Python: $(python --version)"
echo "DVT_DEV: $DVT_DEV"
```

### 7. Common Issues

**"Profile not found"**
- Check `~/.dvt/profiles.yml` exists
- Verify profile name matches `dbt_project.yml`

**"Adapter not found"**
- Check `require-adapters` in `dbt_project.yml`
- Install adapter: `uv add dvt-adapters` (all engines in one package)

**"Sling not found"**
- Install Sling: `brew install slingdata-io/sling/sling` (macOS) or `curl -fsSL https://raw.githubusercontent.com/slingdata-io/sling-cli/main/scripts/install.sh | bash`

**"DuckDB extension not found"**
- Run `dvt sync` to install required DuckDB extensions

## Output

Report findings with:
- Configuration file contents (sanitized - no credentials)
- Any errors or warnings found
- Suggested fixes
