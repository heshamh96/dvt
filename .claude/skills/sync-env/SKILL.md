---
name: sync-env
description: Sync DVT development environment
---

Synchronize the DVT development environment to ensure all dependencies are up to date.

## Steps

### 1. Sync Core Dependencies
```bash
cd /Users/hex/Documents/My_Projects/DVT/dvt-ce/core
uv sync
```

### 2. Verify Sling Installation
```bash
# Check Sling is installed
sling --version

# If not installed (macOS):
brew install slingdata-io/sling/sling

# If not installed (Linux):
curl -fsSL https://raw.githubusercontent.com/slingdata-io/sling-cli/main/scripts/install.sh | bash
```

### 3. Verify DuckDB
```bash
python -c "import duckdb; print('DuckDB', duckdb.__version__)"
```

### 4. Install DuckDB Extensions (if needed)
```bash
python -c "
import duckdb
conn = duckdb.connect()
conn.execute('INSTALL httpfs')
conn.execute('INSTALL parquet')
conn.execute('INSTALL json')
print('DuckDB extensions installed')
conn.close()
"
```

### 5. Install Pre-commit Hooks
```bash
cd /Users/hex/Documents/My_Projects/DVT/dvt-ce/core
hatch run setup
```

### 6. Verify Installation
```bash
uv run dvt --version
```

### 7. Optional: Reset Environment
If there are dependency issues:
```bash
cd /Users/hex/Documents/My_Projects/DVT/dvt-ce/core
rm -rf .venv
uv sync
```

## When to Use

- After pulling new changes
- After modifying `pyproject.toml`
- When dependencies seem out of sync
- After switching branches
- When Sling or DuckDB need updating

## Troubleshooting

**Lock file conflict:**
```bash
uv lock --upgrade
uv sync
```

**Pre-commit hook issues:**
```bash
pre-commit clean
pre-commit install
```

**Sling connection issues:**
```bash
sling conns discover local
sling conns test <connection_name>
```

**DuckDB import error:**
```bash
uv add duckdb --upgrade
```
