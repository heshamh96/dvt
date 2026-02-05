---
name: sync-env
description: Sync DVT development environment
---

Synchronize the DVT development environment to ensure all dependencies are up to date.

## Steps

### 1. Sync Core Dependencies
```bash
cd /Users/hex/Documents/My_Projects/DVT/dvt-core/core
uv sync
```

### 2. Install Pre-commit Hooks
```bash
cd /Users/hex/Documents/My_Projects/DVT/dvt-core/core
hatch run setup
```

### 3. Verify Installation
```bash
uv run dvt --version
```

### 4. Optional: Reset Environment
If there are dependency issues:
```bash
cd /Users/hex/Documents/My_Projects/DVT/dvt-core/core
rm -rf .venv
uv sync
```

## When to Use

- After pulling new changes
- After modifying `pyproject.toml`
- When dependencies seem out of sync
- After switching branches

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
