---
name: lint
description: Run all code quality checks on DVT core
---

Run the complete code quality suite for DVT.

## Command

```bash
cd /Users/hex/Documents/My_Projects/DVT/dvt-core/core && hatch run code-quality
```

## What It Runs

1. **black** - Code formatting check
2. **flake8** - Linting
3. **mypy** - Type checking
4. **pre-commit hooks** - All configured hooks

## Quick Fixes

If black fails:
```bash
cd /Users/hex/Documents/My_Projects/DVT/dvt-core/core && hatch run black --fix
```

If there are many issues, fix them one category at a time:
```bash
hatch run black    # Format first
hatch run flake8   # Then lint
hatch run mypy     # Then types
```
