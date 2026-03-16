---
name: test
description: Run DVT tests (unit tests by default, or specific path)
arguments:
  - name: path
    description: Optional test path (e.g., tests/unit/test_config.py)
    required: false
---

Run DVT tests from the core directory.

## Usage

- `/test` - Run all unit tests
- `/test tests/unit/test_config.py` - Run specific test file
- `/test tests/unit/test_config.py::TestClass::test_method` - Run specific test

## Commands

If no arguments:
```bash
cd /Users/hex/Documents/My_Projects/DVT/dvt-ce/core && .venv/bin/python -m pytest ../tests/unit/ -v
```

If path provided:
```bash
cd /Users/hex/Documents/My_Projects/DVT/dvt-ce/core && .venv/bin/python -m pytest $ARGUMENTS
```

## Other Test Commands

```bash
# Functional tests (requires Postgres)
cd core && .venv/bin/python -m pytest ../tests/functional/ -v

# Stop on first failure
cd core && .venv/bin/python -m pytest ../tests/unit/ -v -x

# With coverage
cd core && .venv/bin/python -m pytest --cov=dvt ../tests/unit/

# Federation-specific unit tests
cd core && .venv/bin/python -m pytest ../tests/unit/federation/ -v
```

## Database Setup

For functional tests:
```bash
hatch run setup-db  # Starts Postgres via docker-compose
```
