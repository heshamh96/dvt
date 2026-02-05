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
cd /Users/hex/Documents/My_Projects/DVT/dvt-core/core && hatch run unit-tests
```

If path provided:
```bash
cd /Users/hex/Documents/My_Projects/DVT/dvt-core/core && python -m pytest $ARGUMENTS
```

## Other Test Commands

```bash
# Functional tests (requires Postgres)
hatch run integration-tests

# Stop on first failure
hatch run integration-tests-fail-fast

# With coverage
python -m pytest --cov=dvt tests/unit/
```

## Database Setup

For functional tests:
```bash
hatch run setup-db  # Starts Postgres via docker-compose
```
