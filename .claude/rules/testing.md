# Testing Guidelines

## Test Structure

- **Unit tests**: `tests/unit/` - Pure Python, no DB required
- **Functional tests**: `tests/functional/` - End-to-end, requires Postgres

## Running Tests

```bash
# All unit tests
cd core && .venv/bin/python -m pytest ../tests/unit/ -v

# All functional tests (needs Postgres)
cd core && .venv/bin/python -m pytest ../tests/functional/ -v

# Specific test
cd core && .venv/bin/python -m pytest ../tests/unit/test_file.py::TestClass::test_method

# With coverage
cd core && .venv/bin/python -m pytest --cov=dvt ../tests/unit/
```

## Database Setup

```bash
cd core
hatch run setup-db  # Starts Postgres via docker-compose
```

## Writing Tests

### Unit Tests
- Test concrete classes, not abstract
- Use Hypothesis for property-based testing
- Mock external dependencies
- Keep tests focused and fast

### Functional Tests
- Test complete workflows
- Use fixtures for common setups
- Clean up test artifacts

## Trial-Based Testing

For exploratory testing, use Testing_Factory/Testing_Playground/:

```bash
# Create trial folder
mkdir ~/Documents/My_Projects/DVT/Testing_Factory/Testing_Playground/trial_<name>
cd trial_<name>
uv init
# Add dvt-ce and dvt-adapters as path dependencies in pyproject.toml:
#   [tool.uv.sources]
#   dvt-ce = { path = "/Users/hex/Documents/My_Projects/DVT/dvt-ce/core" }
#   dvt-adapters = { path = "/Users/hex/Documents/My_Projects/DVT/dvt-adapters" }
uv add dvt-ce dvt-adapters
uv sync
uv run dvt init <project_name>
```

Record findings in `trial_<name>/findings/`.

## Test Naming

```python
def test_<what>_<condition>_<expected>():
    """Test that <what> does <expected> when <condition>."""
```

Example:
```python
def test_extraction_path_triggers_when_targets_differ():
    """Test that extraction via Sling+DuckDB triggers when source and target engines differ."""
```
