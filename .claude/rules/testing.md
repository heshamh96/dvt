# Testing Guidelines

## Test Structure

- **Unit tests**: `tests/unit/` - Pure Python, no DB required
- **Functional tests**: `tests/functional/` - End-to-end, requires Postgres

## Running Tests

```bash
cd core

# All unit tests
hatch run unit-tests

# All functional tests (needs Postgres)
hatch run integration-tests
hatch run integration-tests-fail-fast  # Stop on first failure

# Specific test
python -m pytest tests/unit/test_file.py::TestClass::test_method

# With coverage
python -m pytest --cov=dvt tests/unit/
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

For exploratory testing, use Testing_Playground:

```bash
# Create trial folder
mkdir ~/Documents/My_Projects/DVT/Testing_Playground/trial_<name>
cd trial_<name>
uv init
uv add dvt-core --path /path/to/dvt-core/core
uv add dbt-postgres
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
def test_compute_resolution_cli_overrides_model_config():
    """Test that CLI --compute overrides model config compute setting."""
```
