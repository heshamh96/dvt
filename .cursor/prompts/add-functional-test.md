# Add Functional Test

Use this prompt when adding functional/integration tests.

## Context

I need to add a functional test for `<feature>` that tests against a real database.

## Requirements

1. **Test location**: `tests/functional/`

2. **Prerequisites**:
   ```bash
   cd core
   hatch run setup-db    # Start Postgres via docker-compose
   ```

3. **Test patterns**:
   - Use fixtures from `tests/functional/fixtures/`
   - Create minimal dbt projects for testing
   - Use `dbt_project_yml` and `models` fixtures
   - Clean up test artifacts

4. **Reference files**:
   - Test examples: `tests/functional/`
   - Fixtures: `tests/functional/fixtures/`
   - README: `tests/functional/README.md`

5. **Running tests**:
   ```bash
   cd core
   hatch run integration-tests                       # All functional tests
   hatch run integration-tests-fail-fast             # Stop on first failure
   python -m pytest tests/functional/sources/        # Specific directory
   ```

6. **Test structure**:
   ```python
   import pytest
   from dbt.tests.util import run_dbt

   class TestFeature:
       @pytest.fixture(scope="class")
       def models(self):
           return {"my_model.sql": "SELECT 1 as id"}

       def test_feature_works(self, project):
           results = run_dbt(["run"])
           assert len(results) == 1
   ```

## Agent

Use `dev-team-qa` rules when writing functional tests.
