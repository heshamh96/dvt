# DVT CLI Command: `dvt test`

## Phase: 3 (Core Rules Integration)
## Type: Modification

---

## Current Implementation Summary

**File**: `core/dvt/task/test.py`

The current `dvt test` command:
1. Compiles test SQL (schema tests + singular tests)
2. Executes tests against the target database
3. Reports pass/fail/warn/error status
4. Returns non-zero exit code if tests fail

---

## Required Changes

### Modification: Ensure Tests Execute on Target Adapter

Tests in DVT must always execute on the **model's target adapter**, not via federation. This ensures tests validate the actual data in the target database.

### Reference Document
- `dvt_rules.md` - Section: "Test Execution Rules"

---

## Implementation Details

### 1. Test Target Resolution

**File**: `core/dvt/task/test.py`

```python
class TestRunner(CompileRunner):
    """DVT Test Runner - always executes on target adapter."""
    
    def execute(self, test: TestNode, manifest: Manifest) -> RunResult:
        """Execute test on the model's target adapter.
        
        Tests always run on the target database, never via federation.
        This ensures tests validate the actual materialized data.
        """
        
        # Resolve target for the test
        target = self._resolve_test_target(test, manifest)
        
        # Get adapter for target
        adapter = self._get_adapter_for_target(target)
        
        # Execute test SQL on target
        return self._execute_test_on_adapter(test, adapter)
    
    def _resolve_test_target(self, test: TestNode, manifest: Manifest) -> str:
        """Resolve which target to run the test on.
        
        For tests, target is determined by the model being tested:
        1. Schema tests: Use the tested model's target
        2. Singular tests: Use the ref'd model's target (first ref if multiple)
        3. Source tests: Use the source's connection
        """
        
        # Get the node(s) this test depends on
        depends_on = test.depends_on.nodes
        
        if not depends_on:
            # No dependencies - use default target
            return self.config.profile.target
        
        # Get the primary tested node (first dependency)
        primary_node_id = depends_on[0]
        
        if primary_node_id.startswith("source."):
            # Source test - use source's connection
            source = manifest.sources.get(primary_node_id)
            if source and source.connection:
                return source.connection
        elif primary_node_id.startswith("model."):
            # Model test - resolve model's target
            model = manifest.nodes.get(primary_node_id)
            if model:
                return self._resolve_model_target(model)
        
        # Fallback to default
        return self.config.profile.target
    
    def _resolve_model_target(self, model: ModelNode) -> str:
        """Resolve target for a model (same logic as run)."""
        # CLI flag
        if self.config.args.target:
            return self.config.args.target
        
        # Model config
        if model.config.get("target"):
            return model.config["target"]
        
        # Profile default
        return self.config.profile.target
    
    def _execute_test_on_adapter(
        self,
        test: TestNode,
        adapter: BaseAdapter,
    ) -> RunResult:
        """Execute test SQL on the specified adapter."""
        
        fire_event(TestExecutingOnTarget(
            test=test.unique_id,
            target=adapter.config.target,
        ))
        
        try:
            # Execute the test query
            response, table = adapter.execute(test.compiled_code, fetch=True)
            
            # Interpret results
            if len(table.rows) == 0:
                # No rows returned = test passed
                return RunResult(
                    status=RunStatus.Success,
                    message="PASS",
                )
            else:
                # Rows returned = test failed
                num_failures = len(table.rows)
                
                # Check for warn threshold
                if test.config.get("warn_if") and num_failures <= test.config["warn_if"]:
                    return RunResult(
                        status=RunStatus.Warn,
                        message=f"WARN: {num_failures} failures",
                        failures=num_failures,
                    )
                
                return RunResult(
                    status=RunStatus.Fail,
                    message=f"FAIL: {num_failures} failures",
                    failures=num_failures,
                )
                
        except Exception as e:
            return RunResult(
                status=RunStatus.Error,
                message=f"ERROR: {e}",
            )
```

### 2. Test Task Modifications

**File**: `core/dvt/task/test.py`

```python
class TestTask(RunTask):
    """DVT Test Task - executes tests on target adapters."""
    
    def get_runner_type(self, node) -> Type[BaseRunner]:
        """Return TestRunner for test nodes."""
        if node.resource_type in ("test", "unit_test"):
            return TestRunner
        return super().get_runner_type(node)
    
    def before_run(self, adapter, selected_uids):
        """Prepare for test execution.
        
        Note: Tests do NOT use federation, so no Spark initialization needed.
        """
        # Group tests by target for efficient execution
        self.tests_by_target = self._group_tests_by_target(selected_uids)
        
        fire_event(TestTargetSummary(
            targets=list(self.tests_by_target.keys()),
            counts={t: len(tests) for t, tests in self.tests_by_target.items()},
        ))
        
        return super().before_run(adapter, selected_uids)
    
    def _group_tests_by_target(
        self,
        test_uids: List[str],
    ) -> Dict[str, List[str]]:
        """Group tests by their target for efficient execution."""
        groups = defaultdict(list)
        
        for uid in test_uids:
            test = self.manifest.nodes.get(uid)
            if test:
                target = self._resolve_test_target(test)
                groups[target].append(uid)
        
        return dict(groups)
```

### 3. Source Test Handling

**File**: `core/dvt/task/test.py`

```python
class SourceTestRunner(TestRunner):
    """Runner for source freshness and other source tests."""
    
    def _resolve_test_target(self, test: TestNode, manifest: Manifest) -> str:
        """Source tests always run on the source's connection target."""
        
        # Find the source this test is for
        for dep_id in test.depends_on.nodes:
            if dep_id.startswith("source."):
                source = manifest.sources.get(dep_id)
                if source and source.connection:
                    return source.connection
        
        raise TestConfigError(
            f"Source test '{test.unique_id}' could not determine target. "
            f"Ensure the source has a 'connection' property."
        )
```

### 4. New Events

**File**: `core/dvt/events/types.py`

```python
@dataclass
class TestExecutingOnTarget(DebugLevel):
    test: str
    target: str
    
    def message(self) -> str:
        return f"Test {self.test}: executing on {self.target}"


@dataclass
class TestTargetSummary(InfoLevel):
    targets: List[str]
    counts: Dict[str, int]
    
    def message(self) -> str:
        summary = ", ".join(f"{t}: {c}" for t, c in self.counts.items())
        return f"Tests by target: {summary}"
```

---

## CLI Output Changes

Before:
```
$ dvt test
Running with dbt=1.8.0
Found 15 tests

Running test not_null_users_id...
  PASS
Running test unique_users_email...
  PASS

Completed 15 tests: 14 passed, 1 failed
```

After:
```
$ dvt test
Running with dvt=1.8.0
Found 15 tests

Tests by target: postgres_prod: 8, snowflake_dw: 5, bigquery_analytics: 2

Running test not_null_users_id (postgres_prod)
  PASS
Running test unique_users_email (postgres_prod)
  PASS
Running test not_null_orders_id (snowflake_dw)
  PASS

Completed 15 tests: 14 passed, 1 failed
```

---

## Test Configuration Examples

### Schema Test (uses model's target)

```yaml
# models/staging/users.yml
version: 2

models:
  - name: users
    config:
      target: postgres_prod  # Tests will run on this target
    columns:
      - name: id
        tests:
          - not_null
          - unique
      - name: email
        tests:
          - not_null
```

### Source Test (uses source's connection)

```yaml
# models/staging/_sources.yml
version: 2

sources:
  - name: external_orders
    connection: bigquery_analytics  # Tests will run on this target
    freshness:
      warn_after: {count: 12, period: hour}
      error_after: {count: 24, period: hour}
    tables:
      - name: orders
        columns:
          - name: order_id
            tests:
              - not_null
              - unique
```

### Singular Test

```sql
-- tests/assert_positive_amounts.sql
-- This test refs a model on snowflake_dw, so runs on snowflake_dw

SELECT *
FROM {{ ref('orders') }}  -- orders model is on snowflake_dw
WHERE amount < 0
```

---

## Testing Requirements

### Unit Tests

**File**: `tests/unit/test_test_runner.py`

```python
def test_resolve_test_target_from_model():
    """Test target resolution from tested model."""
    test = MockTestNode(depends_on=["model.my_project.users"])
    model = MockModelNode(config={"target": "postgres_prod"})
    manifest = MockManifest(nodes={"model.my_project.users": model})
    
    runner = TestRunner(config, manifest, test)
    target = runner._resolve_test_target(test, manifest)
    
    assert target == "postgres_prod"


def test_resolve_test_target_from_source():
    """Test target resolution from tested source."""
    test = MockTestNode(depends_on=["source.my_project.external.orders"])
    source = MockSource(connection="bigquery_analytics")
    manifest = MockManifest(sources={"source.my_project.external.orders": source})
    
    runner = TestRunner(config, manifest, test)
    target = runner._resolve_test_target(test, manifest)
    
    assert target == "bigquery_analytics"


def test_test_does_not_use_federation():
    """Tests should never use federation path."""
    test = MockTestNode(depends_on=["model.my_project.cross_target_model"])
    # Even if the model uses federation, the test should not
    
    runner = TestRunner(config, manifest, test)
    # Should get adapter directly, not federation engine
    
    assert not hasattr(runner, 'federation_engine')
```

### Integration Tests

**File**: `tests/functional/test_dvt_test.py`

```python
def test_test_runs_on_model_target(project, postgres_target, snowflake_target):
    """Test that schema tests run on the model's target."""
    # Create model on snowflake
    # Add not_null test
    
    result = run_dbt(["test", "--select", "not_null_orders_id"])
    
    assert result.success
    assert "snowflake_dw" in result.output


def test_source_test_runs_on_source_connection(project):
    """Test that source tests run on source's connection."""
    # Create source with connection: bigquery_analytics
    # Add freshness test
    
    result = run_dbt(["source", "freshness"])
    
    assert result.success
    assert "bigquery_analytics" in result.output
```

---

## Key Rule: Tests Never Use Federation

**Important**: Tests must NEVER execute via Spark federation, even if the model they test was created via federation.

Rationale:
1. Tests validate the actual data in the target database
2. Federation execution would test the query, not the persisted data
3. Test results must be consistent with what downstream consumers see

```python
# WRONG - Never do this
def execute(self, test):
    if needs_federation(test):
        return federation_engine.execute(test)  # NO!

# CORRECT - Always use target adapter
def execute(self, test):
    target = self._resolve_test_target(test)
    adapter = self._get_adapter_for_target(target)
    return adapter.execute(test.compiled_code)
```

---

## Dependencies

- `04_parse.md` - sources must have connection property
- `06_run.md` - models must be materialized before testing

## Blocked By

- `04_parse.md`
- `06_run.md` (for model tests)

## Blocks

- `08_build.md` - build orchestrates test after run

---

## Implementation Checklist

- [ ] Modify `TestRunner` to resolve target from tested model/source
- [ ] Add `_resolve_test_target()` method
- [ ] Add `_resolve_model_target()` helper
- [ ] Add `SourceTestRunner` for source tests
- [ ] Add `TestExecutingOnTarget` event
- [ ] Add `TestTargetSummary` event
- [ ] Group tests by target for efficient execution
- [ ] Add unit tests for target resolution
- [ ] Add integration tests for multi-target testing
- [ ] Update documentation
