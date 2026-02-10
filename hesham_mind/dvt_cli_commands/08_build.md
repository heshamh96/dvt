# DVT CLI Command: `dvt build`

## Phase: 4 (Federation Execution)
## Type: Full Implementation

---

## Current Implementation Summary

**File**: `core/dvt/task/build.py`

The current `dvt build` command orchestrates:
1. `seed` - Load seed data
2. `run` - Execute models
3. `test` - Run tests
4. `snapshot` - Capture snapshots

It runs these in dependency order, respecting the DAG.

---

## Required Changes

### Full: Coordinate All Commands with Federation Awareness

The build command must coordinate seeds, runs, tests, and snapshots with proper federation awareness:
1. Pre-resolve all execution paths
2. Initialize Spark once for all federation operations
3. Execute in correct order respecting cross-target dependencies
4. Handle failures gracefully

### Reference Documents
- `dvt_rules.md` - Execution ordering rules
- `federation_execution_plan.md` - Federation coordination
- `spark_seed_plan.md` - Seed integration

---

## Implementation Details

### 1. Build Task with Federation Coordination

**File**: `core/dvt/task/build.py`

```python
from dvt.federation.resolver import FederationResolver, ExecutionPath
from dvt.federation.spark_manager import SparkManager
from dvt.task.seed import SeedTask
from dvt.task.run import RunTask
from dvt.task.test import TestTask
from dvt.task.snapshot import SnapshotTask


class BuildTask(GraphRunnableTask):
    """DVT Build Task - orchestrates seed, run, test, snapshot with federation.
    
    Coordinates all operations with a single Spark session for efficiency.
    """
    
    def __init__(self, args, config, manifest):
        super().__init__(args, config, manifest)
        self.spark_manager = None
        self.resolved_executions = {}
    
    def run(self) -> BuildResult:
        """Execute build with federation coordination."""
        
        # Step 1: Pre-resolve all executions
        self._pre_resolve_executions()
        
        # Step 2: Initialize Spark if needed
        if self._needs_federation():
            self._initialize_spark()
        
        try:
            # Step 3: Execute phases in order
            results = BuildResult()
            
            # Seeds first (may be dependencies for models)
            if self._has_seeds():
                seed_results = self._run_seeds()
                results.seed_results = seed_results
                if not seed_results.success:
                    return results
            
            # Run models
            if self._has_models():
                run_results = self._run_models()
                results.run_results = run_results
                if not run_results.success and self.config.args.fail_fast:
                    return results
            
            # Snapshots (before tests, as tests may reference snapshots)
            if self._has_snapshots():
                snapshot_results = self._run_snapshots()
                results.snapshot_results = snapshot_results
            
            # Tests last (validate everything)
            if self._has_tests():
                test_results = self._run_tests()
                results.test_results = test_results
            
            return results
            
        finally:
            # Step 4: Cleanup
            self._cleanup_spark()
    
    def _pre_resolve_executions(self) -> None:
        """Pre-resolve execution paths for all nodes."""
        
        resolver = FederationResolver(self.manifest, self.config)
        
        # Resolve models
        model_ids = [n for n in self.selected_nodes if n.startswith("model.")]
        self.resolved_executions = resolver.resolve_all(model_ids)
        
        # Log summary
        federation_count = sum(
            1 for r in self.resolved_executions.values()
            if r.execution_path == ExecutionPath.SPARK_FEDERATION
        )
        
        fire_event(BuildExecutionPlan(
            seeds=len([n for n in self.selected_nodes if n.startswith("seed.")]),
            models=len(model_ids),
            models_federation=federation_count,
            models_pushdown=len(model_ids) - federation_count,
            tests=len([n for n in self.selected_nodes if n.startswith("test.")]),
            snapshots=len([n for n in self.selected_nodes if n.startswith("snapshot.")]),
        ))
    
    def _needs_federation(self) -> bool:
        """Check if any operation requires federation."""
        return any(
            r.execution_path == ExecutionPath.SPARK_FEDERATION
            for r in self.resolved_executions.values()
        )
    
    def _initialize_spark(self) -> None:
        """Initialize shared Spark session for federation."""
        fire_event(BuildInitializingSpark())
        
        self.spark_manager = SparkManager(self.config)
        self.spark_manager.get_or_create_session()
        
        # Share with sub-tasks
        self.config.spark_manager = self.spark_manager
    
    def _cleanup_spark(self) -> None:
        """Clean up Spark session."""
        if self.spark_manager:
            self.spark_manager.stop_session()
    
    def _run_seeds(self) -> SeedResult:
        """Run seeds using shared Spark session."""
        fire_event(BuildPhaseStarting(phase="seed"))
        
        seed_task = SeedTask(
            args=self.args,
            config=self.config,
            manifest=self.manifest,
        )
        
        # Share Spark manager
        if self.spark_manager:
            seed_task.spark_manager = self.spark_manager
        
        return seed_task.run()
    
    def _run_models(self) -> RunResult:
        """Run models with pre-resolved executions."""
        fire_event(BuildPhaseStarting(phase="run"))
        
        run_task = RunTask(
            args=self.args,
            config=self.config,
            manifest=self.manifest,
        )
        
        # Pass pre-resolved executions
        run_task.config.resolved_executions = self.resolved_executions
        
        # Share Spark manager
        if self.spark_manager:
            run_task.spark_manager = self.spark_manager
        
        return run_task.run()
    
    def _run_tests(self) -> TestResult:
        """Run tests (always on target adapters, not federation)."""
        fire_event(BuildPhaseStarting(phase="test"))
        
        test_task = TestTask(
            args=self.args,
            config=self.config,
            manifest=self.manifest,
        )
        
        # Tests don't use Spark - they run on target adapters
        return test_task.run()
    
    def _run_snapshots(self) -> SnapshotResult:
        """Run snapshots (will error if federation path detected)."""
        fire_event(BuildPhaseStarting(phase="snapshot"))
        
        snapshot_task = SnapshotTask(
            args=self.args,
            config=self.config,
            manifest=self.manifest,
        )
        
        return snapshot_task.run()
    
    def _has_seeds(self) -> bool:
        return any(n.startswith("seed.") for n in self.selected_nodes)
    
    def _has_models(self) -> bool:
        return any(n.startswith("model.") for n in self.selected_nodes)
    
    def _has_tests(self) -> bool:
        return any(n.startswith("test.") for n in self.selected_nodes)
    
    def _has_snapshots(self) -> bool:
        return any(n.startswith("snapshot.") for n in self.selected_nodes)
```

### 2. Build Result Aggregation

**File**: `core/dvt/contracts/results.py`

```python
@dataclass
class BuildResult:
    """Aggregated results from build command."""
    
    seed_results: Optional[SeedResult] = None
    run_results: Optional[RunResult] = None
    test_results: Optional[TestResult] = None
    snapshot_results: Optional[SnapshotResult] = None
    
    @property
    def success(self) -> bool:
        """Build succeeds if all phases succeed."""
        results = [
            self.seed_results,
            self.run_results,
            self.test_results,
            self.snapshot_results,
        ]
        return all(r is None or r.success for r in results)
    
    @property
    def summary(self) -> Dict[str, Any]:
        """Summary statistics for build."""
        return {
            "seeds": self._phase_summary(self.seed_results),
            "models": self._phase_summary(self.run_results),
            "tests": self._phase_summary(self.test_results),
            "snapshots": self._phase_summary(self.snapshot_results),
        }
    
    def _phase_summary(self, result) -> Dict[str, int]:
        if result is None:
            return {"total": 0, "success": 0, "error": 0}
        return {
            "total": len(result.results),
            "success": sum(1 for r in result.results if r.status == "success"),
            "error": sum(1 for r in result.results if r.status == "error"),
        }
```

### 3. New Events

**File**: `core/dvt/events/types.py`

```python
@dataclass
class BuildExecutionPlan(InfoLevel):
    seeds: int
    models: int
    models_federation: int
    models_pushdown: int
    tests: int
    snapshots: int
    
    def message(self) -> str:
        return (
            f"Build plan: {self.seeds} seeds, "
            f"{self.models} models ({self.models_pushdown} pushdown, "
            f"{self.models_federation} federation), "
            f"{self.tests} tests, {self.snapshots} snapshots"
        )


@dataclass
class BuildInitializingSpark(InfoLevel):
    def message(self) -> str:
        return "Initializing Spark session for federation operations..."


@dataclass
class BuildPhaseStarting(InfoLevel):
    phase: str
    
    def message(self) -> str:
        return f"Starting {self.phase} phase..."


@dataclass
class BuildPhaseComplete(InfoLevel):
    phase: str
    success: int
    error: int
    elapsed: float
    
    def message(self) -> str:
        return f"Completed {self.phase}: {self.success} succeeded, {self.error} failed ({self.elapsed:.1f}s)"


@dataclass
class BuildComplete(InfoLevel):
    total_time: float
    summary: Dict[str, Any]
    
    def message(self) -> str:
        parts = []
        for phase, stats in self.summary.items():
            if stats["total"] > 0:
                parts.append(f"{phase}: {stats['success']}/{stats['total']}")
        return f"Build complete in {self.total_time:.1f}s - {', '.join(parts)}"
```

### 4. CLI Integration

**File**: `core/dvt/cli/main.py`

```python
@click.command("build")
@click.option("--target", "-t", help="Target for models (overrides default)")
@click.option("--compute", "-c", help="Compute for federation (overrides default)")
@click.option("--select", "-s", multiple=True, help="Select specific nodes")
@click.option("--exclude", multiple=True, help="Exclude specific nodes")
@click.option("--fail-fast", is_flag=True, help="Stop on first failure")
@click.option("--full-refresh", is_flag=True, help="Full refresh for seeds and incremental models")
@click.pass_context
def build(ctx, target, compute, select, exclude, fail_fast, full_refresh):
    """Build project: seed, run, test, snapshot.
    
    Coordinates all operations with federation support.
    Uses a single Spark session for efficiency when federation is needed.
    
    Examples:
        dvt build                          # Build everything
        dvt build --select tag:core        # Build core models
        dvt build --target prod            # Build to production
        dvt build --fail-fast              # Stop on first failure
    """
    # ... implementation
```

---

## CLI Output Changes

Before:
```
$ dvt build
Running with dbt=1.8.0

Running seeds...
  Completed 3 seeds

Running models...
  Completed 10 models

Running tests...
  Completed 15 tests

Done.
```

After:
```
$ dvt build
Running with dvt=1.8.0

Build plan: 3 seeds, 10 models (7 pushdown, 3 federation), 15 tests, 2 snapshots
Initializing Spark session for federation operations...

Starting seed phase...
  my_seed_1: OK (1,000 rows -> postgres_prod)
  my_seed_2: OK (5,000 rows -> snowflake_dw via native connector)
  my_seed_3: OK (500 rows -> bigquery_analytics)
Completed seed: 3 succeeded, 0 failed (8.2s)

Starting run phase...
  staging_users: OK (pushdown -> postgres_prod) 1.2s
  staging_orders: OK (pushdown -> snowflake_dw) 0.8s
  combined_data: OK (federation: [postgres_prod, snowflake_dw] -> snowflake_dw) 5.4s
  ...
Completed run: 10 succeeded, 0 failed (25.3s)

Starting snapshot phase...
  orders_snapshot: OK
Completed snapshot: 2 succeeded, 0 failed (3.1s)

Starting test phase...
  not_null_users_id: PASS (postgres_prod)
  unique_orders_id: PASS (snowflake_dw)
  ...
Completed test: 14 passed, 1 failed (12.4s)

Build complete in 49.0s - seeds: 3/3, models: 10/10, snapshots: 2/2, tests: 14/15
```

---

## Execution Order

Build respects the DAG for execution order:

```
1. Seeds (no dependencies)
   ├── my_seed_1 -> postgres_prod
   ├── my_seed_2 -> snowflake_dw
   └── my_seed_3 -> bigquery_analytics

2. Models (in dependency order)
   ├── staging_users (depends on seed_1)
   ├── staging_orders (depends on seed_2)
   ├── staging_events (depends on source)
   └── combined_data (depends on staging_users, staging_orders)

3. Snapshots (after models they reference)
   └── orders_snapshot (depends on staging_orders)

4. Tests (after models/sources they test)
   ├── not_null_users_id (tests staging_users)
   ├── unique_orders_id (tests staging_orders)
   └── ...
```

---

## Testing Requirements

### Unit Tests

**File**: `tests/unit/test_build_task.py`

```python
def test_build_pre_resolves_executions():
    """Test that build pre-resolves all execution paths."""
    task = BuildTask(args, config, manifest)
    task._pre_resolve_executions()
    
    assert len(task.resolved_executions) == 10  # 10 models
    
    # Check federation models are identified
    federation = [r for r in task.resolved_executions.values() 
                  if r.execution_path == ExecutionPath.SPARK_FEDERATION]
    assert len(federation) == 3


def test_build_initializes_spark_when_needed():
    """Test Spark is initialized when federation is needed."""
    task = BuildTask(args, config, manifest)
    task.resolved_executions = {
        "model.test.m1": ResolvedExecution(execution_path=ExecutionPath.SPARK_FEDERATION)
    }
    
    assert task._needs_federation() is True


def test_build_skips_spark_when_not_needed():
    """Test Spark is not initialized when all pushdown."""
    task = BuildTask(args, config, manifest)
    task.resolved_executions = {
        "model.test.m1": ResolvedExecution(execution_path=ExecutionPath.ADAPTER_PUSHDOWN)
    }
    
    assert task._needs_federation() is False


def test_build_shares_spark_session():
    """Test Spark session is shared across phases."""
    task = BuildTask(args, config, manifest)
    task._initialize_spark()
    
    seed_task = task._create_seed_task()
    assert seed_task.spark_manager is task.spark_manager
```

### Integration Tests

**File**: `tests/functional/test_build.py`

```python
def test_build_full_project(project):
    """Test full build with seeds, models, tests."""
    result = run_dbt(["build"])
    
    assert result.success
    assert "Build complete" in result.output


def test_build_with_federation(project, multi_target_setup):
    """Test build with cross-target models."""
    result = run_dbt(["build"])
    
    assert result.success
    assert "federation" in result.output
    assert "Initializing Spark" in result.output


def test_build_fail_fast(project):
    """Test --fail-fast stops on first failure."""
    # Setup a model that will fail
    
    result = run_dbt(["build", "--fail-fast"], expect_pass=False)
    
    # Should stop before tests
    assert "Starting test phase" not in result.output
```

---

## Dependencies

- `05_seed.md` - seed implementation
- `06_run.md` - run implementation with federation
- `07_test.md` - test implementation
- `10_snapshot.md` - snapshot implementation

## Blocked By

- `05_seed.md`
- `06_run.md`
- `07_test.md`
- `10_snapshot.md`

## Blocks

- None (build is a terminal command)

---

## Implementation Checklist

- [ ] Create `BuildTask` with federation coordination
- [ ] Implement `_pre_resolve_executions()` method
- [ ] Implement `_initialize_spark()` for shared session
- [ ] Implement phase execution methods (seeds, models, tests, snapshots)
- [ ] Create `BuildResult` aggregation class
- [ ] Add build events (plan, phase start/complete, build complete)
- [ ] Update CLI with `--compute` flag
- [ ] Add unit tests for coordination logic
- [ ] Add integration tests for full build
- [ ] Update documentation
