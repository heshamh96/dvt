# DVT CLI Command: `dvt snapshot`

## Phase: 4 (Federation Execution)
## Type: Modification

---

## Current Implementation Summary

**File**: `core/dvt/task/snapshot.py`

The current `dvt snapshot` command:
1. Reads snapshot configurations from `snapshots/` directory
2. Compares current data with previous snapshot
3. Tracks changes using SCD Type 2 pattern
4. Updates `dbt_valid_from` and `dbt_valid_to` columns
5. Maintains history of all record changes

---

## Required Changes

### Modification: Error on Federation Path

Snapshots in DVT **cannot** use the federation path because:
1. Snapshots require comparing current vs previous data in the same database
2. Cross-target snapshots would require storing history across databases
3. The SCD Type 2 pattern assumes single-database transactions

When a snapshot's source requires federation (cross-target), DVT must **fail with a clear error**.

### Reference Document
- `dvt_rules.md` - Section: "Snapshot Restrictions"

---

## Implementation Details

### 1. Federation Detection for Snapshots

**File**: `core/dvt/task/snapshot.py`

```python
from dvt.federation.resolver import FederationResolver, ExecutionPath


class SnapshotRunner(BaseRunner):
    """DVT Snapshot Runner - federation NOT supported."""
    
    def execute(self, snapshot: SnapshotNode, manifest: Manifest) -> RunResult:
        """Execute snapshot, failing if federation would be required."""
        
        # Check if federation would be required
        federation_check = self._check_federation_requirement(snapshot, manifest)
        
        if federation_check.requires_federation:
            raise SnapshotFederationError(
                snapshot=snapshot.unique_id,
                source_targets=federation_check.source_targets,
                snapshot_target=federation_check.snapshot_target,
            )
        
        # Standard snapshot execution
        return self._execute_snapshot(snapshot, manifest)
    
    def _check_federation_requirement(
        self,
        snapshot: SnapshotNode,
        manifest: Manifest,
    ) -> FederationCheck:
        """Check if snapshot would require federation.
        
        Federation is required if the snapshot's source is on a different
        target than where the snapshot table will be created.
        """
        # Resolve snapshot target
        snapshot_target = self._resolve_snapshot_target(snapshot)
        
        # Get source targets
        source_targets = set()
        
        for dep_id in snapshot.depends_on.nodes:
            if dep_id.startswith("source."):
                source = manifest.sources.get(dep_id)
                if source and source.connection:
                    source_targets.add(source.connection)
            elif dep_id.startswith("model."):
                model = manifest.nodes.get(dep_id)
                if model:
                    model_target = self._resolve_model_target(model)
                    source_targets.add(model_target)
        
        # Check if all sources are on same target as snapshot
        requires_federation = (
            len(source_targets) > 1 or
            (len(source_targets) == 1 and snapshot_target not in source_targets)
        )
        
        return FederationCheck(
            requires_federation=requires_federation,
            source_targets=source_targets,
            snapshot_target=snapshot_target,
        )
    
    def _resolve_snapshot_target(self, snapshot: SnapshotNode) -> str:
        """Resolve target for snapshot.
        
        Priority: CLI --target > snapshot config target > profiles.yml default
        """
        if self.config.args.target:
            return self.config.args.target
        
        if snapshot.config.get("target"):
            return snapshot.config["target"]
        
        return self.config.profile.target
    
    def _execute_snapshot(
        self,
        snapshot: SnapshotNode,
        manifest: Manifest,
    ) -> RunResult:
        """Execute snapshot using standard adapter path."""
        
        target = self._resolve_snapshot_target(snapshot)
        adapter = self._get_adapter_for_target(target)
        
        fire_event(SnapshotExecuting(
            snapshot=snapshot.unique_id,
            target=target,
        ))
        
        # Standard dbt snapshot execution
        return super().execute(snapshot, manifest)


@dataclass
class FederationCheck:
    """Result of federation requirement check."""
    requires_federation: bool
    source_targets: Set[str]
    snapshot_target: str
```

### 2. Snapshot Federation Error

**File**: `core/dvt/exceptions.py`

```python
class SnapshotFederationError(DvtRuntimeError):
    """Raised when a snapshot would require cross-target federation.
    
    Snapshots cannot use federation because:
    1. SCD Type 2 requires comparing data in the same database
    2. History tracking assumes single-database transactions
    3. Cross-target snapshots would have inconsistent state
    """
    
    def __init__(
        self,
        snapshot: str,
        source_targets: Set[str],
        snapshot_target: str,
    ):
        self.snapshot = snapshot
        self.source_targets = source_targets
        self.snapshot_target = snapshot_target
        super().__init__(self._build_message())
    
    def _build_message(self) -> str:
        sources = ", ".join(sorted(self.source_targets))
        return f"""
Cannot execute snapshot '{self.snapshot}' - federation not supported.

Snapshot target: {self.snapshot_target}
Source target(s): {sources}

Snapshots cannot use cross-target federation because:
1. SCD Type 2 pattern requires comparing current vs historical data
2. This comparison must happen within a single database transaction
3. Cross-target snapshots would have inconsistent history

To fix this, choose one of the following options:

Option 1: Move source to same target as snapshot (RECOMMENDED)
  Ensure the snapshot's source data is on the same target:
  
  snapshots:
    - name: {self.snapshot.split('.')[-1]}
      config:
        target: {list(self.source_targets)[0] if self.source_targets else 'same_as_source'}
      # Source must also be on this target

Option 2: Create intermediate model on snapshot target
  Create a model that materializes the source data on the snapshot target,
  then snapshot that model instead:
  
  -- models/staging/source_copy.sql
  {{{{ config(
      materialized='table',
      target='{self.snapshot_target}'
  ) }}}}
  
  SELECT * FROM {{{{ source('...', '...') }}}}
  
  Then update the snapshot to reference the model:
  
  -- snapshots/my_snapshot.sql
  {{{{ config(strategy='timestamp', ...) }}}}
  
  SELECT * FROM {{{{ ref('source_copy') }}}}

Option 3: Use manual ETL
  If you need cross-database snapshots, implement manual ETL:
  1. Extract data from source target
  2. Load to snapshot target
  3. Run snapshot on local data
"""
```

### 3. Snapshot Task Modifications

**File**: `core/dvt/task/snapshot.py`

```python
class SnapshotTask(RunTask):
    """DVT Snapshot Task - validates no federation required."""
    
    def before_run(self, adapter, selected_uids):
        """Validate all snapshots before running."""
        
        # Pre-validate all snapshots for federation
        errors = []
        
        for uid in selected_uids:
            snapshot = self.manifest.nodes.get(uid)
            if snapshot and snapshot.resource_type == "snapshot":
                try:
                    self._validate_no_federation(snapshot)
                except SnapshotFederationError as e:
                    errors.append(e)
        
        if errors:
            # Report all federation errors at once
            for error in errors:
                fire_event(SnapshotFederationBlocked(
                    snapshot=error.snapshot,
                    source_targets=list(error.source_targets),
                    snapshot_target=error.snapshot_target,
                ))
            
            raise SnapshotFederationError(
                snapshot=errors[0].snapshot,
                source_targets=errors[0].source_targets,
                snapshot_target=errors[0].snapshot_target,
            )
        
        return super().before_run(adapter, selected_uids)
    
    def _validate_no_federation(self, snapshot: SnapshotNode) -> None:
        """Validate snapshot doesn't require federation."""
        runner = SnapshotRunner(self.config, self.manifest, snapshot)
        check = runner._check_federation_requirement(snapshot, self.manifest)
        
        if check.requires_federation:
            raise SnapshotFederationError(
                snapshot=snapshot.unique_id,
                source_targets=check.source_targets,
                snapshot_target=check.snapshot_target,
            )
```

### 4. New Events

**File**: `core/dvt/events/types.py`

```python
@dataclass
class SnapshotExecuting(DebugLevel):
    snapshot: str
    target: str
    
    def message(self) -> str:
        return f"Executing snapshot {self.snapshot} on {self.target}"


@dataclass
class SnapshotFederationBlocked(ErrorLevel):
    snapshot: str
    source_targets: List[str]
    snapshot_target: str
    
    def message(self) -> str:
        sources = ", ".join(self.source_targets)
        return (
            f"Snapshot '{self.snapshot}' blocked: sources on [{sources}] "
            f"but snapshot on [{self.snapshot_target}]. "
            f"Cross-target snapshots not supported."
        )
```

---

## CLI Output Changes

### Success Case (same target)

```
$ dvt snapshot
Running with dvt=1.8.0
Found 2 snapshots

Executing snapshot orders_snapshot on postgres_prod
  OK snapshotted 1,234 rows (45 new, 12 changed)

Executing snapshot users_snapshot on postgres_prod
  OK snapshotted 5,678 rows (123 new, 0 changed)

Completed 2 snapshots
```

### Error Case (cross-target)

```
$ dvt snapshot
Running with dvt=1.8.0
Found 2 snapshots

ERROR: Snapshot 'orders_snapshot' blocked: sources on [bigquery_analytics] 
but snapshot on [postgres_prod]. Cross-target snapshots not supported.

Cannot execute snapshot 'snapshot.my_project.orders_snapshot' - federation not supported.

Snapshot target: postgres_prod
Source target(s): bigquery_analytics

Snapshots cannot use cross-target federation because:
1. SCD Type 2 pattern requires comparing current vs historical data
2. This comparison must happen within a single database transaction
3. Cross-target snapshots would have inconsistent history

To fix this, choose one of the following options:

Option 1: Move source to same target as snapshot (RECOMMENDED)
  ...

Option 2: Create intermediate model on snapshot target
  ...
```

---

## Valid Snapshot Configuration

```sql
-- snapshots/orders_snapshot.sql
{% snapshot orders_snapshot %}

{{ config(
    target_schema='snapshots',
    target='postgres_prod',  -- Snapshot will be created here
    unique_key='order_id',
    strategy='timestamp',
    updated_at='updated_at',
) }}

-- Source MUST be on same target (postgres_prod)
SELECT *
FROM {{ source('postgres_orders', 'orders') }}  -- connection: postgres_prod

{% endsnapshot %}
```

```yaml
# models/sources.yml
sources:
  - name: postgres_orders
    connection: postgres_prod  # Same as snapshot target
    tables:
      - name: orders
```

---

## Testing Requirements

### Unit Tests

**File**: `tests/unit/test_snapshot_task.py`

```python
def test_snapshot_same_target_succeeds():
    """Test snapshot with same source and snapshot target."""
    snapshot = MockSnapshotNode(
        config={"target": "postgres_prod"},
        depends_on=["source.test.orders"],
    )
    source = MockSource(connection="postgres_prod")
    manifest = MockManifest(sources={"source.test.orders": source})
    
    runner = SnapshotRunner(config, manifest, snapshot)
    check = runner._check_federation_requirement(snapshot, manifest)
    
    assert check.requires_federation is False


def test_snapshot_different_target_raises():
    """Test snapshot with different source and snapshot target raises."""
    snapshot = MockSnapshotNode(
        config={"target": "postgres_prod"},
        depends_on=["source.test.orders"],
    )
    source = MockSource(connection="bigquery_analytics")  # Different!
    manifest = MockManifest(sources={"source.test.orders": source})
    
    runner = SnapshotRunner(config, manifest, snapshot)
    
    with pytest.raises(SnapshotFederationError) as exc_info:
        runner.execute(snapshot, manifest)
    
    assert "federation not supported" in str(exc_info.value)
    assert "bigquery_analytics" in str(exc_info.value)


def test_snapshot_error_suggests_workarounds():
    """Test error message includes helpful suggestions."""
    error = SnapshotFederationError(
        snapshot="snapshot.test.orders_snapshot",
        source_targets={"bigquery_analytics"},
        snapshot_target="postgres_prod",
    )
    
    message = str(error)
    assert "Option 1" in message
    assert "Option 2" in message
    assert "intermediate model" in message
```

### Integration Tests

**File**: `tests/functional/test_snapshot.py`

```python
def test_snapshot_same_target(project):
    """Test snapshot works when source and target match."""
    # Setup source and snapshot on same target
    
    result = run_dbt(["snapshot"])
    
    assert result.success


def test_snapshot_cross_target_fails(project, multi_target_setup):
    """Test snapshot fails when federation would be required."""
    # Setup source on bigquery, snapshot on postgres
    
    result = run_dbt(["snapshot"], expect_pass=False)
    
    assert "federation not supported" in result.output
    assert "Option 1" in result.output
```

---

## Dependencies

- `04_parse.md` - sources must have connection property
- `06_run.md` - models referenced by snapshots must be built

## Blocked By

- `04_parse.md`
- `06_run.md`

## Blocks

- `08_build.md` - build orchestrates snapshots

---

## Implementation Checklist

- [ ] Create `SnapshotFederationError` exception with helpful message
- [ ] Add `_check_federation_requirement()` method
- [ ] Add `_validate_no_federation()` method
- [ ] Modify `SnapshotRunner.execute()` to check federation
- [ ] Add pre-validation in `SnapshotTask.before_run()`
- [ ] Add `SnapshotExecuting` event
- [ ] Add `SnapshotFederationBlocked` event
- [ ] Add unit tests for federation detection
- [ ] Add unit tests for error messages
- [ ] Add integration tests
- [ ] Update documentation with snapshot restrictions
