# DVT CLI Command: `dvt run`

## Phase: 3 + 4 (Core Rules + Federation Execution)
## Type: Full Implementation

---

## Current Implementation Summary

**File**: `core/dvt/task/run.py`

The current `dvt run` command:
1. Parses and compiles models
2. Builds execution graph from dependencies
3. Executes models in topological order
4. Uses adapter to run SQL on target database
5. Handles materializations (table, view, incremental)

**Current limitation**: All models execute on a single target database.

---

## Required Changes

### Full: Federation-Aware Execution

Transform `dvt run` to:
1. Resolve target for each model (CLI > config > default)
2. Detect when federation is needed (cross-target dependencies)
3. Select execution path (adapter pushdown vs. Spark federation)
4. Execute via appropriate path
5. Handle materialization coercion for federation

### Reference Documents
- `dvt_rules.md` - Target/compute resolution, materialization rules
- `federation_execution_plan.md` - Full federation engine details

---

## Implementation Details

### 1. Federation Resolver

**File**: `core/dvt/federation/resolver.py` (new file)

```python
"""Federation resolver for DVT.

Determines execution path for each model based on its dependencies
and target configuration.
"""

from dataclasses import dataclass
from enum import Enum
from typing import Dict, List, Set, Optional
from dvt.contracts.graph.nodes import ModelNode, SourceDefinition


class ExecutionPath(Enum):
    """Execution path for a model."""
    ADAPTER_PUSHDOWN = "adapter_pushdown"  # Execute on target adapter
    SPARK_FEDERATION = "spark_federation"  # Execute via Spark JDBC


@dataclass
class ResolvedExecution:
    """Resolved execution details for a model."""
    model_id: str
    target: str
    compute: Optional[str]
    execution_path: ExecutionPath
    upstream_targets: Set[str]
    requires_materialization_coercion: bool
    original_materialization: str
    coerced_materialization: Optional[str]


class FederationResolver:
    """Resolves execution paths for models.
    
    Determines whether each model should:
    1. Execute via adapter pushdown (same-target dependencies)
    2. Execute via Spark federation (cross-target dependencies)
    """
    
    def __init__(self, manifest, runtime_config):
        self.manifest = manifest
        self.config = runtime_config
    
    def resolve_all(self, selected_nodes: List[str]) -> Dict[str, ResolvedExecution]:
        """Resolve execution for all selected nodes.
        
        Returns:
            Dict mapping node_id -> ResolvedExecution
        """
        resolutions = {}
        
        for node_id in selected_nodes:
            node = self.manifest.nodes.get(node_id)
            if node and node.resource_type == "model":
                resolutions[node_id] = self.resolve_model(node)
        
        return resolutions
    
    def resolve_model(self, model: ModelNode) -> ResolvedExecution:
        """Resolve execution for a single model."""
        
        # Step 1: Resolve target
        target = self._resolve_target(model)
        
        # Step 2: Get upstream targets
        upstream_targets = self._get_upstream_targets(model)
        
        # Step 3: Determine execution path
        execution_path = self._determine_execution_path(target, upstream_targets)
        
        # Step 4: Resolve compute (only for federation)
        compute = None
        if execution_path == ExecutionPath.SPARK_FEDERATION:
            compute = self._resolve_compute(model)
        
        # Step 5: Check materialization coercion
        original_mat = model.config.materialized
        coerced_mat = None
        requires_coercion = False
        
        if execution_path == ExecutionPath.SPARK_FEDERATION:
            coerced_mat = self._coerce_materialization(original_mat)
            requires_coercion = coerced_mat != original_mat
        
        return ResolvedExecution(
            model_id=model.unique_id,
            target=target,
            compute=compute,
            execution_path=execution_path,
            upstream_targets=upstream_targets,
            requires_materialization_coercion=requires_coercion,
            original_materialization=original_mat,
            coerced_materialization=coerced_mat,
        )
    
    def _resolve_target(self, model: ModelNode) -> str:
        """Resolve target for a model.
        
        Priority: CLI --target > model config target > profiles.yml default
        """
        # CLI flag (highest priority)
        if self.config.args.target:
            return self.config.args.target
        
        # Model config
        if model.config.get("target"):
            return model.config["target"]
        
        # Profile default
        return self.config.profile.target
    
    def _resolve_compute(self, model: ModelNode) -> str:
        """Resolve compute for a model (federation only).
        
        Priority: CLI --compute > model config compute > computes.yml default
        """
        # CLI flag
        if self.config.args.compute:
            return self.config.args.compute
        
        # Model config
        if model.config.get("compute"):
            return model.config["compute"]
        
        # computes.yml default
        return self._get_default_compute()
    
    def _get_default_compute(self) -> str:
        """Get default compute from computes.yml."""
        computes_config = load_computes_config()
        profile_name = self.config.profile.profile_name
        
        profile_computes = computes_config.get(profile_name, {})
        return profile_computes.get("default", "local")
    
    def _get_upstream_targets(self, model: ModelNode) -> Set[str]:
        """Get all unique targets from upstream dependencies."""
        targets = set()
        
        for dep_id in model.depends_on.nodes:
            dep_node = self.manifest.nodes.get(dep_id)
            
            if dep_node:
                if dep_node.resource_type == "source":
                    # Sources have explicit connection property
                    source = self.manifest.sources.get(dep_id)
                    if source and source.connection:
                        targets.add(source.connection)
                elif dep_node.resource_type == "model":
                    # Recursively resolve model's target
                    targets.add(self._resolve_target(dep_node))
        
        return targets
    
    def _determine_execution_path(
        self,
        model_target: str,
        upstream_targets: Set[str],
    ) -> ExecutionPath:
        """Determine whether to use pushdown or federation.
        
        Rules:
        - If all upstream targets == model target: ADAPTER_PUSHDOWN
        - If any upstream target != model target: SPARK_FEDERATION
        """
        # No upstream dependencies - use pushdown
        if not upstream_targets:
            return ExecutionPath.ADAPTER_PUSHDOWN
        
        # All same target - use pushdown
        if upstream_targets == {model_target}:
            return ExecutionPath.ADAPTER_PUSHDOWN
        
        # Mixed targets - use federation
        return ExecutionPath.SPARK_FEDERATION
    
    def _coerce_materialization(self, materialization: str) -> str:
        """Coerce materialization for federation path.
        
        Rules from dvt_rules.md:
        - view -> table (cannot create cross-DB view)
        - ephemeral -> ERROR (must materialize)
        - table, incremental -> unchanged
        """
        if materialization == "view":
            return "table"
        elif materialization == "ephemeral":
            raise FederationError(
                f"Cannot execute ephemeral model via federation. "
                f"Ephemeral models must be materialized (table/view) or "
                f"have all dependencies on the same target."
            )
        return materialization
```

### 2. Federation Engine

**File**: `core/dvt/federation/engine.py` (new file)

```python
"""Federation execution engine for DVT.

Executes models that require cross-target data access using Spark JDBC.
"""

from pyspark.sql import SparkSession, DataFrame
from dvt.federation.spark_manager import SparkManager
from dvt.federation.sqlglot_translator import SQLGlotTranslator


class FederationEngine:
    """Executes federated queries via Spark."""
    
    def __init__(self, config, manifest):
        self.config = config
        self.manifest = manifest
        self.spark_manager = SparkManager(config)
        self.translator = SQLGlotTranslator()
    
    def execute(self, model: ModelNode, resolution: ResolvedExecution) -> RunResult:
        """Execute a model via Spark federation.
        
        Steps:
        1. Get Spark session with compute configuration
        2. Register upstream tables as JDBC data sources
        3. Translate SQL to Spark SQL using SQLGlot
        4. Execute query
        5. Write results to target database
        """
        spark = self.spark_manager.get_or_create_session(resolution.compute)
        
        try:
            # Register upstream tables
            self._register_upstream_tables(spark, model, resolution)
            
            # Translate and execute SQL
            spark_sql = self._translate_sql(model, resolution)
            result_df = spark.sql(spark_sql)
            
            # Write to target
            return self._write_to_target(result_df, model, resolution)
            
        except Exception as e:
            return RunResult(
                status=RunStatus.Error,
                message=f"Federation execution failed: {e}",
            )
    
    def _register_upstream_tables(
        self,
        spark: SparkSession,
        model: ModelNode,
        resolution: ResolvedExecution,
    ) -> None:
        """Register all upstream tables as Spark temp views via JDBC."""
        
        for dep_id in model.depends_on.nodes:
            dep_node = self.manifest.nodes.get(dep_id)
            
            if dep_node is None:
                continue
            
            # Get connection details for this dependency
            if dep_node.resource_type == "source":
                source = self.manifest.sources.get(dep_id)
                target = source.connection
                table_ref = f"{source.schema}.{source.name}"
            else:
                target = self._resolve_target(dep_node)
                table_ref = f"{dep_node.schema}.{dep_node.name}"
            
            # Create temp view via JDBC
            connection = self._get_connection(target)
            jdbc_url = self._build_jdbc_url(connection)
            
            df = spark.read.jdbc(
                url=jdbc_url,
                table=table_ref,
                properties={
                    "user": connection.get("user", ""),
                    "password": connection.get("password", ""),
                    "driver": self._get_jdbc_driver(connection),
                },
            )
            
            # Register as temp view with original reference name
            view_name = self._get_view_name(dep_node)
            df.createOrReplaceTempView(view_name)
    
    def _translate_sql(self, model: ModelNode, resolution: ResolvedExecution) -> str:
        """Translate model SQL for Spark execution.
        
        Uses SQLGlot to:
        1. Parse the model's SQL
        2. Rewrite table references to temp view names
        3. Translate dialect-specific syntax to Spark SQL
        """
        source_dialect = self._get_dialect(resolution.target)
        
        return self.translator.translate(
            sql=model.compiled_code,
            source_dialect=source_dialect,
            target_dialect="spark",
            table_mappings=self._build_table_mappings(model),
        )
    
    def _write_to_target(
        self,
        df: DataFrame,
        model: ModelNode,
        resolution: ResolvedExecution,
    ) -> RunResult:
        """Write query results to target database."""
        
        connection = self._get_connection(resolution.target)
        jdbc_url = self._build_jdbc_url(connection)
        
        table_name = f"{model.schema}.{model.name}"
        materialization = resolution.coerced_materialization or model.config.materialized
        
        # Determine write mode based on materialization
        if materialization == "incremental" and not self.config.args.full_refresh:
            mode = "append"
        else:
            mode = "overwrite"
        
        try:
            df.write.jdbc(
                url=jdbc_url,
                table=table_name,
                mode=mode,
                properties={
                    "user": connection.get("user", ""),
                    "password": connection.get("password", ""),
                    "driver": self._get_jdbc_driver(connection),
                },
            )
            
            row_count = df.count()
            
            return RunResult(
                status=RunStatus.Success,
                message=f"Federation: wrote {row_count} rows to {table_name}",
                adapter_response={"rows_affected": row_count},
            )
            
        except Exception as e:
            return RunResult(
                status=RunStatus.Error,
                message=f"Failed to write to {table_name}: {e}",
            )
```

### 3. Modified Model Runner

**File**: `core/dvt/task/run.py`

```python
from dvt.federation.resolver import FederationResolver, ExecutionPath
from dvt.federation.engine import FederationEngine


class ModelRunner(CompileRunner):
    """DVT Model Runner with federation support."""
    
    def __init__(self, config, adapter, node, node_index, num_nodes):
        super().__init__(config, adapter, node, node_index, num_nodes)
        self.federation_engine = None
    
    def execute(self, model: ModelNode, manifest: Manifest) -> RunResult:
        """Execute model via appropriate path."""
        
        # Get pre-resolved execution details
        resolution = self.config.resolved_executions.get(model.unique_id)
        
        if resolution is None:
            # Fallback: resolve now
            resolver = FederationResolver(manifest, self.config)
            resolution = resolver.resolve_model(model)
        
        # Log execution path
        fire_event(ModelExecutionPath(
            model=model.unique_id,
            path=resolution.execution_path.value,
            target=resolution.target,
            upstream_targets=list(resolution.upstream_targets),
        ))
        
        # Handle materialization coercion warning
        if resolution.requires_materialization_coercion:
            fire_event(MaterializationCoerced(
                model=model.unique_id,
                original=resolution.original_materialization,
                coerced=resolution.coerced_materialization,
                reason="Cross-target federation requires table materialization",
            ))
        
        # Execute via appropriate path
        if resolution.execution_path == ExecutionPath.ADAPTER_PUSHDOWN:
            return self._execute_pushdown(model, manifest, resolution)
        else:
            return self._execute_federation(model, manifest, resolution)
    
    def _execute_pushdown(
        self,
        model: ModelNode,
        manifest: Manifest,
        resolution: ResolvedExecution,
    ) -> RunResult:
        """Execute model via adapter pushdown (standard dbt path)."""
        
        # Get adapter for target
        adapter = self._get_adapter_for_target(resolution.target)
        
        # Standard dbt execution
        return super().execute(model, manifest)
    
    def _execute_federation(
        self,
        model: ModelNode,
        manifest: Manifest,
        resolution: ResolvedExecution,
    ) -> RunResult:
        """Execute model via Spark federation."""
        
        if self.federation_engine is None:
            self.federation_engine = FederationEngine(self.config, manifest)
        
        return self.federation_engine.execute(model, resolution)
```

### 4. Run Task Modifications

**File**: `core/dvt/task/run.py`

```python
class RunTask(CompileTask):
    """DVT Run Task with federation support."""
    
    def before_run(self, adapter, selected_uids):
        """Pre-resolve all executions before running."""
        
        # Resolve execution paths for all selected models
        resolver = FederationResolver(self.manifest, self.config)
        self.config.resolved_executions = resolver.resolve_all(selected_uids)
        
        # Log federation summary
        federation_count = sum(
            1 for r in self.config.resolved_executions.values()
            if r.execution_path == ExecutionPath.SPARK_FEDERATION
        )
        
        if federation_count > 0:
            fire_event(FederationSummary(
                total_models=len(selected_uids),
                federation_models=federation_count,
                pushdown_models=len(selected_uids) - federation_count,
            ))
            
            # Initialize Spark for federation
            self.spark_manager = SparkManager(self.config)
            self.spark_manager.get_or_create_session()
        
        return super().before_run(adapter, selected_uids)
    
    def after_run(self, adapter, results):
        """Clean up after run."""
        
        # Stop Spark if it was started
        if hasattr(self, 'spark_manager'):
            self.spark_manager.stop_session()
        
        return super().after_run(adapter, results)
```

### 5. New Events

**File**: `core/dvt/events/types.py`

```python
@dataclass
class ModelExecutionPath(InfoLevel):
    model: str
    path: str
    target: str
    upstream_targets: List[str]
    
    def message(self) -> str:
        if self.path == "adapter_pushdown":
            return f"Model {self.model}: executing on {self.target} (pushdown)"
        else:
            upstreams = ", ".join(self.upstream_targets)
            return f"Model {self.model}: federation from [{upstreams}] -> {self.target}"


@dataclass
class MaterializationCoerced(WarnLevel):
    model: str
    original: str
    coerced: str
    reason: str
    
    def message(self) -> str:
        return (
            f"Model {self.model}: materialization changed from '{self.original}' "
            f"to '{self.coerced}' ({self.reason})"
        )


@dataclass
class FederationSummary(InfoLevel):
    total_models: int
    federation_models: int
    pushdown_models: int
    
    def message(self) -> str:
        return (
            f"Execution plan: {self.pushdown_models} models via pushdown, "
            f"{self.federation_models} models via federation"
        )
```

---

## CLI Output Changes

Before:
```
$ dvt run
Running with dbt=1.8.0
Found 10 models

Running model my_model_1...
  OK created table my_model_1 in 2.3s
```

After:
```
$ dvt run
Running with dvt=1.8.0
Found 10 models

Execution plan: 7 models via pushdown, 3 models via federation
Initializing Spark session for federation...

Running model staging_users (pushdown -> postgres_prod)
  OK created table staging_users in 1.2s

Running model staging_events (pushdown -> bigquery_analytics)
  OK created table staging_events in 0.8s

Running model combined_user_events (federation: [postgres_prod, bigquery_analytics] -> snowflake_dw)
  WARNING: materialization changed from 'view' to 'table' (cross-target federation)
  OK created table combined_user_events in 5.4s (12,340 rows)

Completed 10 models in 32.1s
```

---

## Example Model Configuration

```yaml
# models/marts/combined_data.yml
version: 2

models:
  - name: combined_user_events
    config:
      target: snowflake_dw       # Write to Snowflake
      compute: databricks_prod   # Use Databricks for federation
      materialized: table
    description: Combines user data from Postgres with events from BigQuery
```

```sql
-- models/marts/combined_user_events.sql
{{ config(
    target='snowflake_dw',
    compute='databricks_prod',
    materialized='table'
) }}

SELECT
    u.user_id,
    u.email,
    e.event_type,
    e.event_timestamp
FROM {{ source('postgres_users', 'users') }} u  -- From postgres_prod
JOIN {{ source('bigquery_events', 'events') }} e  -- From bigquery_analytics
    ON u.user_id = e.user_id
WHERE e.event_timestamp > CURRENT_DATE - INTERVAL '7 days'
```

---

## Testing Requirements

### Unit Tests

**File**: `tests/unit/test_federation_resolver.py`

```python
def test_resolve_target_from_cli():
    """CLI --target takes priority."""
    config = MockConfig(args={"target": "cli_target"})
    model = MockModel(config={"target": "model_target"})
    
    resolver = FederationResolver(manifest, config)
    resolution = resolver.resolve_model(model)
    
    assert resolution.target == "cli_target"


def test_execution_path_pushdown_same_target():
    """Same-target dependencies use pushdown."""
    model = MockModel(depends_on=["source_a", "model_b"])
    # Both depend on postgres_prod
    
    resolver = FederationResolver(manifest, config)
    resolution = resolver.resolve_model(model)
    
    assert resolution.execution_path == ExecutionPath.ADAPTER_PUSHDOWN


def test_execution_path_federation_cross_target():
    """Cross-target dependencies use federation."""
    model = MockModel(depends_on=["source_a", "source_b"])
    # source_a on postgres, source_b on bigquery
    
    resolver = FederationResolver(manifest, config)
    resolution = resolver.resolve_model(model)
    
    assert resolution.execution_path == ExecutionPath.SPARK_FEDERATION


def test_materialization_coercion_view_to_table():
    """View is coerced to table for federation."""
    model = MockModel(config={"materialized": "view"})
    
    resolver = FederationResolver(manifest, config)
    coerced = resolver._coerce_materialization("view")
    
    assert coerced == "table"


def test_materialization_coercion_ephemeral_raises():
    """Ephemeral raises error for federation."""
    resolver = FederationResolver(manifest, config)
    
    with pytest.raises(FederationError):
        resolver._coerce_materialization("ephemeral")
```

### Integration Tests

**File**: `tests/functional/test_federation_run.py`

```python
def test_run_with_cross_target_model(project, postgres_target, bigquery_target):
    """Test running a model with cross-target dependencies."""
    # Setup sources on different targets
    # Setup model that joins them
    
    result = run_dbt(["run", "--select", "combined_model"])
    
    assert result.success
    assert "federation" in result.output


def test_run_materialization_coercion_warning(project):
    """Test that view materialization triggers warning."""
    # Setup cross-target model with view materialization
    
    result = run_dbt(["run", "--select", "cross_target_view"])
    
    assert result.success
    assert "materialization changed from 'view' to 'table'" in result.output
```

---

## Dependencies

- `04_parse.md` - sources must have connection property
- Phase 1 complete (init, sync, debug, parse)
- `05_seed.md` - optional (seeds may be upstream dependencies)

## Blocked By

- Phase 1 commands
- `04_parse.md` (source connection validation)

## Blocks

- `08_build.md` - build orchestrates run
- `07_test.md` - tests run after models

---

## Implementation Checklist

- [ ] Create `core/dvt/federation/resolver.py`
- [ ] Create `core/dvt/federation/engine.py`
- [ ] Create `core/dvt/federation/spark_manager.py`
- [ ] Create `core/dvt/federation/sqlglot_translator.py`
- [ ] Modify `ModelRunner` to support federation
- [ ] Modify `RunTask` to pre-resolve executions
- [ ] Add new event types
- [ ] Add `--compute` CLI flag
- [ ] Add unit tests for resolver
- [ ] Add unit tests for engine
- [ ] Add integration tests for federation
- [ ] Update documentation
