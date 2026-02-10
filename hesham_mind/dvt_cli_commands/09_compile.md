# DVT CLI Command: `dvt compile`

## Phase: 3 (Core Rules Integration)
## Type: Full Implementation

---

## Current Implementation Summary

**File**: `core/dvt/task/compile.py`

The current `dvt compile` command:
1. Parses all project files
2. Resolves Jinja templates and macros
3. Generates compiled SQL for each model
4. Writes compiled SQL to `target/compiled/`
5. Does NOT execute any SQL

---

## Required Changes

### Full: Always Compile Using Target Adapter

DVT compile must:
1. **Always use the target adapter** for compilation (model's target or default)
2. **Fail on ephemeral models** that don't have an explicit target, with a helpful warning
3. Support multi-target compilation (different models compiled with different adapters)

### Reference Document
- `dvt_rules.md` - Section: "Compilation Rules"

---

## Implementation Details

### 1. Ephemeral Model Handling

**Key Rule**: Ephemeral models without explicit targets cannot be compiled because:
- They don't materialize, so there's no "target" to compile against
- Federation cannot execute ephemeral models
- User must either materialize the model OR set an explicit target config

**File**: `core/dvt/task/compile.py`

```python
class CompileTask(GraphRunnableTask):
    """DVT Compile Task - compiles models using their target adapters."""
    
    def compile_node(self, node: ModelNode) -> CompiledNode:
        """Compile a single node using its target adapter.
        
        Raises:
            EphemeralCompilationError: If ephemeral model has no explicit target
        """
        
        # Check for ephemeral without target
        if self._is_ephemeral_without_target(node):
            raise EphemeralCompilationError(
                model=node.unique_id,
                file_path=node.original_file_path,
            )
        
        # Resolve target for this node
        target = self._resolve_compile_target(node)
        
        # Get adapter for target
        adapter = self._get_adapter_for_target(target)
        
        # Compile using adapter's dialect
        compiled = self._compile_with_adapter(node, adapter)
        
        fire_event(ModelCompiled(
            model=node.unique_id,
            target=target,
            adapter=adapter.type(),
        ))
        
        return compiled
    
    def _is_ephemeral_without_target(self, node: ModelNode) -> bool:
        """Check if node is ephemeral without explicit target.
        
        Ephemeral models are problematic because:
        1. They don't materialize - no physical table/view
        2. Federation can't execute them (needs materialized source)
        3. Without explicit target, we don't know which adapter to use
        """
        if node.config.materialized != "ephemeral":
            return False
        
        # Check if explicit target is set
        has_explicit_target = (
            node.config.get("target") is not None or
            self.config.args.target is not None
        )
        
        return not has_explicit_target
    
    def _resolve_compile_target(self, node: ModelNode) -> str:
        """Resolve target for compilation.
        
        Priority: CLI --target > model config target > profiles.yml default
        """
        # CLI flag (highest priority)
        if self.config.args.target:
            return self.config.args.target
        
        # Model config
        if node.config.get("target"):
            return node.config["target"]
        
        # Profile default
        return self.config.profile.target
    
    def _compile_with_adapter(
        self,
        node: ModelNode,
        adapter: BaseAdapter,
    ) -> CompiledNode:
        """Compile node using specified adapter.
        
        Uses adapter's:
        - Jinja context (adapter-specific macros)
        - SQL dialect (quote characters, type mappings)
        - Relation handling
        """
        # Build Jinja context with adapter
        context = self._build_compile_context(node, adapter)
        
        # Render Jinja template
        rendered_sql = self._render_sql(node.raw_code, context)
        
        # Apply adapter-specific SQL transformations
        compiled_sql = adapter.compile_sql(rendered_sql)
        
        return CompiledNode(
            unique_id=node.unique_id,
            compiled_code=compiled_sql,
            compiled_path=self._get_compiled_path(node),
        )
```

### 2. Ephemeral Compilation Error

**File**: `core/dvt/exceptions.py`

```python
class EphemeralCompilationError(CompilationError):
    """Raised when an ephemeral model cannot be compiled.
    
    Ephemeral models without explicit targets cannot be compiled because:
    1. They don't materialize, so no target database exists
    2. Federation cannot execute ephemeral models
    3. The adapter/dialect to use is ambiguous
    """
    
    def __init__(self, model: str, file_path: str):
        self.model = model
        self.file_path = file_path
        super().__init__(self._build_message())
    
    def _build_message(self) -> str:
        return f"""
Cannot compile ephemeral model '{self.model}'.

Location: {self.file_path}

Ephemeral models without explicit targets cannot be compiled because:
1. They don't materialize to a physical table/view
2. DVT federation cannot execute ephemeral models
3. Without a target, the SQL dialect to use is ambiguous

To fix this, choose one of the following options:

Option 1: Materialize the model (RECOMMENDED)
  Change the materialization to 'table' or 'view':

  -- {self.file_path}
  {{{{ config(materialized='table') }}}}

  SELECT ...

Option 2: Set an explicit target
  If you need ephemeral behavior, set an explicit target so DVT
  knows which adapter to use for compilation:

  -- {self.file_path}
  {{{{ config(
      materialized='ephemeral',
      target='my_target'  -- Add this line
  ) }}}}

  SELECT ...

Option 3: Use CLI --target flag
  Compile with an explicit target:

  dvt compile --target my_target --select {self.model}
"""
```

### 3. Multi-Target Compilation

**File**: `core/dvt/task/compile.py`

```python
class CompileTask(GraphRunnableTask):
    """DVT Compile Task with multi-target support."""
    
    def run(self) -> CompileResult:
        """Compile all selected nodes."""
        
        # Group nodes by target for efficient compilation
        nodes_by_target = self._group_nodes_by_target()
        
        fire_event(CompileTargetSummary(
            targets=list(nodes_by_target.keys()),
            counts={t: len(nodes) for t, nodes in nodes_by_target.items()},
        ))
        
        results = []
        
        for target, nodes in nodes_by_target.items():
            fire_event(CompilingForTarget(target=target, count=len(nodes)))
            
            # Get adapter once per target
            adapter = self._get_adapter_for_target(target)
            
            for node in nodes:
                try:
                    compiled = self._compile_with_adapter(node, adapter)
                    results.append(compiled)
                    
                    # Write compiled SQL to target/compiled/
                    self._write_compiled_file(compiled)
                    
                except EphemeralCompilationError as e:
                    fire_event(EphemeralCompilationWarning(
                        model=node.unique_id,
                        message=str(e),
                    ))
                    # Don't add to results, but continue with other models
                    
                except Exception as e:
                    fire_event(CompilationError(
                        model=node.unique_id,
                        error=str(e),
                    ))
                    results.append(FailedCompilation(
                        unique_id=node.unique_id,
                        error=str(e),
                    ))
        
        return CompileResult(results=results)
    
    def _group_nodes_by_target(self) -> Dict[str, List[ModelNode]]:
        """Group nodes by their resolved target."""
        groups = defaultdict(list)
        
        for node_id in self.selected_nodes:
            node = self.manifest.nodes.get(node_id)
            if node and node.resource_type == "model":
                try:
                    target = self._resolve_compile_target(node)
                    groups[target].append(node)
                except EphemeralCompilationError:
                    # Will be handled in compile_node
                    groups["__ephemeral_errors__"].append(node)
        
        return dict(groups)
```

### 4. New Events

**File**: `core/dvt/events/types.py`

```python
@dataclass
class ModelCompiled(DebugLevel):
    model: str
    target: str
    adapter: str
    
    def message(self) -> str:
        return f"Compiled {self.model} for {self.target} ({self.adapter})"


@dataclass
class CompileTargetSummary(InfoLevel):
    targets: List[str]
    counts: Dict[str, int]
    
    def message(self) -> str:
        summary = ", ".join(f"{t}: {c}" for t, c in self.counts.items())
        return f"Compiling models by target: {summary}"


@dataclass
class CompilingForTarget(InfoLevel):
    target: str
    count: int
    
    def message(self) -> str:
        return f"Compiling {self.count} models for target '{self.target}'..."


@dataclass
class EphemeralCompilationWarning(WarnLevel):
    model: str
    message: str
    
    def message(self) -> str:
        return f"Cannot compile ephemeral model '{self.model}' - see error for details"
```

---

## CLI Output Changes

### Success Case

```
$ dvt compile
Running with dvt=1.8.0
Found 15 models

Compiling models by target: postgres_prod: 8, snowflake_dw: 5, bigquery_analytics: 2

Compiling 8 models for target 'postgres_prod'...
  staging_users: compiled
  staging_orders: compiled
  ...

Compiling 5 models for target 'snowflake_dw'...
  analytics_orders: compiled
  ...

Compiling 2 models for target 'bigquery_analytics'...
  events_summary: compiled
  ...

Compiled 15 models to target/compiled/
```

### Ephemeral Error Case

```
$ dvt compile
Running with dvt=1.8.0
Found 15 models

WARNING: Cannot compile ephemeral model 'intermediate_calc'

Cannot compile ephemeral model 'model.my_project.intermediate_calc'.

Location: models/intermediate/intermediate_calc.sql

Ephemeral models without explicit targets cannot be compiled because:
1. They don't materialize to a physical table/view
2. DVT federation cannot execute ephemeral models
3. Without a target, the SQL dialect to use is ambiguous

To fix this, choose one of the following options:

Option 1: Materialize the model (RECOMMENDED)
  Change the materialization to 'table' or 'view':

  -- models/intermediate/intermediate_calc.sql
  {{ config(materialized='table') }}

  SELECT ...

Option 2: Set an explicit target
  ...

Compiled 14 models to target/compiled/ (1 skipped)
```

---

## Example Configurations

### Model with Explicit Target (compiles successfully)

```sql
-- models/marts/combined_data.sql
{{ config(
    materialized='table',
    target='snowflake_dw'
) }}

SELECT *
FROM {{ ref('staging_users') }}
JOIN {{ ref('staging_orders') }} USING (user_id)
```

### Ephemeral with Target (compiles successfully)

```sql
-- models/intermediate/temp_calc.sql
{{ config(
    materialized='ephemeral',
    target='postgres_prod'  -- Explicit target allows compilation
) }}

SELECT
    user_id,
    SUM(amount) as total
FROM {{ ref('orders') }}
GROUP BY 1
```

### Ephemeral without Target (fails compilation)

```sql
-- models/intermediate/bad_ephemeral.sql
{{ config(materialized='ephemeral') }}
-- No target specified - WILL FAIL

SELECT * FROM {{ ref('source_data') }}
```

---

## Testing Requirements

### Unit Tests

**File**: `tests/unit/test_compile_task.py`

```python
def test_compile_uses_model_target():
    """Test compilation uses model's configured target."""
    node = MockModelNode(config={"target": "snowflake_dw"})
    
    task = CompileTask(config, manifest)
    target = task._resolve_compile_target(node)
    
    assert target == "snowflake_dw"


def test_compile_ephemeral_without_target_raises():
    """Test ephemeral without target raises error."""
    node = MockModelNode(config={"materialized": "ephemeral"})
    
    task = CompileTask(config, manifest)
    
    with pytest.raises(EphemeralCompilationError) as exc_info:
        task.compile_node(node)
    
    assert "ephemeral" in str(exc_info.value)
    assert "materialize" in str(exc_info.value)


def test_compile_ephemeral_with_target_succeeds():
    """Test ephemeral with explicit target compiles."""
    node = MockModelNode(config={
        "materialized": "ephemeral",
        "target": "postgres_prod",
    })
    
    task = CompileTask(config, manifest)
    
    # Should not raise
    compiled = task.compile_node(node)
    assert compiled is not None


def test_compile_groups_by_target():
    """Test nodes are grouped by target for compilation."""
    manifest = MockManifest(nodes={
        "model.test.m1": MockModelNode(config={"target": "postgres"}),
        "model.test.m2": MockModelNode(config={"target": "postgres"}),
        "model.test.m3": MockModelNode(config={"target": "snowflake"}),
    })
    
    task = CompileTask(config, manifest)
    groups = task._group_nodes_by_target()
    
    assert len(groups["postgres"]) == 2
    assert len(groups["snowflake"]) == 1
```

### Integration Tests

**File**: `tests/functional/test_compile.py`

```python
def test_compile_multi_target(project, multi_target_setup):
    """Test compiling models with different targets."""
    result = run_dbt(["compile"])
    
    assert result.success
    assert "postgres_prod" in result.output
    assert "snowflake_dw" in result.output


def test_compile_ephemeral_error(project):
    """Test ephemeral without target fails with helpful message."""
    write_file(
        project.project_root / "models" / "ephemeral.sql",
        "{{ config(materialized='ephemeral') }}\nSELECT 1"
    )
    
    result = run_dbt(["compile"], expect_pass=False)
    
    assert "Cannot compile ephemeral model" in result.output
    assert "Option 1: Materialize" in result.output


def test_compile_with_cli_target(project):
    """Test --target flag overrides model config."""
    result = run_dbt(["compile", "--target", "snowflake_dw"])
    
    assert result.success
    # All models compiled for snowflake
```

---

## Dependencies

- `04_parse.md` - sources must have connection property for ref resolution

## Blocked By

- `04_parse.md`

## Blocks

- `06_run.md` - run depends on compilation
- `07_test.md` - tests depend on compiled models

---

## Implementation Checklist

- [ ] Create `EphemeralCompilationError` exception with helpful message
- [ ] Add `_is_ephemeral_without_target()` check
- [ ] Add `_resolve_compile_target()` method
- [ ] Add `_compile_with_adapter()` method
- [ ] Implement `_group_nodes_by_target()` for efficient compilation
- [ ] Add compilation events
- [ ] Update CLI output to show targets
- [ ] Add unit tests for ephemeral handling
- [ ] Add unit tests for multi-target compilation
- [ ] Add integration tests
- [ ] Update documentation with ephemeral requirements
