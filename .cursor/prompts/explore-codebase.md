# Explore Codebase

Use this prompt when you need to understand how something works in DVT.

## Quick Navigation

### By Feature Area

| Area | Key Files |
|------|-----------|
| CLI Commands | `core/dvt/cli/main.py`, `core/dvt/cli/params.py` |
| Tasks | `core/dvt/task/` (see `task/README.md` for hierarchy) |
| Runners | `core/dvt/runners/` |
| Configuration | `core/dvt/config/project.py`, `core/dvt/config/profile.py` |
| Source Connections | `core/dvt/config/source_connections.py` |
| Target Resolution | `core/dvt/config/target_resolver.py` |
| Parsing | `core/dvt/parser/` |
| DAG/Graph | `core/dvt/graph/` |
| Contracts | `core/dvt/contracts/graph/nodes.py` |
| DvtCache | `core/dvt/federation/dvt_cache.py` |
| Optimizer | `core/dvt/federation/optimizer.py` |
| Sling Extraction | `core/dvt/extraction/sling_client.py` |
| Adapters | dvt-adapters package (13 engines) |

### By Command

| Command | Task File | Key Logic |
|---------|-----------|-----------|
| `dvt init` | `task/init.py` | Project creation |
| `dvt parse` | `task/parse.py` | Manifest generation |
| `dvt compile` | `task/compile.py` | SQL compilation |
| `dvt run` | `task/run.py` | Model execution |
| `dvt test` | `task/test.py` | Test execution |
| `dvt debug` | `task/debug.py` | Config/connection checks |
| `dvt sync` | `task/sync.py` | Environment sync |
| `dvt docs` | `task/docs/` | Documentation |

## Documentation

| Doc | Purpose |
|-----|---------|
| `ARCHITECTURE.md` | High-level architecture |
| `docs/dvt_implementation_plan.md` | DVT RULES |
| `hesham_mind/dvt_rules.md` | Detailed rules |
| `core/dvt/task/README.md` | Task/Runner hierarchy |
| `CONTRIBUTING.md` | Development guide |

## Search Patterns

```bash
# Find where something is defined
grep -r "class MyClass" core/dvt/

# Find where something is used
grep -r "from dvt.module import" core/dvt/

# Find CLI command
grep -r "@cli.command" core/dvt/cli/

# Find task implementation
grep -r "class.*Task" core/dvt/task/
```

## Tracing Execution

1. **Start at CLI**: `core/dvt/cli/main.py`
2. **Find command**: `@cli.command("mycommand")`
3. **Find task**: Look for `run_task(ctx, MyTask, kwargs)`
4. **Read task**: `core/dvt/task/mytask.py`
5. **Check base class**: Follow inheritance in `task/README.md`

## Key Patterns

### Task Pattern
```
CLI Command -> Task.run() -> Runner.run() -> Adapter/Extraction
```

### Execution Paths (Rule 3)
```
Default pushdown:     same target -> adapter pushdown
Non-default pushdown: different target, same refs -> adapter pushdown on non-default
Extraction:           refs span targets -> Sling -> DvtCache (DuckDB) -> materialize
```

### Resolution Pattern
```
CLI args -> Config files -> Defaults
```

### Parsing Pattern
```
Files -> Parser -> Contracts -> Manifest -> DAG
```
