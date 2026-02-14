# DVT Refactoring Plan: Align with dbt-core Design Patterns

## Date: Feb 12, 2026
## Status: In Progress

---

## Vision

DVT should follow dbt-core's design patterns exactly so that future rebases are smooth.
All DVT functionality (federation, target-aware compilation, multi-target seeds, `dvt show`,
`dvt sync`, etc.) is preserved, but extracted from inline modifications into clean extension
points (subclasses, hooks, separate modules).

### Key Principles
1. **DVT = dbt + transparent federation layer** -- base Task/Runner patterns identical to dbt-core
2. **Spark is invisible** -- connects targets behind the scenes, users write declarative SQL
3. **SQLGlot + adapters = dialect translators** -- enabling on-the-fly target migration
4. **Buckets enhance EL performance** -- staging data in cloud storage
5. **Modular Spark** -- supports multiple versions, could be swapped for another engine
6. **Rebase-friendly** -- minimal hooks in base files, all DVT logic in subclasses

---

## Approach: Hook-Based (Option A)

- Add minimal, well-defined hook points in base dbt files (factory methods, template methods)
- DVT logic lives in DVT-specific subclasses (`DvtRunTask`, `DvtShowTask`, etc.)
- CLI wiring selects DVT subclasses via import aliases
- ~5-10 one-line additions to base files (down from ~500+ inline modifications)

---

## Pre-Refactoring State

### Inline Modifications (41 deviations total)

| Risk Level | Count | Key Files |
|---|---|---|
| CRITICAL | 5 | `task/run.py` (11 mods, ~500 lines), `task/seed.py` (replaced), `task/show.py` (6 mods), `compilation.py` (4 mods), `context/providers.py` (~30 mods) |
| MODERATE | 5 | `task/retry.py`, `parser/sources.py`, `contracts/graph/manifest.py`, `contracts/graph/nodes.py`, `artifacts/resources/types.py` |
| CLEAN | 31+ | `federation/` dir, new task files, `config/user_config.py`, CLI params |

### DVT Additions Inserted Into dbt Base Files
- **`run.py`**: `FederationModelRunner` (363-559), `NonDefaultPushdownRunner` (562-831), RunTask init/before_run/after_run/get_runner overrides
- **`show.py`**: `ShowRunnerTargetAware` (72-127), ShowTask init/runtime_init/get_runner overrides, subquery wrapping bugfix
- **`seed.py`**: Entire file replaced with Spark-based implementation
- **`compilation.py`**: `adapter` parameter threaded through 4 methods
- **`providers.py`**: `adapter` parameter on ProviderContext + generate_runtime_model_context, function() system

---

## Refactoring Phases

### Phase 0: Preparation
- Create directory structure: `dvt_tasks/`, `dvt_runners/`, `dvt_compilation/`

### Phase 1: Extract `run.py` (CRITICAL)
- Move `FederationModelRunner` -> `dvt_runners/federation_runner.py`
- Move `NonDefaultPushdownRunner` -> `dvt_runners/pushdown_runner.py`
- Create `DvtRunTask(RunTask)` in `dvt_tasks/dvt_run.py`
- Clean base `run.py` to match upstream dbt-core

### Phase 2: Extract `show.py`
- Move `ShowRunnerTargetAware` -> `dvt_runners/show_runner_target_aware.py`
- Create `DvtShowTask(ShowTask)` in `dvt_tasks/dvt_show.py`
- Keep 2-line subquery bugfix in base (genuine dbt bug)

### Phase 3: Extract `compilation.py`
- Add `_create_compiler()` factory method to `ConfiguredTask` (3 lines in base)
- Create `DvtCompiler(Compiler)` in `dvt_compilation/dvt_compiler.py`
- Remove `adapter` parameter from base `compilation.py`

### Phase 4: Clean `providers.py`
- Keep `adapter=None` parameter (2 lines, backward-compatible)
- Keep `function()` system (clean additions following existing patterns)

### Phase 5: Extract `seed.py`
- Restore original dbt `SeedRunner(ModelRunner)` and `SeedTask(RunTask)`
- Keep `SparkSeedRunner` in `spark_seed.py`
- Create `DvtSeedTask(SeedTask)` that conditionally uses Spark

### Phase 6: Extract `retry.py`
- Create `DvtRetryTask(RetryTask)` with `DROP_FLAGS` logic

### Phase 7: Wire CLI
- Change imports in `main.py` to use DVT subclasses
- Change imports in `retry.py` TASK_DICT to use DVT subclasses

### Phase 8: Handle `BuildTask`
- Create `DvtBuildTask(BuildTask)` with federation runner routing

---

## Post-Refactoring State

### Base File Changes Remaining

| File | Changes | Lines |
|---|---|---|
| `task/run.py` | ZERO | 0 |
| `task/show.py` | 2-line bugfix (subquery wrap) | 2 |
| `task/seed.py` | ZERO (restored to dbt original) | 0 |
| `task/retry.py` | ZERO | 0 |
| `compilation.py` | ZERO | 0 |
| `task/base.py` | 3-line hook (`_create_compiler` factory) | 3 |
| `context/providers.py` | 2 lines (`adapter=None`) + function() (~10 lines) | ~12 |
| `parser/sources.py` | `connection` validation (~15 lines) | ~15 |
| **Total** | | **~32 lines** (down from ~500+) |

### New DVT Extension Files

| File | Contents |
|---|---|
| `dvt_tasks/__init__.py` | Package marker |
| `dvt_tasks/dvt_run.py` | `DvtRunTask(RunTask)` |
| `dvt_tasks/dvt_show.py` | `DvtShowTask(ShowTask)` |
| `dvt_tasks/dvt_seed.py` | `DvtSeedTask(SeedTask)` |
| `dvt_tasks/dvt_retry.py` | `DvtRetryTask(RetryTask)` |
| `dvt_tasks/dvt_build.py` | `DvtBuildTask(BuildTask)` |
| `dvt_runners/__init__.py` | Package marker |
| `dvt_runners/federation_runner.py` | `FederationModelRunner` |
| `dvt_runners/pushdown_runner.py` | `NonDefaultPushdownRunner` |
| `dvt_runners/show_runner_target_aware.py` | `ShowRunnerTargetAware` |
| `dvt_compilation/__init__.py` | Package marker |
| `dvt_compilation/dvt_compiler.py` | `DvtCompiler(Compiler)` |

---

## Decisions Made

1. **Option A: Hook-based** -- minimal hooks in base files, DVT logic in subclasses
2. **Keep `function()` node type** -- clean additions following existing dbt patterns
3. **Factory method in base** -- `_create_compiler()` in `ConfiguredTask` (3 lines)
4. **Keep subquery bugfix** -- genuine dbt bug, 2 lines in `ShowRunner.execute()`
5. **Execution order**: Phase 0 -> 3 -> 4 -> 1 -> 2 -> 5 -> 6 -> 7 -> 8 (compilation first since runners depend on it)
