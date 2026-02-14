# Rebase Strategy 1: DVT onto Upstream dbt-core

**Date:** February 2026
**Author:** Claude Code (architect analysis)
**Status:** Proposed

---

## 1. Executive Summary

Rebasing DVT onto upstream dbt-core is **moderately difficult** -- estimated **2-3 days** of focused work for an experienced developer. The recent refactoring (commit `14b9a04a5`) that extracted DVT logic into subclass extensions was the single best architectural decision for rebase compatibility. Without it, the conflict surface would be 5-10x larger.

**Bottom line:** DVT's subclass-based architecture keeps 91 DVT-specific files (18,643 lines) completely conflict-free. The real work is in ~12 base files with behavioral changes and the mechanical overhead of the `dbt` -> `dvt` package rename.

---

## 2. Current State

### Branch Structure

| Branch | Purpose |
|--------|---------|
| `dev` | DVT development (49 commits ahead of fork) |
| `master` | Production releases |
| `upstream/main` | Upstream dbt-core tracking |

### Fork Point

- **Fork commit:** `8034b8e07` on `upstream/main` ("Small fixes in post (#12317)")
- **Upstream distance since fork:** 44 commits (~2,800 insertions, ~740 deletions across 31 files)
- **DVT distance since fork:** 49 DVT-specific commits

---

## 3. DVT vs dbt-core CLI Compatibility

### Command Compatibility Table

| # | Command | dbt-core | DVT | Task Override | DVT-Specific Flags | Status |
|---|---------|----------|-----|---------------|--------------------|--------|
| 1 | `build` | Yes | Yes | `DvtBuildTask` | `--compute`, `--bucket`, `--sample` | Full + Enhanced |
| 2 | `run` | Yes | Yes | `DvtRunTask` | `--compute`, `--bucket`, `--sample` | Full + Enhanced |
| 3 | `compile` | Yes | Yes | `DvtCompileTask` | -- | Full + Enhanced |
| 4 | `show` | Yes | Yes | `DvtShowTask` | `--inline-direct` | Full + Enhanced |
| 5 | `seed` | Yes | Yes | `DvtSeedTask` | `--compute` | Full + Enhanced |
| 6 | `retry` | Yes | Yes | `DvtRetryTask` | -- | Full + Enhanced |
| 7 | `test` | Yes | Yes | -- (standard) | -- | Full (passthrough) |
| 8 | `parse` | Yes | Yes | -- (standard) | -- | Full (passthrough) |
| 9 | `clean` | Yes | Yes | -- (extended) | `--bucket`, `--older-than` | Full + Enhanced |
| 10 | `deps` | Yes | Yes | -- (standard) | -- | Full (passthrough) |
| 11 | `init` | Yes | Yes | -- (extended) | -- | Full + Enhanced |
| 12 | `debug` | Yes | Yes | -- (rewritten) | `--connection`, `--config`, `--manifest`, `--debug-target`, `--debug-compute`, `--debug-bucket` | Full + Enhanced |
| 13 | `list` / `ls` | Yes | Yes | -- (standard) | -- | Full (passthrough) |
| 14 | `clone` | Yes | Yes | -- (standard) | -- | Full (passthrough) |
| 15 | `run-operation` | Yes | Yes | -- (standard) | -- | Full (passthrough) |
| 16 | `snapshot` | Yes | Yes | -- (standard) | -- | Full (passthrough) |
| 17 | `source freshness` | Yes | Yes | -- (standard) | -- | Full (passthrough) |
| 18 | `docs generate` | Yes | Yes | -- (standard) | -- | Full (passthrough) |
| 19 | `docs serve` | Yes | Yes | -- (standard) | -- | Full (passthrough) |
| 20 | **`sync`** | **No** | **Yes** | `SyncTask` | `--python-env` | **DVT-only** |

### Compatibility Summary

| Metric | Value |
|--------|-------|
| dbt-core commands supported by DVT | **19/19 (100%)** |
| DVT-enhanced commands (custom task override or extended behavior) | 12 |
| DVT passthrough commands (unchanged dbt behavior) | 7 |
| DVT-only commands (not in dbt) | 1 (`sync`) |
| dbt-core global flags (44) supported | **44/44 (100%)** |
| dbt-core command-specific flags (~85 unique) supported | **~85/~85 (100%)** |
| DVT-added flags | 13 |
| **Overall dbt-core compatibility** | **100%** |

### DVT-Added Flags (13 total)

| Flag | Commands | Purpose |
|------|----------|---------|
| `--compute`, `-c` | `build`, `run`, `seed` | Select compute engine from `computes.yml` |
| `--bucket`, `-b` | `build`, `run` | Select staging bucket for federation |
| `--sample` | `build`, `run` | Sample mode with window spec |
| `--inline-direct` | `show` | Direct SQL without Jinja templating (hidden) |
| `--connection` | `debug` | Test connection to a specific target |
| `--config` | `debug` | Show resolved config only |
| `--manifest` | `debug` | Show manifest summary only |
| `--debug-target` | `debug` | Debug a specific target |
| `--debug-compute` | `debug` | Debug a specific compute |
| `--debug-bucket` | `debug` | Debug a specific bucket |
| `--python-env` | `sync` | Path to Python venv for sync |
| `--bucket` (clean) | `clean` | Clean specific staging bucket |
| `--older-than` | `clean` | Clean files older than duration |

---

## 4. DVT Architecture Overview

### Extension Architecture

```
                    +-----------------------------+
                    |     dbt-core base files      |
                    |  (task/, compilation, cli/)   |
                    |     Mostly untouched          |
                    +--------------+----------------+
                                   | extends
         +-------------------------+---------------------------+
         |                         |                           |
  +------+------+          +-------+-------+          +--------+------+
  |  dvt_tasks/ |          | dvt_runners/  |          |dvt_compilation|
  |  6 classes  |          |  5 classes    |          |  1 class      |
  +-------------+          +-------+-------+          +---------------+
                                   | uses
                           +-------+-------+
                           |  federation/  |
                           |  64 files     |
                           |  (100% new)   |
                           +---------------+
```

### Task Override Chain

| CLI Command | DVT Task | DVT Runner(s) | Compiler |
|-------------|----------|---------------|----------|
| `dvt run` | `DvtRunTask` | `FederationModelRunner`, `NonDefaultPushdownRunner`, `ModelRunner` | `DvtCompiler` |
| `dvt build` | `DvtBuildTask` | Same as run (for models) + `SparkSeedRunner` (for seeds) | `DvtCompiler` |
| `dvt compile` | `DvtCompileTask` | `DvtCompileRunner` | `DvtCompiler` |
| `dvt show` | `DvtShowTask` | `ShowRunnerTargetAware`, `ShowRunner` | `DvtCompiler` |
| `dvt seed` | `DvtSeedTask` | `SparkSeedRunner` | -- |
| `dvt retry` | `DvtRetryTask` | Delegates to above tasks | Inherits |

### CLI Wiring Pattern (Low Conflict)

DVT uses lazy imports with aliasing in `cli/main.py`:

```python
# 5 import swaps -- the ONLY changes needed in the CLI function bodies
from dvt.dvt_tasks.dvt_build import DvtBuildTask as BuildTask
from dvt.dvt_tasks.dvt_run import DvtRunTask as RunTask
from dvt.dvt_tasks.dvt_compile import DvtCompileTask as CompileTask
from dvt.dvt_tasks.dvt_show import DvtShowTask as ShowTask
from dvt.dvt_tasks.dvt_seed import DvtSeedTask as SeedTask
from dvt.dvt_tasks.dvt_retry import DvtRetryTask as RetryTask
```

Function signatures remain unchanged. This is the lowest-friction substitution pattern possible.

---

## 5. Conflict Surface Analysis

### Files by Conflict Risk

| Category | Files | Lines | Conflict Risk |
|----------|-------|-------|---------------|
| DVT-added files (dvt_tasks/, dvt_runners/, dvt_compilation/, federation/) | 91 | 18,643 | **Zero** |
| Base files unchanged (post-rename normalization) | 40 | -- | **Zero** |
| Base files with formatting-only changes | ~120 | -- | **Low** (mechanical) |
| Base files with real behavioral changes | ~12 | -- | **Medium to High** |

### The 12 Real Conflict Files

#### HIGH RISK (upstream also changed these heavily)

| File | DVT Change | Upstream Change | Strategy |
|------|-----------|-----------------|----------|
| `contracts/graph/unparsed.py` | Formatting only | +475 lines (25 new Semantic Layer v2 classes) | Accept upstream wholesale |
| `parser/schema_yaml_readers.py` | Formatting only | +532 lines (16 new SL v2 parsing methods) | Accept upstream wholesale |
| `parser/manifest.py` | +1 method (`check_valid_target_config`) + formatting | +185 lines (bug fixes, SL parsing) | Accept upstream, re-append DVT method |

#### MEDIUM RISK (one side changed heavily)

| File | DVT Change | Upstream Change | Strategy |
|------|-----------|-----------------|----------|
| `compilation.py` | Removed `selected_node_ids` + formatting | +12 lines (FK deferred logic added `selected_node_ids`) | Accept upstream's addition back |
| `task/run.py` | Formatting only | +49 lines (microbatch deadlock fix) | Accept upstream |
| `task/runnable.py` | Formatting only | +32 lines (concurrent fix) | Accept upstream |
| `cli/flags.py` | Rewritten `args_to_context()` | No change | Keep DVT version |
| `config/project.py` | Dual project file fallback | No change | Keep DVT version |
| `task/clean.py` | +374 lines (bucket cleanup) | No change | Keep DVT version |
| `task/init.py` | +123 lines (~/.dvt/ scaffolding) | No change | Keep DVT version |

#### LOW RISK (DVT additions, no upstream overlap)

| File | DVT Change | Upstream Change | Strategy |
|------|-----------|-----------------|----------|
| `cli/main.py` | 5 import swaps + `sync` command + new params | +1 line | Keep DVT, trivial merge |
| `cli/requires.py` | Error deduplication + clean messages | No change | Keep DVT version |
| `cli/exceptions.py` | Simplified ExceptionExit | No change | Keep DVT version |
| `cli/params.py` | 13 new DVT parameters appended | No change | Keep DVT version |
| `parser/sources.py` | `connection` validation added | No change | Keep DVT version |

#### UNMERGEABLE (complete rewrite)

| File | DVT Change | Strategy |
|------|-----------|----------|
| `task/debug.py` | +1,490 lines -- near-complete rewrite with rich formatting, computes/buckets/JDBC/Spark diagnostics | Keep DVT version; manually port any upstream debug changes |

---

## 6. The #1 Obstacle: Package Rename (`dbt` -> `dvt`)

Every file in the codebase has had imports changed:
- `from dbt.X` -> `from dvt.X`
- `dbtRunner` -> `dvtRunner`
- `core/dbt/` directory -> `core/dvt/`

This means **`git rebase upstream/main` will not work directly**. Upstream's files are in `core/dbt/`, DVT's are in `core/dvt/`.

### Solution: Automated Rename Script

Create a transformation script that:

1. Takes an upstream commit
2. Renames `core/dbt/` -> `core/dvt/` in file paths
3. Applies `s/\bfrom dbt\./from dvt./g` and similar transforms on file contents
4. Preserves `dbt_common`, `dbt_adapters`, `dbt_semantic_interfaces` (external packages, NOT renamed)
5. Handles edge cases: `dbt_project.yml` references, test fixtures, docstrings

```bash
# Pseudocode for rename script
#!/bin/bash
# rename_upstream_commit.sh

# 1. Apply the upstream patch
git cherry-pick --no-commit $UPSTREAM_COMMIT

# 2. Move files from dbt/ to dvt/
git mv core/dbt/ core/dvt/ 2>/dev/null || true

# 3. Transform imports in Python files
find core/dvt/ -name '*.py' -exec sed -i '' \
  -e 's/from dbt\.\(adapters\|common\|semantic\)/DO_NOT_RENAME/g' \
  -e 's/from dbt\./from dvt./g' \
  -e 's/import dbt\./import dvt./g' \
  -e 's/DO_NOT_RENAME/from dbt.\1/g' \
  {} +

# 4. Commit with attribution
git commit -m "upstream: $(git log -1 --format='%s' $UPSTREAM_COMMIT)"
```

---

## 7. Upstream Gap: Semantic Layer v2

The largest functional gap between DVT and upstream is the **Semantic Layer v2** additions (44 upstream commits since fork). Key additions DVT is missing:

| Upstream Addition | Files Affected | Impact on DVT |
|-------------------|---------------|---------------|
| 25 new SL v2 dataclass types | `contracts/graph/unparsed.py` | No DVT conflict -- pure additions |
| 16 new YAML parsing methods | `parser/schema_yaml_readers.py` | No DVT conflict -- pure additions |
| FK constraint deferred logic | `compilation.py` | Minor conflict -- DVT removed `selected_node_ids` |
| Microbatch deadlock fix | `task/run.py` | No DVT conflict -- formatting only |
| Bug fixes (5-10 commits) | Various | Low conflict |

**Strategy:** Accept all Semantic Layer v2 additions wholesale. DVT doesn't modify these areas semantically -- the conflicts are purely formatting-based.

---

## 8. Recommended Rebase Procedure

### Phase 1: Preparation (1 hour)

1. **Create the rename script** (see Section 6)
2. **Create a clean rebase branch** from current `dev`
3. **Identify upstream commits to absorb** (44 commits since fork)

### Phase 2: De-noise (2 hours)

4. **Revert DVT's Black formatting changes in base files** -- create a commit that restores the original dbt-core formatting in the ~120 files with formatting-only changes. This eliminates mechanical conflicts.

   ```bash
   # For each file that only has formatting changes:
   git show upstream/main:core/dbt/path/to/file.py | \
     sed 's/from dbt\./from dvt./g' > core/dvt/path/to/file.py
   ```

5. **Commit as:** `chore: revert formatting to match upstream (pre-rebase)`

### Phase 3: Cherry-Pick Upstream (4-6 hours)

6. **For each upstream commit** (oldest to newest):
   a. Run the rename script to transform the commit
   b. Resolve conflicts in the ~12 behavioral-change files
   c. Test: `uv run --project core python -m pytest tests/unit -q --tb=short`
   d. Commit

**Expected conflicts per commit:** Most upstream commits touch 1-3 files. Only ~5 commits will have real conflicts (the SL v2 batch, the microbatch fix, the FK constraint addition).

### Phase 4: Re-apply DVT Specifics (2 hours)

7. **Re-apply Black formatting** as a single commit
8. **Verify all DVT additions are intact:**
   - `dvt_tasks/` -- 6 task overrides
   - `dvt_runners/` -- 5 runners
   - `dvt_compilation/` -- DvtCompiler
   - `federation/` -- 64 files
   - CLI wiring -- 5 import swaps + sync command
   - Validation -- `check_valid_target_config`, `_validate_source_connection`
   - Error handling -- clean CLI error output

### Phase 5: Validation (2 hours)

9. **Unit tests:** `uv run --project core python -m pytest tests/unit -q` -- expect 1861+ passed, 24 pre-existing failures
10. **E2E tests on Trial 15:**
    - `dvt compile --select pushdown_pg_only` (pg->pg no-op)
    - `dvt compile --select pushdown_pg_only --target dbx_dev` (pg->databricks transpilation)
    - `dvt show --select pushdown_pg_only` (execution on postgres)
    - `dvt parse` with deliberate bad connection (clean error message)
11. **Smoke test all commands:** `dvt debug`, `dvt list`, `dvt deps`, `dvt clean`

---

## 9. Risk Mitigation

| Risk | Mitigation |
|------|-----------|
| Rename script misses edge cases | Run full test suite after each cherry-pick batch |
| SL v2 types break DVT's parser | Accept upstream types unchanged; DVT's parser extensions are additive |
| `task/debug.py` upstream changes lost | Manually diff upstream debug.py against DVT's rewrite; port any new diagnostics |
| `compilation.py` selected_node_ids conflict | Accept upstream's addition; verify DvtCompiler doesn't depend on its absence |
| Regression in federation routing | E2E tests on Trial 15 catch routing issues immediately |

---

## 10. Why This Is Manageable

The recent refactoring commit (`14b9a04a5 refactor(architecture): extract DVT logic from dbt-core base files into subclass extensions`) was the critical enabler. Before that refactoring:

- DVT's federation logic was **inlined** into `task/run.py`, `task/show.py`, `compilation.py`
- Every upstream change to those files would conflict with DVT's inline additions
- Rebase would have been a **week-long** effort with high regression risk

After the refactoring:

- DVT's logic lives in **separate files** (`dvt_tasks/`, `dvt_runners/`, `dvt_compilation/`)
- Base files are restored close to upstream
- CLI wiring is **5 lazy-import swaps**
- The 91 DVT-specific files (18,643 lines) have **zero conflict risk**
- Rebase is **2-3 days** of focused, predictable work

---

## 11. Future Rebase Considerations

To make future rebases even easier:

1. **Automate the rename script** and integrate it into CI
2. **Minimize base file modifications** -- prefer subclass extensions over inline changes
3. **Avoid Black reformatting base files** -- formatting changes inflate the diff and create mechanical conflicts
4. **Track upstream releases** -- rebase frequently (every 1-2 upstream releases) rather than letting the gap grow
5. **Consider a git filter** that auto-applies the dbt->dvt rename on merge, allowing `git merge upstream/main` to work semi-automatically
