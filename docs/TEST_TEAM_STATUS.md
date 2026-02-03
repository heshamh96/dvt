# Test Team Status

## What's Done

1. **Test team agents created** (`.cursor/rules/test-team-*.mdc`):
   - `test-team-data-engineer`: Uses Cocacola_DWH_on_DBT reference, Coke_DB scenario
   - `test-team-technical-qa`: Paths/names/files checklist per feature spec
   - `test-team-negative-tester`: Bad inputs, edge cases, clear errors

2. **Package workflow documented**: All agents require:
   - Build and install dvt-core in dev mode: `cd core && uv sync`
   - Run installed `dvt` CLI from Testing_Playground (not standalone scripts)

3. **Adapters shim** (`core/dvt/adapters/__init__.py`): Enables CLI to use existing `dbt-adapters` package

4. **Testing_Playground** (`/Users/hex/Documents/My_Projects/DVT/Testing_Playground`): Directory for all test runs

5. **Trial structure**: Do not remove or overwrite test runs. Each run goes in a folder: `trial_<what_we_are_testing>_<number>` (e.g. `trial_dvt_init_1`). **Each trial is a self-contained uv project**: the trial has its own uv (pyproject.toml), its own .venv, and dvt-core installed inside it. You cd into the trial and run `uv run dvt ...` there. **Findings** are written under that folder in `findings/`. All agents follow this; see `.cursor/rules/test-team-*.mdc`.

## What Test Team Should Do

### Each trial is a uv-contained project (how to test)

**Each trial folder is a self-contained uv project:** it has its own uv setup, its own .venv, and the tool (dvt-core) installed inside it. So each trial is a uv-contained folder/environment.

1. **Create a trial folder**: e.g. `Testing_Playground/trial_dvt_init_1`.
2. **Make the trial a uv project and install the tool inside it**:
   ```bash
   cd /Users/hex/Documents/My_Projects/DVT/Testing_Playground/trial_dvt_init_1
   uv init
   uv add /Users/hex/Documents/My_Projects/DVT/dvt-core/core
   uv sync
   ```
   The trial now has its own `.venv` and `dvt` installed inside it.
3. **Run dvt from inside the trial**:
   ```bash
   cd /Users/hex/Documents/My_Projects/DVT/Testing_Playground/trial_dvt_init_1
   uv run dvt init Coke_DB --skip-profile-setup
   ```
   No `--project` pointing elsewhere; the tool runs from the trial’s own env.
4. **Write findings** under `trial_dvt_init_1/findings/`.

### Prerequisites

- **Python environment** with matching CPU architecture (arm64 on Apple Silicon, x86_64 on Intel)
- **uv** installed on your system (`brew install uv` or equivalent)
- **dbt-adapters** package (e.g. `dbt-postgres`) installed alongside dvt-core when you run `uv sync` in core

### Workflow

1. **Create a trial folder** (do not remove existing ones): e.g. `Testing_Playground/trial_dvt_init_1`.

2. **Make the trial a uv project and install dvt-core inside it**:
   ```bash
   cd /Users/hex/Documents/My_Projects/DVT/Testing_Playground/trial_dvt_init_1
   uv init
   uv add /Users/hex/Documents/My_Projects/DVT/dvt-core/core
   uv sync
   ```
   The trial now has its own .venv and dvt; ensure Python/deps match your CPU arch.

3. **Run dvt init from inside the trial** (so the project is created inside the trial):
   ```bash
   cd /Users/hex/Documents/My_Projects/DVT/Testing_Playground/trial_dvt_init_1
   uv run dvt init Coke_DB --skip-profile-setup
   ```

4. **Verify** (per test-team-technical-qa checklist):
   - `Coke_DB/` exists with `dvt_project.yml` and standard dirs (`models/`, `tests/`, `macros/`, `seeds/`, `snapshots/`, `analyses/`)
   - `~/.dvt/` contains `computes.yml`, `data/mdm.duckdb` (and `profiles.yml` if profile setup was run)

5. **Write findings** under the trial folder: `trial_dvt_init_1/findings/` (e.g. findings.md or per-agent files).

6. **Negative tests** (per test-team-negative-tester, in a separate trial folder if desired):
   - Invalid project names (`dvt init 123`, `dvt init my-project`)
   - Existing dir (`dvt init Coke_DB` when `Coke_DB/` exists)
   - Invalid flags (`dvt init --profile nonexistent`)

## Known Issues

- **Architecture mismatch**: If you see `mach-o file, but is an incompatible architecture`, your Python or a dependency (e.g. `rpds`) is the wrong arch. Use a venv created with a native interpreter and reinstall.
- **uv sync timeout**: Dependency resolution can take time. Run `uv sync` and wait for completion before running `dvt`.

## References

- **`docs/TEST_TEAM_FINDINGS.md`** - Test team findings (what’s verified, what’s blocked, recommendations)
- `.cursor/rules/test-team-*.mdc` - Agent rules and responsibilities
- `docs/RUNNING_DVT.md` - How to run dvt CLI
- `docs/TEAM_AGENTS.md` - All agents overview
- `dvt-core-features/01-dvt-init/FEATURE.md` - Feature spec and acceptance criteria
- `dvt-core-features/02-dvt-debug/FEATURE.md` - Feature 02 dvt debug (targets = current profile only)
- `Testing_Playground/trial_dvt_debug_1/findings/` - Feature 02 debug trial findings
