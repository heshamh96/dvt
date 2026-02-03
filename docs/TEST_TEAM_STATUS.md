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

## What Test Team Should Do

### Prerequisites

- **Python environment** with matching CPU architecture (arm64 on Apple Silicon, x86_64 on Intel)
- **uv** installed (`brew install uv` or equivalent)
- **dbt-adapters** package (e.g. `dbt-postgres`) installed alongside dvt-core

### Workflow

1. **Install dvt-core in dev mode**:
   ```bash
   cd /Users/hex/Documents/My_Projects/DVT/dvt-core/core
   uv sync
   ```
   This may take several minutes (resolves dependencies). Ensure Python and deps match your CPU arch.

2. **Run dvt init from Testing_Playground**:
   ```bash
   cd /Users/hex/Documents/My_Projects/DVT/Testing_Playground
   uv run --project /Users/hex/Documents/My_Projects/DVT/dvt-core/core dvt init Coke_DB --skip-profile-setup
   ```

3. **Verify** (per test-team-technical-qa checklist):
   - `Coke_DB/` exists with `dvt_project.yml` and standard dirs (`models/`, `tests/`, `macros/`, `seeds/`, `snapshots/`, `analyses/`)
   - `~/.dvt/` contains `computes.yml`, `data/mdm.duckdb` (and `profiles.yml` if profile setup was run)

4. **Negative tests** (per test-team-negative-tester):
   - Invalid project names (`dvt init 123`, `dvt init my-project`)
   - Existing dir (`dvt init Coke_DB` when `Coke_DB/` exists)
   - Invalid flags (`dvt init --profile nonexistent`)

## Known Issues

- **Architecture mismatch**: If you see `mach-o file, but is an incompatible architecture`, your Python or a dependency (e.g. `rpds`) is the wrong arch. Use a venv created with a native interpreter and reinstall.
- **uv sync timeout**: Dependency resolution can take time. Run `uv sync` and wait for completion before running `dvt`.

## References

- `.cursor/rules/test-team-*.mdc` - Agent rules and responsibilities
- `docs/RUNNING_DVT.md` - How to run dvt CLI
- `docs/TEAM_AGENTS.md` - All agents overview
- `dvt-core-features/01-dvt-init/FEATURE.md` - Feature spec and acceptance criteria
