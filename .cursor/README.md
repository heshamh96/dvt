# DVT Cursor Configuration

This directory contains Cursor IDE configurations for DVT development.

## Structure

```
.cursor/
├── README.md           # This file
├── rules/              # Agent rules (.mdc files)
│   ├── project.mdc     # Global project rules
│   ├── dev-team-*.mdc  # Development team agents
│   └── test-team-*.mdc # Test team agents
└── prompts/            # Reusable prompts for common tasks
    ├── implement-cli-command.md
    ├── add-unit-test.md
    ├── add-functional-test.md
    ├── add-config-option.md
    ├── add-dvt-feature.md
    ├── create-trial.md
    ├── debug-issue.md
    ├── fix-lint-errors.md
    ├── commit-changes.md
    ├── explore-codebase.md
    └── review-dvt-rules.md
```

## Agent Teams

### Development Team (`dev-team-*`)

| Agent | Scope |
|-------|-------|
| `dev-team-architecture` | Design decisions, DVT rules compliance, cross-cutting concerns |
| `dev-team-backend` | CLI commands, config loading, manifest, DAG construction |
| `dev-team-cli` | Click command structure, parameters, dvtRunner API |
| `dev-team-parser` | YAML/SQL parsing, manifest generation, Jinja context |
| `dev-team-sync` | `dvt sync` command, adapter installation |
| `dev-team-federation` | DvtCache (DuckDB), Sling extraction, cross-DB queries, predicate pushdown |
| `dev-team-adapters` | Database adapters (13 engines), Sling connectivity, target management |
| `dev-team-mdm-types` | MDM database, type mappings, schema caching |
| `dev-team-qa` | Testing infrastructure, CI test jobs |
| `dev-team-deploy` | Build, CI/CD, release management |
| `dev-team-docs` | Documentation, `dvt docs` command |

### Test Team (`test-team-*`)

| Agent | Scope |
|-------|-------|
| `test-team-data-engineer` | dbt compatibility testing, reference project validation (Coke_DB) |
| `test-team-technical-qa` | Paths, names, files, design compliance |
| `test-team-negative-tester` | Edge cases, invalid inputs, error handling |

## Prompts

Reusable prompts in `.cursor/prompts/` for common development tasks:

| Prompt | Use Case |
|--------|----------|
| `implement-cli-command.md` | Adding new CLI commands |
| `add-unit-test.md` | Writing unit tests |
| `add-functional-test.md` | Writing integration tests |
| `add-config-option.md` | Adding configuration options |
| `add-dvt-feature.md` | Implementing new features |
| `create-trial.md` | Creating test trials in Testing_Playground |
| `debug-issue.md` | Debugging DVT issues |
| `fix-lint-errors.md` | Fixing linting/type errors |
| `commit-changes.md` | Committing with proper format |
| `explore-codebase.md` | Understanding code structure |
| `review-dvt-rules.md` | Checking DVT rules compliance |

## Quick Reference

### Repository

- **Repo**: `git@github.com:heshamh96/dvt-ce.git`
- **Branch**: `new_dev` (main development)
- **Packages**: dvt-ce (dbt.* + dvt.*) and dvt-adapters (dbt.adapters.* + dbt.include.*, 13 engines)
- **No external dbt**: No dbt-core or dbt-adapters from PyPI

### Development Commands

```bash
cd core

# Setup
hatch run setup                    # Install dev dependencies + pre-commit

# Testing
hatch run unit-tests               # Unit tests
hatch run integration-tests        # Functional tests (needs Postgres)
python -m pytest tests/unit/test_file.py::test_name  # Single test

# Code quality
hatch run lint                     # flake8 + mypy
hatch run code-quality             # All pre-commit hooks
```

### DVT Commands

```bash
# From a project directory with dvt installed
dvt init <project_name>            # Create new project
dvt parse                          # Parse project, write manifest
dvt debug                          # Configuration and connection checks
dvt sync                           # Sync adapters from configs
dvt compile                        # Compile models
dvt run                            # Run models
dvt test                           # Run tests
dvt docs generate                  # Generate docs
```

### Execution Paths (Rule 3)

- **Default pushdown**: Model target = default, all refs same target -> adapter pushdown
- **Non-default pushdown**: Different target, all refs share same target -> adapter pushdown
- **Extraction**: Refs span targets -> Sling extraction to DvtCache (DuckDB) -> materialize

## Key Files

| File | Location | Purpose |
|------|----------|---------|
| `dbt_project.yml` | Project root | Project configuration |
| `profiles.yml` | `~/.dvt/` | Database connections |
| `mdm.duckdb` | `~/.dvt/data/` | MDM database |
| `dvt_implementation_plan.md` | `docs/` | Canonical DVT RULES |

## Key Code Paths

| Module | Path | Purpose |
|--------|------|---------|
| DvtCache | `core/dvt/federation/dvt_cache.py` | DuckDB-backed cache engine |
| Optimizer | `core/dvt/federation/optimizer.py` | Query optimization |
| Sling Client | `core/dvt/extraction/sling_client.py` | Sling streaming extraction |
| Target Resolver | `core/dvt/config/target_resolver.py` | Target/execution path resolution |
| Source Connections | `core/dvt/config/source_connections.py` | Source connection configs |
| Runners | `core/dvt/runners/` | Runner implementations |
| Tasks | `core/dvt/tasks/` | Task definitions |

## DVT Rules Summary

1. **Target Resolution**: CLI > model config > profiles.yml default
2. **Execution Path**: 3 paths (default pushdown, non-default pushdown, extraction)
3. **Materialization**: Extraction path views coerced to tables
4. **Filter Optimization**: Predicate pushdown on extraction path

## Testing Playground

Test team agents use: `/Users/hex/Documents/My_Projects/DVT/Testing_Factory/Testing_Playground`

Each test run creates a **trial folder**: `trial_<feature>_<number>`
- Each trial is a self-contained uv project with `pyproject.toml`
- Run `uv sync` then `uv run dvt ...` from within the trial
- Write findings under `trial_xxx/findings/`
- Reference test project: Coke_DB

## Related Documentation

- [TEAM_AGENTS.md](../docs/TEAM_AGENTS.md) - Full agent roster
- [RUNNING_DVT.md](../docs/RUNNING_DVT.md) - How to run DVT
- [CONTRIBUTING.md](../CONTRIBUTING.md) - Contribution guide
- [dvt_implementation_plan.md](../docs/dvt_implementation_plan.md) - DVT RULES
