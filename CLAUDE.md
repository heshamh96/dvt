# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

DVT (Data Virtualization Tool) is a fork of dbt-core that adds federated query capabilities across multiple data sources. It prioritizes adapter pushdown (SQL execution on the target DB) and falls back to Spark federation when cross-engine queries are needed.

**Key difference from dbt**: DVT supports multi-datasource federation using Spark JDBC connectors when models reference sources from different targets.

## Build & Development Commands

All commands run from `core/` directory using Hatch:

```bash
cd core

# Setup development environment
hatch run setup              # Install dev dependencies + pre-commit hooks

# Run tests
hatch run unit-tests                    # Run unit tests
hatch run integration-tests             # Run functional tests (requires Postgres)
hatch run integration-tests-fail-fast   # Stop on first failure

# Single test
python -m pytest tests/unit/test_file.py::TestClass::test_method

# Code quality
hatch run lint          # flake8 + mypy
hatch run code-quality  # All pre-commit hooks
hatch run black         # Format check
hatch run flake8        # Lint check
hatch run mypy          # Type check

# Database setup for integration tests
hatch run setup-db      # Starts Postgres via docker-compose

# Build package
hatch build
```

**Alternative with uv** (for running dvt locally):
```bash
cd core && uv sync
uv run --project /path/to/dvt-core/core dvt <command>
```

## Architecture

### Core Module Structure (`core/dvt/`)

- **cli/**: Click-based command interface; `main.py` defines `dvtRunner` for programmatic invocation
- **task/**: Task implementations mapping to CLI commands (e.g., `dbt run` → `RunTask`). See `task/README.md` for hierarchy
- **config/**: Profile/project configuration reconciliation (`profiles.yml`, `dbt_project.yml`, `computes.yml`)
- **parser/**: YAML/SQL parsing, validation, Python object construction
- **graph/**: DAG construction via networkx, node selection logic
- **contracts/**: Dataclass definitions for nodes, manifests, results
- **context/**: Jinja context building and exposure
- **artifacts/**: Manifest, catalog, run results schemas

### DVT-Specific Additions

- **computes.yml** (`~/.dvt/`): Spark compute engine configurations per profile
- **Federation path**: Triggered when model's target differs from upstream targets
- **`connection` in sources**: Each source block can specify which profile target it uses

### Execution Flow

1. **Compute Resolution**: CLI → model config → `computes.yml` default
2. **Target Resolution**: CLI → model config → `profiles.yml` default
3. **Execution Path**: Same-target = adapter pushdown; Cross-target = Spark federation

### Task/Runner Pattern

Tasks orchestrate; Runners execute per-node:
```
BaseTask → ConfiguredTask → GraphRunnableTask → RunTask → BuildTask
BaseRunner → CompileRunner → ModelRunner → SeedRunner
```

## Testing

- **Unit tests** (`tests/unit/`): Pure Python, no DB needed; use Hypothesis for property testing
- **Functional tests** (`tests/functional/`): End-to-end against Postgres

## Key Configuration Files

| File | Location | Purpose |
|------|----------|---------|
| `dbt_project.yml` | Project root | Project config (name, profile, paths, `require-adapters`) |
| `profiles.yml` | `~/.dvt/` | Database connection profiles |
| `computes.yml` | `~/.dvt/` | Spark compute configs (profile → target + computes) |
| `mdm.duckdb` | `~/.dvt/data/` | MDM database |

## DVT Rules Reference (from `hesham_mind/dvt_rules.md`)

1. **Pushdown preference**: Use adapter pushdown when model and inputs are same target
2. **Federation path**: Use Spark JDBC when cross-target queries required
3. **Filter optimization**: Predicate pushdown on federation path
4. **Materialization coercion**: Cross-target views become tables (can't create cross-DB view)
5. **Compute hierarchy**: CLI > model config > `computes.yml` default

## Branch Strategy

- **dev**: Main development branch
- **master**: Production releases
- Codebase rebases onto upstream dbt-core; preserve rebase compatibility
