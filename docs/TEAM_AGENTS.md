# DVT Team Agents

This document provides an index of all **dev-team-*** and **test-team-*** sub-agents used in DVT development. Each agent has a specific scope and set of responsibilities.

## Overview

All sub-agents follow these principles:
- **Repository**: Work is done in the DVT repository (`git@github.com:heshamh96/dvt.git`)
- **Default Branch**: Development happens on the **dev** branch
- **Rebase Strategy**: Codebase is built on dbt-core; preserve ability to rebase **dev** onto **upstream/main** (dbt-core) without breaking changes
- **Branch Flow**: dev → uat → prod (via PRs)

## Agent Roster

| Agent ID | Cursor Rule File | Scope | When to Use |
|----------|------------------|-------|-------------|
| **project** | `.cursor/rules/project.mdc` | Global project rules, structure, conventions | Always active - provides project-wide context |
| **dev-team-architecture** | `.cursor/rules/dev-team-architecture.mdc` | Design, DVT rules compliance, cross-cutting concerns | When making architectural decisions, reviewing DVT RULES compliance, or working on execution path resolution |
| **dev-team-backend** | `.cursor/rules/dev-team-backend.mdc` | CLI commands, configuration, manifest, DAG | When implementing CLI commands (`dvt init`, `dvt compile`, `dvt run`), config loading, or DAG construction |
| **dev-team-cli** | `.cursor/rules/dev-team-cli.mdc` | Click commands, parameters, dvtRunner API | When working on CLI structure, parameters, or programmatic API |
| **dev-team-parser** | `.cursor/rules/dev-team-parser.mdc` | YAML/SQL parsing, manifest generation | When working on parsing logic, schema validation, or Jinja context |
| **dev-team-sync** | `.cursor/rules/dev-team-sync.mdc` | dvt sync, adapter installation, pyspark management | When working on `dvt sync` or environment management |
| **dev-team-federation** | `.cursor/rules/dev-team-federation.mdc` | Spark federation engine, cross-database queries | When working on federation engine, Spark integration, predicate pushdown, or SQL dialect translation |
| **dev-team-adapters** | `.cursor/rules/dev-team-adapters.mdc` | Database adapters, JDBC connectivity, target management | When implementing adapters, JDBC connectivity, or `dvt target` commands |
| **dev-team-mdm-types** | `.cursor/rules/dev-team-mdm-types.mdc` | MDM database, datatype mappings, type conversion | When working on MDM database, type system, column naming rules, or schema caching |
| **dev-team-qa** | `.cursor/rules/dev-team-qa.mdc` | Testing, test infrastructure, CI test jobs | When writing tests, setting up test infrastructure, or working on CI test workflows |
| **dev-team-deploy** | `.cursor/rules/dev-team-deploy.mdc` | Build, CI/CD, release management, branch strategy | When setting up CI/CD workflows, managing releases, or enforcing branch strategy |
| **dev-team-docs** | `.cursor/rules/dev-team-docs.mdc` | Documentation, `dvt docs` command | When writing documentation, implementing `dvt docs`, or maintaining contributor guides |
| **test-team-data-engineer** | `.cursor/rules/test-team-data-engineer.mdc` | dbt-aligned testing, reference project (Cocacola_DWH_on_DBT), Coke_DB scenario | When testing features with a real dbt reference, verifying dvt init/output matches dbt mindset |
| **test-team-technical-qa** | `.cursor/rules/test-team-technical-qa.mdc` | Paths, names, files, design compliance | When verifying paths, names, and file structure match feature specs and Rule 10 |
| **test-team-negative-tester** | `.cursor/rules/test-team-negative-tester.mdc` | Bad inputs, user errors, robustness | When testing edge cases, invalid inputs, and immunity to user errors |

## How to Use Agents

### For AI Assistants

When working on a specific area, reference the appropriate agent's Cursor rule file. For example:
- Working on federation engine? Use `dev-team-federation` rules
- Writing tests? Use `dev-team-qa` rules
- Implementing CLI commands? Use `dev-team-backend` rules
- Testing with a real dbt project (e.g. dvt init Coke_DB)? Use `test-team-data-engineer` rules
- Verifying paths and file layout? Use `test-team-technical-qa` rules
- Testing bad inputs and edge cases? Use `test-team-negative-tester` rules

### For Human Developers

1. **Identify your task** - Determine which agent's scope your work falls into
2. **Read the agent's rule file** - Check `.cursor/rules/dev-team-<name>.mdc` for specific guidelines
3. **Follow the instructions** - Each agent has specific instructions for their domain
4. **Reference DVT RULES** - All agents reference the canonical DVT RULES from `dvt_implementation_plan.md`

## Agent Responsibilities Summary

### dev-team-architecture
- Ensures compliance with DVT RULES
- Reviews architectural decisions
- Maintains consistency with dvt-core-features
- Handles execution path resolution logic

### dev-team-backend
- Implements CLI commands
- Manages configuration loading
- Builds manifest and DAG
- Handles project initialization

### dev-team-federation
- Implements Spark federation engine
- Optimizes query execution (predicate pushdown)
- Translates SQL between dialects
- Manages Spark sessions

### dev-team-adapters
- Implements database adapters
- Manages JDBC connectivity
- Handles target management (`dvt target`)
- Provides pushdown capabilities

### dev-team-mdm-types
- Manages MDM database
- Implements type conversion system
- Handles column naming rules
- Caches schema information

### dev-team-qa
- Writes and maintains tests
- Sets up test infrastructure
- Manages CI test workflows
- Ensures code quality

### dev-team-deploy
- Sets up CI/CD pipelines
- Manages releases and versioning
- Enforces branch strategy
- Handles build processes

### dev-team-docs
- Writes user documentation
- Implements `dvt docs` command
- Maintains contributor guides
- Documents features and APIs

### Test team

#### test-team-data-engineer
- Uses Cocacola_DWH_on_DBT as reference dbt project (postgres flavour)
- Creates and verifies DVT projects (e.g. Coke_DB) for dvt init and related features
- Ensures dbt mindset and profile/layout compatibility; pulls needed files into test fixtures

#### test-team-technical-qa
- Verifies paths (~/.dvt/, project dirs), names, and file contents match design
- Checks idempotency and overwrite behaviour; validates against Rule 10 and feature specs

#### test-team-negative-tester
- Challenges features with invalid project names, paths, CLI usage, and bad state
- Ensures clear, actionable errors and no silent failures; encourages automated negative tests

**Testing Playground**: All test-team agents run their tests under `/Users/hex/Documents/My_Projects/DVT/Testing_Playground`. **Do not remove things.** Each test run uses a **trial folder**: `trial_<what_we_are_testing>_<number>` (e.g. `trial_dvt_init_1`). **Each trial is a self-contained uv project**: the trial has its own uv (pyproject.toml), its own .venv, and dvt-core installed inside it; you `cd` into the trial and run `uv run dvt ...` there. **Findings** are written in that folder under `findings/`.

**Environment**: Test-team agents use **Python** and **uv**. Each trial must be a **uv-contained folder** with the tool installed inside the trial (e.g. `uv init`, `uv add .../dvt-core/core`, `uv sync`, then `uv run dvt ...` from the trial)—not a shared env or standalone scripts.

## Important Reminders

1. **Always work on the `dev` branch** - All development happens on `dev`
2. **Respect branch flow** - dev → uat → prod (via PRs)
3. **Preserve rebase compatibility** - Changes must be rebaseable onto dbt-core
4. **Reference DVT RULES** - The canonical rules are in `dvt_implementation_plan.md`
5. **Check feature dependencies** - Review `dvt-core-features/` before making changes

## Related Documentation

- [Running DVT](./RUNNING_DVT.md) - How to run the dvt CLI (venv, arch, test team)
- [Branching and Rebase Strategy](./BRANCHING_AND_REBASE.md) - Git workflow documentation
- `dvt_implementation_plan.md` - Canonical DVT RULES and implementation plan
- `dvt-core-features/` - Feature specifications
