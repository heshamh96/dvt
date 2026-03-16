# DVT CLI Commands — Full Flag Matrix

All 19 DVT CLI commands with their flags, expected behavior, and test scenarios.

**Base invocation pattern**:
```bash
# Via uv in a trial:
uv run dvt <command> [flags] --project-dir <project> --profiles-dir <profiles>

# Via .venv directly:
.venv/bin/dvt <command> [flags] --project-dir <project> --profiles-dir <profiles>

# Common global flags (apply to most commands):
--project-dir DIRECTORY          # Project directory (contains dbt_project.yml)
--profiles-dir DIRECTORY         # Profiles directory (contains profiles.yml)
-d, --debug / --no-debug         # Debug logging
--log-format [text|debug|json|default]
--log-format-file [text|debug|json|default]
--log-level [debug|info|warn|error|none]
--log-level-file [debug|info|warn|error|none]
--log-path DIRECTORY
-q, --quiet / --no-quiet
--no-partial-parse / --partial-parse
--no-print / --print
--printer-width INTEGER
--target TEXT                    # Override target from profiles.yml
--target-path DIRECTORY
--threads INTEGER
--vars TEXT                      # YAML dict of variables
--version                        # Show version
```

## 1. `dvt init`

Creates a new DVT project with scaffolding.

| Flag | Type | Test Scenario |
|------|------|--------------|
| `PROJECT_NAME` | positional | `dvt init TestProject` |
| `--skip-profile-setup` | flag | Skip interactive profile prompt |
| (no args) | - | Should prompt for project name |

**Test cases**:
- `dvt init TestProject --skip-profile-setup` -> creates dir, dbt_project.yml, profiles stub
- `dvt init` (no name) -> prompts interactively (skip in automated test)
- `dvt init ExistingDir` -> should warn/fail if dir exists
- Exit code 0 on success

## 2. `dvt debug`

Displays configuration, connections, and federation readiness.

| Flag | Type | Test Scenario |
|------|------|--------------|
| `--config` | flag | Show only config section |
| `--targets` | flag | Show only targets section |
| `--manifest` | flag | Show manifest summary |
| `--connection` | flag | Test connection for default target |

**Test cases**:
- `dvt debug` -> all sections displayed, all targets show OK
- `dvt debug --config` -> only config section
- `dvt debug --targets` -> only targets section
- `dvt debug --connection` -> connection test for default target
- With invalid profiles dir -> error
- Exit code 0 on success

## 3. `dvt clean`

Removes target/ and dvt_packages/ directories.

| Flag | Type | Test Scenario |
|------|------|--------------|
| `--clean-targets` | list | Custom dirs to clean (default: target, dvt_packages) |

**Test cases**:
- `dvt clean` -> removes target/ and dvt_packages/
- Run twice -> second run is no-op (idempotent)
- Exit code 0 on success

## 4. `dvt deps`

Installs packages from packages.yml.

| Flag | Type | Test Scenario |
|------|------|--------------|
| (none specific) | - | Just global flags |

**Test cases**:
- `dvt deps` -> installs packages from packages.yml
- `dvt deps` with no packages.yml -> should handle gracefully
- `dvt deps` after `dvt clean` -> re-installs
- Exit code 0 on success

## 5. `dvt sync`

Syncs DVT development environment (adapters, Sling connections, DuckDB extensions).

| Flag | Type | Test Scenario |
|------|------|--------------|
| `--python-env PATH` | option | Explicit Python env path |

**Test cases**:
- `dvt sync --python-env .venv` -> installs to specified venv
- `dvt sync` (auto-detect) -> finds .venv/venv/env
- With no adapter types in profiles -> prints warning
- Summary printed at end
- Exit code 0 on success

## 6. `dvt compile`

Compiles SQL without executing.

| Flag | Type | Test Scenario |
|------|------|--------------|
| `-s, --select TEXT` | multi | Select specific models |
| `--exclude TEXT` | multi | Exclude models |
| `--selector TEXT` | option | YAML selector |
| `--inline TEXT` | option | Inline SQL to compile |
| `--output [json|text]` | option | Output format |
| `--compile-inline TEXT` | option | Alias for --inline |

**Test cases**:
- `dvt compile` -> compiles all models, creates target/compiled/
- `dvt compile -s model_name` -> compiles specific model
- `dvt compile --inline "SELECT 1"` -> compiles inline SQL
- `dvt compile --output json` -> JSON output
- Exit code 0 on success

## 7. `dvt show`

Previews query results without materializing.

| Flag | Type | Test Scenario |
|------|------|--------------|
| `-s, --select TEXT` | multi | Select model to preview |
| `--inline TEXT` | option | Inline SQL to preview |
| `--limit INTEGER` | option | Row limit (default 5) |
| `--output [json|text]` | option | Output format |

**Test cases**:
- `dvt show -s model_name --limit 5` -> shows 5 rows
- `dvt show --inline "SELECT 1 as x"` -> inline preview
- `dvt show --output json` -> JSON output
- Exit code 0 on success

## 8. `dvt run`

Compiles and executes models.

| Flag | Type | Test Scenario |
|------|------|--------------|
| `-s, --select TEXT` | multi | Select models |
| `--exclude TEXT` | multi | Exclude models |
| `--selector TEXT` | option | YAML selector |
| `--full-refresh` | flag | DROP+CREATE+INSERT instead of TRUNCATE+INSERT |
| `-x, --fail-fast` | flag | Stop on first failure |
| `--target TEXT` | option | Override target |
| `--threads INTEGER` | option | Parallelism |
| `--defer / --no-defer` | flag | Defer to --state manifest |
| `--state DIRECTORY` | option | State directory for deferral |
| `--favor-state` | flag | Favor state over database |

**Test cases**:
- `dvt run` -> TRUNCATE+INSERT all models
- `dvt run --full-refresh` -> DROP+CREATE+INSERT all models
- `dvt run -s model_name` -> run specific model
- `dvt run --exclude model_name` -> skip specific model
- `dvt run --target dbx_dev` -> override target (triggers Sling+DuckDB federation if needed)
- `dvt run -x` -> fail-fast on error
- `dvt run --threads 1` -> single-threaded
- Exit code 0 on full success, 1 on partial failure, 2 on error

## 9. `dvt seed`

Loads CSV/JSON/Parquet seed files into target database.

| Flag | Type | Test Scenario |
|------|------|--------------|
| `-s, --select TEXT` | multi | Select specific seeds |
| `--exclude TEXT` | multi | Exclude seeds |
| `--full-refresh` | flag | DROP+CREATE instead of TRUNCATE+INSERT |
| `--target TEXT` | option | Override target |
| `-x, --fail-fast` | flag | Stop on first failure |
| `--show` | flag | Show seed data preview |

**Test cases**:
- `dvt seed` -> loads all seeds to default target
- `dvt seed -s seed_name` -> specific seed
- `dvt seed --target dbx_dev` -> seed to Databricks
- `dvt seed --full-refresh` -> drop and recreate
- `dvt seed --show` -> preview without loading
- Exit code 0 on success

## 10. `dvt test`

Runs data tests defined in YAML.

| Flag | Type | Test Scenario |
|------|------|--------------|
| `-s, --select TEXT` | multi | Select tests |
| `--exclude TEXT` | multi | Exclude tests |
| `--indirect-selection [eager|cautious|buildable|empty]` | option | Test adjacency selection |
| `-x, --fail-fast` | flag | Stop on first failure |

**Test cases**:
- `dvt test` -> runs all data tests
- `dvt test -s test_name` -> specific test
- Exit code 0 on all pass, 1 on some fail

## 11. `dvt build`

Runs seeds, models, snapshots, and tests in dependency order.

| Flag | Type | Test Scenario |
|------|------|--------------|
| `-s, --select TEXT` | multi | Select nodes |
| `--exclude TEXT` | multi | Exclude nodes |
| `--full-refresh` | flag | Full refresh all materializations |
| `-x, --fail-fast` | flag | Stop on first failure |
| `--resource-types` | multi | Filter by type (model, seed, test, snapshot) |

**Test cases**:
- `dvt build` -> full build pipeline
- `dvt build -s +model_name` -> build with upstream deps
- `dvt build --resource-types model seed` -> only models and seeds
- Exit code 0 on success

## 12. `dvt list`

Lists project resources.

| Flag | Type | Test Scenario |
|------|------|--------------|
| `-s, --select TEXT` | multi | Select resources |
| `--exclude TEXT` | multi | Exclude resources |
| `--resource-types` | multi | Filter by type |
| `--output [json|text|path|name|selector]` | option | Output format |
| `--output-keys TEXT` | multi | Keys for JSON output |

**Test cases**:
- `dvt list` -> lists all resources
- `dvt list --resource-types model` -> only models
- `dvt list --output json` -> JSON format
- `dvt list -s model_name` -> specific model
- Exit code 0 on success

## 13. `dvt parse`

Parses project without compilation.

| Flag | Type | Test Scenario |
|------|------|--------------|
| (none specific) | - | Just global flags |

**Test cases**:
- `dvt parse` -> parses project, writes manifest
- Exit code 0 on success, non-zero on parse error

## 14. `dvt retry`

Retries previously failed nodes.

| Flag | Type | Test Scenario |
|------|------|--------------|
| (none specific) | - | Just global flags |

**Test cases**:
- `dvt retry` after failed run -> retries only failed nodes
- `dvt retry` with no previous run -> error
- Exit code 0 on success

## 15. `dvt clone`

Clones database objects.

| Flag | Type | Test Scenario |
|------|------|--------------|
| `-s, --select TEXT` | multi | Select nodes |
| `--exclude TEXT` | multi | Exclude nodes |
| `--state DIRECTORY` | option | State directory |
| `--full-refresh` | flag | Force full clone |

**Test cases**:
- `dvt clone --state path -s model_name` -> clones from state
- Exit code 0 on success

## 16. `dvt run-operation`

Runs a macro as an operation.

| Flag | Type | Test Scenario |
|------|------|--------------|
| `MACRO` | positional | Macro name to run |
| `--args TEXT` | option | YAML dict of arguments |

**Test cases**:
- `dvt run-operation macro_name` -> runs macro
- `dvt run-operation macro_name --args '{key: value}'` -> with args
- Exit code 0 on success

## 17. `dvt snapshot`

Runs snapshot nodes.

| Flag | Type | Test Scenario |
|------|------|--------------|
| `-s, --select TEXT` | multi | Select snapshots |
| `--exclude TEXT` | multi | Exclude snapshots |
| `-x, --fail-fast` | flag | Stop on first failure |

**Test cases**:
- `dvt snapshot` -> runs all snapshots
- `dvt snapshot -s snapshot_name` -> specific snapshot
- Exit code 0 on success

## 18. `dvt docs generate`

Generates documentation catalog.

| Flag | Type | Test Scenario |
|------|------|--------------|
| `--no-compile` | flag | Skip compilation |
| `--empty-catalog` | flag | Generate empty catalog |

**Test cases**:
- `dvt docs generate` -> creates target/catalog.json
- `dvt docs generate --no-compile` -> uses existing manifest
- Exit code 0 on success

## 19. `dvt source freshness`

Checks source freshness.

| Flag | Type | Test Scenario |
|------|------|--------------|
| `-s, --select TEXT` | multi | Select sources |
| `--exclude TEXT` | multi | Exclude sources |
| `-o, --output TEXT` | option | Output path for results |

**Test cases**:
- `dvt source freshness` -> checks all sources
- `dvt source freshness -s source_name` -> specific source
- `dvt source freshness -o results.json` -> output to file
- Exit code 0 on all fresh, non-zero on stale

## Negative Test Cases (Apply to All Commands)

Every command should also be tested with:

| Scenario | Expected |
|----------|----------|
| `--project-dir /nonexistent` | Error: project not found |
| `--profiles-dir /nonexistent` | Error: profiles not found |
| `--target nonexistent_target` | Error: target not found in profiles |
| Invalid YAML in dbt_project.yml | Parse error |
| `--help` flag | Exit 0, usage text printed |
| `--version` flag | Exit 0, version printed |
