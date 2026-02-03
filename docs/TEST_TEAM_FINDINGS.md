# Test Team Findings

Summary of what the test team has verified: dvt init, dvt debug (Feature 02), and what remains blocked or unverified.

---

## Feature 02: dvt debug (tested 2026-02-03)

**Trial**: `Testing_Playground/trial_dvt_debug_1` (findings in that folder). Commands run from trial_dvt_init_2/Coke_DB (project with Coke_DB profile, dev target).

### Verified

- `dvt debug --config`: Shows DVT directory, profiles.yml, computes.yml, MDM path, project path; project name and profile from dvt_project.yml.
- `dvt debug --targets`: Lists **targets for the current project's profile only** (not all profiles); shows each target with type, host/schema, and connection status (✓ Connected / ✗).
- `dvt debug --computes`: Lists compute engines from computes.yml; PySpark availability.
- `dvt debug --manifest`: Shows target/manifest.json existence, model/test/seed/source counts, generated_at.
- `dvt debug --connection dev`: Tests the specified target (dev) for the current project's profile; prints connection info and success/failure.
- `dvt debug` (no flags): Original full flow (version, profile, project, deps, connection) — All checks passed.

### Spec alignment

- Targets are shown for the **current project profile only** per updated Feature 02 spec (dvt-core-features/02-dvt-debug/FEATURE.md).

---

## 0. Dev-team fix (dvt init CLI)

**Issue**: After `uv sync` in trial_dvt_init_2, `uv run dvt init Coke_DB --skip-profile-setup` failed with:

`ModuleNotFoundError: No module named 'dvt.include.global_project'`

**Fix**: Added missing package in dvt-core:

- **`core/dvt/include/global_project/__init__.py`** with `PROJECT_NAME = "dvt"`.

Used by `task/init.py` (reserved project name), `context/macro_resolver.py`, and `context/macros.py` for the internal macro namespace. Rebrand left references to this module but the package was not created.

**Verification**: From `Testing_Playground/trial_dvt_init_2`: `uv sync --reinstall-package dvt-core` then `uv run dvt init Coke_DB --skip-profile-setup` — project `Coke_DB/` created successfully.

---

## 1. Data Engineer (test-team-data-engineer)

### Verified

- **Reference project**: Cocacola_DWH_on_DBT is available at `/Users/hex/Documents/My_Projects/DVT/Cocacola_DWH_on_DBT` with postgres profile in `Connections/profiles.yml` (dbname: coke_db, type: postgres). Structure is suitable as reference for Coke_DB and dbt-aligned layout.
- **Starter project layout**: `core/dvt/include/starter_project/` contains `dvt_project.yml` (with `{project_name}` / `{profile_name}` placeholders), and standard dirs: `models/`, `tests/`, `macros/`, `seeds/`, `snapshots/`, `analyses/` (with `.gitkeep` where needed). Matches dbt mindset (model-paths, target-path, clean-targets, etc.).
- **User config creation**: When init logic runs (via standalone script in this env), `~/.dvt/` is created with:
  - `computes.yml` with `computes.default` (type: spark, master: local[*], config).
  - `data/mdm.duckdb` (DuckDB stub with `_dvt_init` table when duckdb is available).
  So the user-level layout and content from `user_config.py` behave as specified.

### Now verified (after dev-team fix)

- **Full `dvt init Coke_DB` via installed package**: Run successfully from trial_dvt_init_2 after adding `dvt.include.global_project` and reinstalling dvt-core; project `Coke_DB/` created.

### Not verified (blocked by environment)

- **Full `dvt init Coke_DB` via installed package (legacy note)**: Previously not run successfully. Requires `uv sync` in `core/` to complete and `dvt` to be run from that env; in this environment `uv sync` did not complete within the attempted window, and running the CLI via system-installed dvt hit an architecture mismatch (rpds: x86_64 vs arm64). So end-to-end “create Coke_DB from installed dvt CLI” is **not** verified.
- **Profile creation**: With `--skip-profile-setup`, `profiles.yml` is not created by init. Creation of a postgres profile (e.g. Coke_DB) in `~/.dvt/profiles.yml` and alignment with reference `Connections/profiles.yml` **not** verified via CLI.

---

## 2. Technical QA (test-team-technical-qa)

### Verified

- **Paths (code and one run)**:
  - User-level: `~/.dvt/`, `~/.dvt/computes.yml`, `~/.dvt/data/`, `~/.dvt/data/mdm.duckdb` created by init user-config logic. `profiles.yml` is only created when profile setup runs (not with `--skip-profile-setup`).
  - Project-level: Starter has `dvt_project.yml` and dirs; copy + placeholder replacement produces correct project name and profile name in YAML.
- **computes.yml schema**: Content has top-level `computes`, then `default` with `type`, `master`, `config` (spark.driver.memory, spark.sql.adaptive.enabled). Matches `user_config.create_default_computes_yml`.
- **Project name in dvt_project.yml**: Placeholders `{project_name}` and `{profile_name}` are replaced; project name follows identifier rules (letters/numbers/underscore, not starting with digit) per contracts.
- **Idempotency (code)**: `create_default_computes_yml` and `init_mdm_db` return early if file exists; no overwrite. `create_dvt_user_config` does not overwrite existing computes or mdm.duckdb.

### Not verified

- **Full checklist on a CLI-created project**: Because `dvt init Coke_DB` was not run successfully from the installed package, the full technical-qa checklist (all items in `test-team-technical-qa.mdc`) was not executed against a real CLI output.
- **profiles.yml creation**: Not verified (skipped when using `--skip-profile-setup`).
- **Idempotency of full init**: Re-running init in same dir / with existing project dir not tested via CLI (only code paths reviewed).

---

## 3. Negative Tester (test-team-negative-tester)

### Verified (code review)

- **Project name validation**: `InitTask.get_valid_project_name()` uses `ProjectName` (i.e. `Identifier`) with regex `^[^\d\W]\w*$`: rejects empty, numeric-only, special characters (e.g. `my-project`, `my.project`). Rejects reserved names (internal package names, global project name). Loops with click.prompt until valid; no silent accept.
- **Existing project dir**: If `project_path.exists()`, code fires `ProjectNameAlreadyExists` and returns without creating or overwriting.
- **CLI `--profile`**: If run inside an existing project with `--profile`, raises `DbtRuntimeError`: "Can not init existing project with specified profile, edit dvt_project.yml instead". If `--profile <name>` and profile not in profiles: raises "Could not find profile named '...'".
- **Overwrite profile**: When profile already exists in profiles.yml, `check_if_can_write_profile()` prompts for overwrite; no silent overwrite.

### Not verified (no CLI runs)

- **Actual error messages and exit behaviour**: Invalid names (e.g. `dvt init 123`, `dvt init my-project`), existing dir (`dvt init Coke_DB` when Coke_DB exists), and invalid `--profile` were **not** run against the installed `dvt` CLI. So wording, exit codes, and user experience of errors are **not** verified.
- **Permission / bad path**: Non-writable dir, read-only `~/.dvt`, invalid PROFILES_DIR not tested.
- **Corrupt YAML**: Behaviour with corrupted `profiles.yml` not tested.

---

## 4. Environment / Blockers

- **uv sync**: In the environment used, `uv sync` in `core/` did not complete within the attempted time (dependency resolution). The test team workflow depends on it completing so `dvt` is available via `uv run` or venv.
- **Architecture**: Running the CLI with a system-installed dvt (pip install -e .) hit `rpds` (used by jsonschema) built for x86_64 on an arm64 process. So any path that imports dbt_common/jsonschema can fail until Python and deps match the CPU arch.
- **Adapters shim**: `core/dvt/adapters/__init__.py` exposes `dbt.adapters` as `dvt.adapters`. Code review and import tests indicate the CLI can load with this in place; no full CLI run was completed to confirm.

---

## 5. Summary Table

| Area                    | Verified | Not verified / blocked |
|-------------------------|----------|-------------------------|
| User config (~/.dvt/)   | Yes (logic + one run) | Full CLI path |
| Project layout (starter + copy) | Yes (code + one run) | Full CLI path |
| computes.yml, mdm.duckdb | Yes | — |
| profiles.yml            | No | Creation via CLI |
| Project name validation | Code only | CLI behaviour and messages |
| Existing-dir / --profile guards | Code only | CLI behaviour and messages |
| Idempotency             | Code only | Full init re-run via CLI |
| Negative scenarios (errors) | No | All (no CLI runs) |

---

## 6. Recommendations

1. **Run full flow once env is ready**: After `uv sync` completes in `core/` and Python/deps match CPU arch, run from Testing_Playground:  
   `uv run --project .../dvt-core/core dvt init Coke_DB --skip-profile-setup`  
   Then run data-engineer and technical-qa checklists on the result.

2. **Run negative cases**: Execute the negative-tester scenarios (invalid name, existing dir, invalid `--profile`) and record exit codes and message text.

3. **Optional automation**: Add pytest tests (e.g. in `tests/functional/init/` or `tests/unit/`) that invoke the init task or CLI with valid/invalid names and existing-dir to lock in behaviour and prevent regressions.

4. **profiles.yml**: Run init without `--skip-profile-setup` (or with an existing profile) and verify `~/.dvt/profiles.yml` is created and matches reference shape for postgres when that path is used.
