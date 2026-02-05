# Trial 10: Coke_DB – Full feature test (dvt-core from PyPI)

**Trial folder**: `Testing_Playground/trial_10_coke_db`  
**Project name**: **Coke_DB**  
**Critical rule**: **DO NOT OVERRIDE** `profiles.yml` or `computes.yml`. New profile **Coke_DB** must be **appended** (or merged); existing profiles and compute blocks must remain.

**Difference from Trial 6**: dvt-core is installed **from PyPI** (published package), not from a local path. This validates the released package with the same full-feature flow.

Use the **testing agents** (test-team-technical-qa, test-team-data-engineer, test-team-negative-tester) to run and verify. Each agent uses this doc and their own rules; findings go under `trial_10_coke_db/findings/`.

---

## 1. Trial setup (Testing Playground)

- **Path**: `/Users/hex/Documents/My_Projects/DVT/Testing_Playground`
- **Trial folder**: `trial_10_coke_db` (uv project: `pyproject.toml`, `.venv`, **dvt-core from PyPI**).
- **Do not remove** existing trial folders. Do not overwrite `~/.dvt/profiles.yml` or `~/.dvt/computes.yml`; expect **append/merge** only.

### dvt-core from PyPI

1. Create the trial folder: `Testing_Playground/trial_10_coke_db`.
2. Copy the template into it:
   ```bash
   cp /path/to/dvt-core/docs/trial_10_pyproject.toml Testing_Playground/trial_10_coke_db/pyproject.toml
   ```
3. From the **trial folder** (`trial_10_coke_db`), run `uv sync`. The template sets `[tool.uv] index-url = "https://pypi.org/simple/"` so uv resolves dvt-core from PyPI without extra flags. dvt-core is installed from PyPI (no path or editable source). If uv reports "no version" right after a new dvt-core release, run `uv sync --refresh` once to refresh the index cache.
4. Run all `dvt` commands from the trial folder (e.g. `uv run dvt sync --project-dir Coke_DB`) so the env that has dvt-core from PyPI is used.
5. **If `uv sync` installs pyspark 4.x**: the template pins `pyspark==3.5.0` so it matches Coke_DB's computes. Re-copy the latest `docs/trial_10_pyproject.toml` so the trial has that pin, then `uv sync` again.

---

## 2. Expected behaviour (no override)

- **profiles.yml**: Before trial, note existing profiles (e.g. Fanta_DB, Pepsi_DB). After `dvt init Coke_DB` (with profile setup), **Coke_DB** must be **added** (or merged). Existing profiles must still be present and unchanged.
- **computes.yml**: Before trial, note existing top-level keys. After init, **Coke_DB** must appear as a **new** top-level key with `target` and `computes`. Existing keys must still be present and unchanged.

---

## 3. Flow to run (by agents)

### 3.1 One-time setup

1. `cd` into `trial_10_coke_db`.
2. Ensure trial is a uv project: copy `docs/trial_10_pyproject.toml` to this folder as `pyproject.toml`, then run `uv sync`.
3. (Optional) Back up or note current `~/.dvt/profiles.yml` and `~/.dvt/computes.yml` to verify append later.

### 3.2 Init (Coke_DB)

4. Run: `uv run dvt init Coke_DB` (do **not** use `--skip-profile-setup` so that profile and computes are set up).
5. Complete adapter/connection prompts (e.g. postgres) so a **Coke_DB** profile is written.
6. **Verify**:
   - Project dir `Coke_DB/` exists with `dbt_project.yml` (name, profile: Coke_DB, paths), and standard dirs: `models/`, `tests/`, `macros/`, `seeds/`, `snapshots/`, `analyses/`.
   - **profiles.yml**: Contains **Coke_DB** and still contains any pre-existing profiles (no override).
   - **computes.yml**: Contains a **Coke_DB** block (target + computes) and still contains any pre-existing profile blocks (no override).

### 3.3 Sync (adapters + PySpark + JDBC)

7. **Environment for sync**: Sync uses only (a) a venv **inside** the project dir (e.g. `Coke_DB/.venv`), or (b) `--python-env "/path/to/venv"` (use quotes). **Recommended**: from the **trial root** (`trial_10_coke_db`), run `uv run dvt sync --project-dir Coke_DB --python-env "$(pwd)/.venv"`. If you run from inside `Coke_DB/`, the trial's `.venv` is in the parent directory, so you **must** pass `--python-env` with the absolute path (e.g. `--python-env "/path/to/trial_10_coke_db/.venv"`); otherwise sync will prompt you interactively for the path, or in non-interactive mode fail with a message to stderr. Always use `uv run dvt ...` from the trial root.
8. **Verify**: Adapters installed; pyspark version from **Coke_DB's** block in `~/.dvt/computes.yml` (active target's `version`); **`~/.dvt/.jdbc_jars/`** contains JARs for the adapter type(s) in Coke_DB profile (e.g. postgres).
9. **Optional**: `uv run dvt sync --help` shows expected flags (same as released dvt-core on PyPI).

### 3.4 Debug (config, targets, computes, connection, full)

10. `uv run dvt debug --project-dir Coke_DB --config`: project name, profile (Coke_DB), DVT dir, computes path, MDM path.
11. `uv run dvt debug --project-dir Coke_DB --targets`: lists **Coke_DB** profile and its targets (and connection status if applicable).
12. `uv run dvt debug --project-dir Coke_DB --computes`: shows compute(s) for the current project's profile (Coke_DB).
13. `uv run dvt debug --project-dir Coke_DB --connection <target>`: tests the given target (success or clear failure).
14. `uv run dvt debug --project-dir Coke_DB` (no section flags): full debug flow passes (version, profile, project, deps, connection).

### 3.5 Parse

15. `uv run dvt parse --project-dir Coke_DB`: completes without missing project/config errors; target/manifest produced as expected.

---

## 4. Agent-specific checklists

### 4.1 test-team-technical-qa

- [ ] Paths: user-level `~/.dvt/` (or PROFILES_DIR); project-level `Coke_DB/` with required dirs and `dbt_project.yml`.
- [ ] **No override**: `profiles.yml` and `computes.yml` still contain all pre-existing entries; **Coke_DB** added as new profile and new computes block.
- [ ] Names: project name Coke_DB valid; `dbt_project.yml` has `profile: Coke_DB`; `profiles.yml` has key `Coke_DB`; `computes.yml` has key `Coke_DB` with `target` and `computes`.
- [ ] Idempotency: re-running init in same dir does not overwrite existing user-level files; existing project dir handled per spec (e.g. ProjectNameAlreadyExists or no overwrite).
- [ ] JDBC: after sync, `~/.dvt/.jdbc_jars/` exists and has JARs for Coke_DB's adapter type(s).
- [ ] **PyPI**: dvt-core is from PyPI (trial uses `trial_10_pyproject.toml` with no path source for dvt-core); `uv run dvt --version` (or similar) reflects released version.

**Findings**: `trial_10_coke_db/findings/technical_qa_checklist.md` (or findings.md).

### 4.2 test-team-data-engineer

- [ ] Init produces a dbt-compatible layout (model-paths, target-path, profile, etc.) and a postgres (or chosen adapter) profile shape compatible with dbt.
- [ ] Profile Coke_DB in `profiles.yml` has correct structure (outputs, target, type, host, port, etc.); existing profiles unchanged.
- [ ] Sync installs correct `dbt-<adapter>` and JDBC JARs in `~/.dvt/.jdbc_jars/` for the adapter(s) in Coke_DB.
- [ ] Parse and debug commands work against the Coke_DB project and profile.

**Findings**: `trial_10_coke_db/findings/data_engineer_findings.md`.

### 4.3 test-team-negative-tester

- [ ] Invalid project name (e.g. numeric-only, special chars): clear validation error or prompt; no overwrite of profiles/computes.
- [ ] `dvt init Coke_DB` when Coke_DB dir already exists: no overwrite of project; appropriate message (e.g. ProjectNameAlreadyExists).
- [ ] Profile already exists: if init would overwrite, user is prompted (per check_if_can_write_profile); no silent overwrite of other profiles.
- [ ] `dvt sync` / `dvt debug` / `dvt parse` with bad or missing project/profile: clear errors, no silent failure; no overwrite of `profiles.yml` or `computes.yml`.

**Findings**: `trial_10_coke_db/findings/negative_findings.md`.

---

## 5. Implementation reference (no override)

- **profiles.yml**: `core/dvt/task/init.py` – `write_profile` reads existing file, merges `profiles[profile_name] = profile`, writes back (no full-file overwrite of other profiles). `create_profile_from_sample` appends when file exists.
- **computes.yml**: `core/dvt/config/user_config.py` – `create_default_computes_yml` only creates file if it does not exist; `append_profile_to_computes_yml(profile_name)` adds a new top-level key only if that profile is not already present (merge, no override of existing keys). Init calls `append_profile_to_computes_yml` after `setup_profile`.
- **JDBC**: Sync always writes to `~/.dvt/.jdbc_jars/` (canonical); see `docs/DVT_SYNC.md` and `docs/TRIAL_4_DVT_SYNC_CHECKLIST.md`.

---

## 6. Summary

- **Trial 10** = project **Coke_DB**, full feature pass, **append-only** for `profiles.yml` and `computes.yml`, **dvt-core installed from PyPI**.
- **Sync** behaviour: same as other trials (env from in-project `.venv`/`venv`/`env` or `--python-env`, pyspark from profile's block in `~/.dvt/computes.yml`, JDBC to `~/.dvt/.jdbc_jars/`).
- **Testing agents** use this doc and their rules; each records findings under `trial_10_coke_db/findings/`.
- **Do not override** existing profiles or computes; verify Coke_DB is **added** and all prior content remains.
