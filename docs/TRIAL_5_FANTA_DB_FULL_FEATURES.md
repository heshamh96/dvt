# Trial 5: Fanta_DB – Full feature test (append-only profiles/computes)

**Trial folder**: `Testing_Playground/trial_5_fanta_db`  
**Project name**: **Fanta_DB**  
**Critical rule**: **DO NOT OVERRIDE** `profiles.yml` or `computes.yml`. New profile **Fanta_DB** must be **appended** (or merged); existing profiles and compute blocks must remain.

Use the **testing agents** (test-team-technical-qa, test-team-data-engineer, test-team-negative-tester) to run and verify. Each agent uses this doc and their own rules; findings go under `trial_5_fanta_db/findings/`.

---

## 1. Trial setup (Testing Playground)

- **Path**: `/Users/hex/Documents/My_Projects/DVT/Testing_Playground`
- **Trial folder**: `trial_5_fanta_db` (uv project: `pyproject.toml`, `.venv`, dvt-core installed via uv; see **Local dvt-core with uv** below).
- **Do not remove** existing trial folders. Do not overwrite `~/.dvt/profiles.yml` or `~/.dvt/computes.yml`; expect **append/merge** only.

### Local dvt-core with uv (non-editable)

**Option A – use the Trial 5 uv config template (recommended)**

1. Create the trial folder: `Testing_Playground/trial_5_fanta_db`.
2. Copy the template into it:
   ```bash
   cp /path/to/dvt-core/docs/trial_5_pyproject.toml Testing_Playground/trial_5_fanta_db/pyproject.toml
   ```
3. If your layout is not `.../Testing_Playground/trial_5_fanta_db` with `dvt-core` as a sibling of `Testing_Playground`, edit `pyproject.toml` and set `[tool.uv.sources].dvt-core.path` to the **absolute path** to `dvt-core/core` (e.g. `path = "/Users/hex/Documents/My_Projects/DVT/dvt-core/core"`).
4. From the **trial folder** (`trial_5_fanta_db`), not from `Fanta_DB/`: run `uv sync`. Then run all `dvt` commands from the trial folder too (e.g. `uv run dvt sync --project-dir Fanta_DB`), so the env that has local dvt-core is used.
5. **If `dvt sync --help` still shows dozens of flags**: the active env is not using local dvt-core. Ensure (a) the `pyproject.toml` with the path source is in the **trial root** (`trial_5_fanta_db`), (b) you ran `uv sync` from the trial root, and (c) you run `uv run dvt ...` from the trial root (or have activated the trial’s `.venv`). Do not run `uv sync` from `Fanta_DB/` if it has its own `pyproject.toml`—that env will not have the path source.
6. **If `uv sync` installs pyspark 4.1.0**: the template pins `pyspark==3.5.0` so it matches Fanta_DB’s computes. Re-copy the latest `docs/trial_5_pyproject.toml` so the trial has that pin, then `uv sync` again.

**Option B – add config by hand**

See **[docs/USING_LOCAL_DVT_WITH_UV.md](USING_LOCAL_DVT_WITH_UV.md)**. In the trial's `pyproject.toml`: `dependencies = ["dvt-core", "dbt-postgres"]` (or your deps) and `[tool.uv.sources]` with `dvt-core = { path = "/full/path/to/dvt-core/core", editable = false }`, then `uv sync`.

---

## 2. Expected behaviour (no override)

- **profiles.yml**: Before trial, note existing profiles (e.g. Coke_DB). After `dvt init Fanta_DB` (with profile setup), **Fanta_DB** must be **added** (or merged). Existing profiles must still be present and unchanged.
- **computes.yml**: Before trial, note existing top-level keys (e.g. `default`, `Coke_DB`). After init, **Fanta_DB** must appear as a **new** top-level key with `target` and `computes`. Existing keys must still be present and unchanged.

---

## 3. Flow to run (by agents)

### 3.1 One-time setup

1. `cd` into `trial_5_fanta_db`.
2. Ensure trial is a uv project with dvt-core: copy `docs/trial_5_pyproject.toml` to this folder as `pyproject.toml` (or add the path source per §1), fix the path if needed, then run `uv sync`.
3. (Optional) Back up or note current `~/.dvt/profiles.yml` and `~/.dvt/computes.yml` to verify append later.

### 3.2 Init (Fanta_DB)

4. Run: `uv run dvt init Fanta_DB` (do **not** use `--skip-profile-setup` so that profile and computes are set up).
5. Complete adapter/connection prompts (e.g. postgres) so a **Fanta_DB** profile is written.
6. **Verify**:
   - Project dir `Fanta_DB/` exists with `dbt_project.yml` (name, profile: Fanta_DB, paths), and standard dirs: `models/`, `tests/`, `macros/`, `seeds/`, `snapshots/`, `analyses/`.
   - **profiles.yml**: Contains **Fanta_DB** and still contains any pre-existing profiles (no override).
   - **computes.yml**: Contains a **Fanta_DB** block (target + computes) and still contains any pre-existing profile blocks (no override).

### 3.3 Sync (adapters + PySpark + JDBC)

7. **Environment for sync**: Sync uses only (a) a venv **inside** `Fanta_DB/` (`.venv`, `venv`, or `env`), or (b) `--python-env /path/to/venv`. From trial root you can run `uv run dvt sync --project-dir Fanta_DB --python-env $(pwd)/.venv` to use the trial’s venv, or create a `.venv` inside `Fanta_DB/` (e.g. `cd Fanta_DB && uv venv && uv sync` if Fanta_DB has its own pyproject.toml) and then `uv run dvt sync --project-dir Fanta_DB`.
8. **Verify**: Adapters installed; pyspark version from **Fanta_DB’s** block in `~/.dvt/computes.yml` (active target’s `version`); **`~/.dvt/.jdbc_jars/`** contains JARs for the adapter type(s) in Fanta_DB profile (e.g. postgres).
9. **Optional**: `uv run dvt sync --help` shows only `--project-dir`, `--profiles-dir`, `--python-env`, and `--help` (minimal flags when using local dvt-core build).

### 3.4 Debug (config, targets, computes, connection, full)

10. `uv run dvt debug --project-dir Fanta_DB --config`: project name, profile (Fanta_DB), DVT dir, computes path, MDM path.
11. `uv run dvt debug --project-dir Fanta_DB --targets`: lists **Fanta_DB** profile and its targets (and connection status if applicable).
12. `uv run dvt debug --project-dir Fanta_DB --computes`: shows compute(s) for the current project’s profile (Fanta_DB).
13. `uv run dvt debug --project-dir Fanta_DB --connection <target>`: tests the given target (success or clear failure).
14. `uv run dvt debug --project-dir Fanta_DB` (no section flags): full debug flow passes (version, profile, project, deps, connection).

### 3.5 Parse

15. `uv run dvt parse --project-dir Fanta_DB`: completes without missing project/config errors; target/manifest produced as expected.

---

## 4. Agent-specific checklists

### 4.1 test-team-technical-qa

- [ ] Paths: user-level `~/.dvt/` (or PROFILES_DIR); project-level `Fanta_DB/` with required dirs and `dbt_project.yml`.
- [ ] **No override**: `profiles.yml` and `computes.yml` still contain all pre-existing entries; **Fanta_DB** added as new profile and new computes block.
- [ ] Names: project name Fanta_DB valid; `dbt_project.yml` has `profile: Fanta_DB`; `profiles.yml` has key `Fanta_DB`; `computes.yml` has key `Fanta_DB` with `target` and `computes`.
- [ ] Idempotency: re-running init in same dir does not overwrite existing user-level files; existing project dir handled per spec (e.g. ProjectNameAlreadyExists or no overwrite).
- [ ] JDBC: after sync, `~/.dvt/.jdbc_jars/` exists and has JARs for Fanta_DB’s adapter type(s).

- [ ] **Sync help**: `uv run dvt sync --help` shows only `--project-dir`, `--profiles-dir`, `--python-env`, and `--help` (when trial uses local dvt-core per §1).

**Findings**: `trial_5_fanta_db/findings/technical_qa_checklist.md` (or findings.md).

### 4.2 test-team-data-engineer

- [ ] Init produces a dbt-compatible layout (model-paths, target-path, profile, etc.) and a postgres (or chosen adapter) profile shape compatible with dbt.
- [ ] Profile Fanta_DB in `profiles.yml` has correct structure (outputs, target, type, host, port, etc.); existing profiles unchanged.
- [ ] Sync installs correct `dbt-<adapter>` and JDBC JARs in `~/.dvt/.jdbc_jars/` for the adapter(s) in Fanta_DB.
- [ ] Parse and debug commands work against the Fanta_DB project and profile.

**Findings**: `trial_5_fanta_db/findings/data_engineer_findings.md`.

### 4.3 test-team-negative-tester

- [ ] Invalid project name (e.g. numeric-only, special chars): clear validation error or prompt; no overwrite of profiles/computes.
- [ ] `dvt init Fanta_DB` when Fanta_DB dir already exists: no overwrite of project; appropriate message (e.g. ProjectNameAlreadyExists).
- [ ] Profile already exists: if init would overwrite, user is prompted (per check_if_can_write_profile); no silent overwrite of other profiles.
- [ ] `dvt sync` / `dvt debug` / `dvt parse` with bad or missing project/profile: clear errors, no silent failure; no overwrite of `profiles.yml` or `computes.yml`.

**Findings**: `trial_5_fanta_db/findings/negative_findings.md`.

---

## 5. Implementation reference (no override)

- **profiles.yml**: `core/dvt/task/init.py` – `write_profile` reads existing file, merges `profiles[profile_name] = profile`, writes back (no full-file overwrite of other profiles). `create_profile_from_sample` appends when file exists.
- **computes.yml**: `core/dvt/config/user_config.py` – `create_default_computes_yml` only creates file if it does not exist; `append_profile_to_computes_yml(profile_name)` adds a new top-level key only if that profile is not already present (merge, no override of existing keys). Init calls `append_profile_to_computes_yml` after `setup_profile`.
- **JDBC**: Sync always writes to `~/.dvt/.jdbc_jars/` (canonical); see `docs/DVT_SYNC.md` and `docs/TRIAL_4_DVT_SYNC_CHECKLIST.md`.

---

## 6. Summary

- **Trial 5** = project **Fanta_DB**, full feature pass, **append-only** for `profiles.yml` and `computes.yml`.
- **Testing agents** use this doc and their rules; each records findings under `trial_5_fanta_db/findings/`.
- **Do not override** existing profiles or computes; verify Fanta_DB is **added** and all prior content remains.
