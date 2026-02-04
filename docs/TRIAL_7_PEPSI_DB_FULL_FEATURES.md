# Trial 7: Pepsi_DB – Full feature test (sync working, minimal help, editable dvt-core)

**Trial folder**: `Testing_Playground/trial_7_pepsi_db`  
**Project name**: **Pepsi_DB**  
**Critical rule**: **DO NOT OVERRIDE** `profiles.yml` or `computes.yml`. New profile **Pepsi_DB** must be **appended** (or merged); existing profiles and compute blocks must remain.

This trial validates that **dvt sync** works correctly with: interactive mode, quoted `--python-env`, in-project env lookup first, and minimal `dvt sync --help`. Use **editable** dvt-core so the trial always uses the latest code.

---

## 1. Trial setup (Testing Playground)

- **Path**: `Testing_Playground` (e.g. `/Users/hex/Documents/My_Projects/DVT/Testing_Playground`).
- **Trial folder**: `trial_7_pepsi_db` (uv project with **editable** dvt-core path dependency).
- **Do not remove** existing trial folders. Do not overwrite `~/.dvt/profiles.yml` or `~/.dvt/computes.yml`; expect **append/merge** only.

### Local dvt-core with uv (editable – recommended for Trial 7)

Using **editable** mode ensures the trial always runs the latest dvt-core (sync fixes, minimal help, etc.) without running `uv sync` after every code change.

1. Create the trial folder: `Testing_Playground/trial_7_pepsi_db`.
2. Copy the template:  
   `cp /path/to/dvt-core/docs/trial_7_pyproject.toml Testing_Playground/trial_7_pepsi_db/pyproject.toml`
3. Edit `pyproject.toml` and set the **absolute path** to `dvt-core/core` in `[tool.uv.sources].dvt-core`. Use **`editable = true`**:
   ```toml
   [tool.uv.sources]
   dvt-core = { path = "/Users/hex/Documents/My_Projects/DVT/dvt-core/core", editable = true }
   ```
4. From the **trial root** (`trial_7_pepsi_db`): `uv sync`. Then run all commands from the trial root: `uv run dvt ...`.

---

## 2. Expected behaviour (no override)

- **profiles.yml**: After `dvt init Pepsi_DB`, **Pepsi_DB** is **added**; existing profiles remain.
- **computes.yml**: After init, **Pepsi_DB** appears as a new top-level key; existing keys remain.

---

## 3. Flow to run

### 3.1 One-time setup

1. `cd` into `trial_7_pepsi_db`.
2. Copy `docs/trial_7_pyproject.toml` to `pyproject.toml`, set the correct path to dvt-core, keep **editable = true**, then run `uv sync`.
3. (Optional) Back up or note current `~/.dvt/profiles.yml` and `~/.dvt/computes.yml`.

### 3.2 Init (Pepsi_DB)

4. Run: `uv run dvt init Pepsi_DB` (do **not** use `--skip-profile-setup` if you want profile/computes written).
5. Complete adapter prompts so a **Pepsi_DB** profile is written.
6. **Verify**: Project dir `Pepsi_DB/` exists with `dbt_project.yml` (profile: Pepsi_DB). `profiles.yml` and `computes.yml` contain Pepsi_DB and still contain existing entries.

### 3.3 Sync (must work: interactive, quoted path, in-project first, minimal help)

7. **Minimal help**  
   - `uv run dvt sync --help` must show **only**: `--project-dir`, `--profiles-dir`, `--python-env`, `--help`.

8. **Env lookup order**  
   - Sync looks for a venv **inside the project dir** first (`.venv`, `venv`, or `env` under `Pepsi_DB/`).
   - If none is found, it uses `--python-env` if provided, or **prompts interactively** for the path.

9. **Quoted path for `--python-env`**  
   - From the trial root (venv is at `trial_7_pepsi_db/.venv`), run:
     ```bash
     uv run dvt sync --project-dir Pepsi_DB --python-env "$(pwd)/.venv"
     ```
   - Use **quotes** so paths with spaces work. Example with literal path:  
     `uv run dvt sync --project-dir Pepsi_DB --python-env "/path/to/trial_7_pepsi_db/.venv"`

10. **Interactive mode**  
    - From the project dir (`Pepsi_DB/`) **without** `--python-env`: run `uv run dvt sync` (no args). If there is no `.venv` inside `Pepsi_DB/`, sync must **prompt** for the absolute path to the Python env. You can paste the trial’s `.venv` path (e.g. `/path/to/trial_7_pepsi_db/.venv`).

11. **Visible output**  
    - Sync must print progress to stderr (e.g. "dvt sync: starting...", "Using environment: ...", "Installing ...", "Sync complete."). No silent exit.

12. **Verify after sync**  
    - Adapters and pyspark installed in the chosen env; `~/.dvt/.jdbc_jars/` contains JARs for the profile’s adapter type(s); pyspark version matches the profile’s block in `~/.dvt/computes.yml`.

### 3.4 Debug

13. `uv run dvt debug --project-dir Pepsi_DB --config`  
14. `uv run dvt debug --project-dir Pepsi_DB --computes`  
15. `uv run dvt debug --project-dir Pepsi_DB` (full debug)

### 3.5 Parse

16. `uv run dvt parse --project-dir Pepsi_DB`: completes without project/config errors.

---

## 4. Checklist summary

- [ ] **Minimal help**: `uv run dvt sync --help` shows only `--project-dir`, `--profiles-dir`, `--python-env`, `--help`.
- [ ] **Quoted path**: `uv run dvt sync --project-dir Pepsi_DB --python-env "$(pwd)/.venv"` works from trial root.
- [ ] **In-project first**: Sync looks for `.venv`/`venv`/`env` inside the project dir before prompting or using `--python-env`.
- [ ] **Interactive**: Running `uv run dvt sync` from project dir with no in-project venv prompts for env path.
- [ ] **Visible output**: Sync prints progress to stderr; no silent exit with code 2.
- [ ] **Append-only**: Init adds Pepsi_DB to profiles/computes without removing existing entries.
- [ ] **Debug and parse** run successfully for Pepsi_DB.

---

## 5. Agent-specific checklists

Use the **testing agents** (test-team-technical-qa, test-team-data-engineer, test-team-negative-tester) to run and verify. Each agent uses this doc and their own rules; findings go under `trial_7_pepsi_db/findings/`.

### 5.1 test-team-technical-qa

- [ ] Paths: user-level `~/.dvt/` (or PROFILES_DIR); project-level `Pepsi_DB/` with required dirs and `dbt_project.yml`.
- [ ] **No override**: `profiles.yml` and `computes.yml` still contain all pre-existing entries; **Pepsi_DB** added as new profile and new computes block.
- [ ] Names: project name Pepsi_DB valid; `dbt_project.yml` has `profile: Pepsi_DB`; `profiles.yml` has key `Pepsi_DB`; `computes.yml` has key `Pepsi_DB` with `target` and `computes`.
- [ ] Idempotency: re-running init in same dir does not overwrite existing user-level files; existing project dir handled per spec.
- [ ] JDBC: after sync, `~/.dvt/.jdbc_jars/` exists and has JARs for Pepsi_DB's adapter type(s).
- [ ] **Sync help**: `uv run dvt sync --help` shows only `--project-dir`, `--profiles-dir`, `--python-env`, and `--help`.
- [ ] **Sync visible output**: Sync prints progress to stderr (e.g. "dvt sync: starting...", "Sync complete."); no silent exit with code 2.
- [ ] **Quoted path**: `uv run dvt sync --project-dir Pepsi_DB --python-env "$(pwd)/.venv"` works from trial root.

**Findings**: `trial_7_pepsi_db/findings/technical_qa_checklist.md`.

### 5.2 test-team-data-engineer

- [ ] Init produces a dbt-compatible layout (model-paths, target-path, profile, etc.) and adapter profile shape compatible with dbt.
- [ ] Profile Pepsi_DB in `profiles.yml` has correct structure (outputs, target, type, etc.); existing profiles unchanged.
- [ ] Sync installs correct `dbt-<adapter>` and JDBC JARs in `~/.dvt/.jdbc_jars/` for the adapter(s) in Pepsi_DB.
- [ ] Parse and debug commands work against the Pepsi_DB project and profile.
- [ ] Sync env lookup: in-project `.venv`/`venv`/`env` first; then `--python-env`; then interactive prompt.

**Findings**: `trial_7_pepsi_db/findings/data_engineer_findings.md`.

### 5.3 test-team-negative-tester

- [ ] Invalid project name (e.g. numeric-only, special chars): clear validation error or prompt; no overwrite of profiles/computes.
- [ ] `dvt init Pepsi_DB` when Pepsi_DB dir already exists: no overwrite of project; appropriate message.
- [ ] Profile already exists: if init would overwrite, user is prompted; no silent overwrite of other profiles.
- [ ] `dvt sync` / `dvt debug` / `dvt parse` with bad or missing project/profile: clear errors, no silent failure; no overwrite of `profiles.yml` or `computes.yml`.
- [ ] Sync without `--python-env` and no in-project venv: in non-interactive mode, clear stderr message; in interactive mode, prompt for path.

**Findings**: `trial_7_pepsi_db/findings/negative_findings.md`.

---

## 6. Implementation reference

- **Sync**: `core/dvt/task/sync.py` – `_sync_log()` writes all messages to stderr; env resolution: in-project first, then `--python-env`, then interactive prompt. Quoted paths in help and error messages.
- **Flags**: `core/dvt/cli/flags.py` – When already in the invoked subcommand’s context (e.g. sync), we do **not** re-parse `sys.argv`; we keep the Click context so `--project-dir` and `--python-env` are correct.
- **Minimal help**: `core/dvt/cli/main.py` – `sync` uses `_MinimalHelpCommand` and only `@p.project_dir`, `@p.profiles_dir`, `@p.sync_python_env`.
- **Testing agents** use this doc and their rules; each records findings under `trial_7_pepsi_db/findings/`.
