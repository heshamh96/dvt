# Trial 4: dvt sync (adapters, PySpark, JDBC drivers) – Test team checklist

Use this checklist when running **trial 4** in the Testing Playground (`trial_dvt_sync_4` or similar). Each team can use the sections that apply.

## Scope for this trial

- **dvt sync** installs:
  1. **Adapters**: `dbt-<adapter>` per profile target type (from `profiles.yml`).
  2. **PySpark**: single version from `computes.yml` for the active target.
  3. **JDBC drivers**: JARs for each adapter type, always downloaded to **`~/.dvt/.jdbc_jars/`** only (even when the trial has its own `profiles.yml` in the project).

- **Adapter–JDBC relationship**: Profile adapter types (e.g. `postgres`, `snowflake`) are mapped to JDBC driver JARs in `core/dvt/task/jdbc_drivers.py`. Sync downloads only the JARs that correspond to the adapters in the project’s profile.

---

## Technical QA (paths, names, files)

- [ ] **Environment**: From project dir, `dvt sync` uses an in-project env (`.venv`, `venv`, or `env`) if present; otherwise prompts for path.
- [ ] **Profile**: Profile name comes from `dbt_project.yml` → `profile`; adapter types from `profiles.yml` for that profile (each target’s `type`).
- [ ] **JDBC directory**: After sync, **`~/.dvt/.jdbc_jars/`** exists and contains one JAR per adapter type that has a JDBC driver in the registry (e.g. `postgresql-42.7.3.jar` for `postgres`). Check this path even when running from a trial folder that has its own `profiles.yml`.
- [ ] **computes.yml**: If present, active target’s `version` is used for `pyspark==<version>`; other pyspark is uninstalled first.
- [ ] **Idempotency**: Re-running `dvt sync` does not re-download JARs that are already present in `.jdbc_jars/`.

---

## Data engineer / functional

- [ ] **Profile with one adapter**: Profile with `type: postgres` → sync installs `dbt-postgres` and downloads PostgreSQL JDBC JAR.
- [ ] **Profile with multiple adapters**: Profile with e.g. `postgres` and `snowflake` → both adapters installed and both JDBC JARs in `.jdbc_jars/`.
- [ ] **Adapter not in registry**: Profile with `type: spark` only → no JDBC download (spark has no JDBC driver); sync still completes.
- [ ] **Require-adapters**: If `dbt_project.yml` has `require-adapters: { postgres: ">=1.0.0" }`, adapter install respects that constraint.

---

## Negative testing

- [ ] **No project**: Run `dvt sync` from a directory with no `dbt_project.yml` → clear message (e.g. “Not in a DVT project”) and non-zero exit.
- [ ] **No profile in project**: `dbt_project.yml` has no `profile` → sync skipped with message.
- [ ] **No venv and non-interactive**: No in-project env and no input (e.g. CI) → sync skipped with message, no hang.
- [ ] **Invalid env path**: User enters a path that is not a directory or has no `bin/python` → clear error.

---

## Implementation reference

- **Registry**: `core/dvt/task/jdbc_drivers.py` – `ADAPTER_TO_JDBC_DRIVERS` and `get_jdbc_drivers_for_adapters`, `download_jdbc_jars`.
- **Sync task**: `core/dvt/task/sync.py` – step 5 uses profile adapter types → JDBC drivers → download into `get_jdbc_drivers_dir(profiles_dir)` (resolves to `~/.dvt/.jdbc_jars/`).
- **Docs**: `docs/DVT_SYNC.md` – behavior and supported adapters/JARs table.
- **Verify URLs**: `PYTHONPATH=core python scripts/verify_jdbc_urls.py` – all registry URLs should return 200.

---

## Trial folder setup (Testing Playground)

1. Create `trial_dvt_sync_4` as a uv project with dvt-core installed via **local path, non-editable**. See **[docs/USING_LOCAL_DVT_WITH_UV.md](USING_LOCAL_DVT_WITH_UV.md)**. In the trial's `pyproject.toml`: `dependencies = ["dvt-core", ...]` and `[tool.uv.sources] dvt-core = { path = "/full/path/to/dvt-core/core", editable = false }`, then `uv sync`.
2. Add a minimal `dbt_project.yml` with `profile: <name>` and `profiles.yml` with that profile and at least one target with `type: postgres` (or another supported adapter).
3. Create `.venv` in the project or point sync to an existing env when prompted.
4. Run `uv run dvt sync` from the trial folder.
5. Record findings under `trial_dvt_sync_4/findings/` (e.g. `technical_qa_checklist.md`, `negative_findings.md`).
