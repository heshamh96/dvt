# DVT Implementation Plan and RULES

Canonical reference for file locations, commands, and feature state. All agents should align with this plan.

---

## Rule 10: File Locations

- **User-level config** (DVT home): `~/.dvt/` (or `PROFILES_DIR` when set)
  - `profiles.yml` – database connection profiles
  - `computes.yml` – Spark compute configs (profile-based: profile → target + computes)
  - `data/mdm.duckdb` – MDM database
- **Project root**: Contains **dbt_project.yml** (primary); **dvt_project.yml** is supported as fallback for backward compatibility.
- **Project config file**: `dbt_project.yml` – name, profile, paths, require-dvt-version, **require-adapters** (adapter version pinning), vars, models, etc.

---

## Project File and Compatibility

- **Primary**: `dbt_project.yml` (ecosystem/tooling compatibility).
- **Fallback**: `dvt_project.yml` (discovery and loading accept both).
- **Starter project**: `core/dvt/include/starter_project/dbt_project.yml`.
- **Adapter packages**: Each has its own root with `dbt_project.yml`; core loads via `get_project_yml_path()`.

---

## computes.yml Structure (Current)

Top-level keys are **profile names**. Each profile has:

- **target**: Name of the active compute for that profile.
- **computes**: Dict of compute name → config (`type`, `version` [for pyspark], `master`, `config`).

Example:

```yaml
default:
  target: default
  computes:
    default:
      type: spark
      version: "3.5.0"
      master: "local[*]"
      config:
        spark.driver.memory: "2g"
```

---

## Commands (Rule 7)

- **dvt init** – Create new project (dbt_project.yml, ~/.dvt/, computes.yml, data/, mdm.duckdb).
- **dvt parse** – Parse project and write manifest.
- **dvt debug** – Configuration and connection checks; supports --config, --targets, --computes, --manifest, --connection &lt;target&gt;.
- **dvt sync** – Sync environment: install adapters and pyspark from dbt_project.yml, profiles.yml, and computes.yml (env discovery: in-project .venv/venv/env or prompt for path; single pyspark version from active target).
- Other: compile, run, test, docs, source, clean, deps, etc. (per dbt-core alignment).

---

## Adapter Version Pinning

- **Location**: `require-adapters` in **dbt_project.yml** (e.g. `require-adapters: { postgres: ">=1.0.0" }`).
- **Honored by**: `dvt sync` when installing dbt-&lt;adapter&gt; packages.

---

## PySpark (Sync)

- **Single version only**: The pyspark version is taken from the **active target** in computes.yml for the project's profile. Any other installed pyspark is uninstalled before installing the required version.

---

## Integration Trials

- **Trial 3** (`trial_integration_3`): init, parse, debug (all sections).
- **Trial 4** (`trial_integration_4`): init (dbt_project.yml), parse, debug (all sections), **dvt sync** – full feature run.
