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

## Sources and connection (multi-datasource)

- In **sources** (sources.yml or any schema YAML), each source block can specify **`connection`**.
- **`connection`** must be the **target (output) name** defined in **profiles.yml** for the project's profile (e.g. `dev`, `prod`, or a custom output name).
- DVT uses this to know which datasource a source uses for virtualization; each source can point to a different profile output so multiple datasources are tracked.

Example (profiles.yml has profile `my_project` with outputs `dev` and `warehouse`):

```yaml
# schema.yml
sources:
  - name: app_events
    connection: dev
    tables:
      - name: events
  - name: legacy_db
    connection: warehouse
    tables:
      - name: users
```

**Catalog and docs (dvt docs generate / dvt docs serve)**  
The catalog artifact (`catalog.json`) includes:
- **source_connections**: map of source unique_id → connection (target) name, so docs can show which connection each source uses.
- **connection_metadata**: map of connection (target) name → optional metadata (e.g. adapter_type for the default target). When docs serve and multi-connection catalog are implemented, this can hold metadata per connection so docs show all datasources and their metadata, not just the default target.

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
