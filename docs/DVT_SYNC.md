# dvt sync

**Purpose**: Sync the project's Python environment with adapters, pyspark, and JDBC drivers required by the current project (profile + computes). Adapters and JDBC drivers are **related**: each dbt adapter type maps to the JDBC driver(s) Spark needs for federation to that database.

## Behavior

1. **Environment discovery**
   - Look for a virtual environment **inside the project folder** (`.venv`, `venv`, or `env`). If found, use it.
   - If not found, **prompt the user** for the absolute path to the Python environment folder they are using. Use that path for all installs.

2. **Adapters**
   - Read the project's **profile** from `dbt_project.yml`.
   - Read **adapter types** used by that profile from `profiles.yml` (targets' `type`).
   - Read **require-adapters** from `dbt_project.yml` (e.g. `require-adapters: { postgres: ">=1.0.0" }`).
   - Install `dbt-<adapter>` for each type, respecting version constraints from `require-adapters` when present.

3. **PySpark (single version only)**
   - Read **computes.yml** (per-profile structure: profile name → `target` + `computes`).
   - Resolve the **active target** for the project's profile (`target:` key).
   - Read that compute's **version** (e.g. `3.5.0`).
   - **Uninstall any installed pyspark**, then install **only** `pyspark==<version>` for the active target. No multiple pyspark versions.

4. **JDBC drivers (adapter–JDBC relationship)**
   - For each **adapter type** from the profile, the implementation looks up the **related** JDBC driver(s) in the registry (e.g. `postgres` → PostgreSQL JDBC jar).
   - Required JARs are downloaded from Maven Central into **`~/.dvt/.jdbc_jars/`** only (canonical location). This is used even when you run from a project or trial folder that has its own `profiles.yml`, so jars are always in one place for Spark federation.
   - Detection is **dynamic**: only adapters present in the profile are considered; the registry maps all supported dbt adapter types to their JDBC driver(s).

5. **Project file**
   - Primary project file is **dbt_project.yml**; **dvt_project.yml** is supported as a fallback for backward compatibility.

## Usage

From a project directory (or with `--project-dir <path>`):

```bash
dvt sync
```

If no in-project env is found, the command will prompt for the path to your Python environment.

## Configuration

- **dbt_project.yml**: `profile`, `require-adapters` (optional).
- **profiles.yml**: Profile outputs with `type` (e.g. postgres). These types are **related** to JDBC drivers in the sync registry.
- **computes.yml**: Profile-based structure; each profile has `target` and `computes`; each compute may have `version` for pyspark.
- **JDBC drivers directory**: `~/.dvt/.jdbc_jars/` only (canonical; not under `DBT_PROFILES_DIR`). Populated by sync based on profile adapter types.

## Supported adapters and JDBC JARs

Sync uses the adapter–JDBC registry in `core/dvt/task/jdbc_drivers.py`. All URLs are verified against Maven Central.

| Adapter     | Maven coordinates (groupId:artifactId:version)           | JAR filename                    |
|-------------|-------------------------------------------------------------|----------------------------------|
| athena      | com.amazonaws:athena-jdbc:2024.51.1                        | athena-jdbc-2024.51.1.jar       |
| clickhouse  | com.clickhouse:clickhouse-jdbc:0.6.0                       | clickhouse-jdbc-0.6.0.jar       |
| databricks  | com.databricks:databricks-jdbc:2.6.34                      | databricks-jdbc-2.6.34.jar      |
| db2         | com.ibm.db2:jcc:11.5.9.0                                    | jcc-11.5.9.0.jar                |
| duckdb      | org.duckdb:duckdb_jdbc:0.10.0                               | duckdb_jdbc-0.10.0.jar          |
| exasol      | com.exasol:exasol-jdbc:7.1.19                               | exasol-jdbc-7.1.19.jar          |
| fabric      | com.microsoft.sqlserver:mssql-jdbc:12.4.2.jre11            | mssql-jdbc-12.4.2.jre11.jar     |
| firebolt    | io.firebolt:firebolt-jdbc:3.6.5                             | firebolt-jdbc-3.6.5.jar         |
| greenplum   | org.postgresql:postgresql:42.7.3                            | postgresql-42.7.3.jar           |
| materialize | org.postgresql:postgresql:42.7.3                            | postgresql-42.7.3.jar           |
| mysql       | com.mysql:mysql-connector-j:8.2.0                           | mysql-connector-j-8.2.0.jar     |
| oracle      | com.oracle.database.jdbc:ojdbc11:23.3.0.23.09              | ojdbc11-23.3.0.23.09.jar        |
| postgres    | org.postgresql:postgresql:42.7.3                            | postgresql-42.7.3.jar           |
| redshift    | com.amazon.redshift:redshift-jdbc42:2.1.0.21                | redshift-jdbc42-2.1.0.21.jar     |
| singlestore | com.singlestore:singlestore-jdbc-client:1.2.0              | singlestore-jdbc-client-1.2.0.jar |
| snowflake   | net.snowflake:snowflake-jdbc:3.10.3                         | snowflake-jdbc-3.10.3.jar       |
| sqlserver   | com.microsoft.sqlserver:mssql-jdbc:12.4.2.jre11             | mssql-jdbc-12.4.2.jre11.jar     |
| synapse     | com.microsoft.sqlserver:mssql-jdbc:12.4.2.jre11            | mssql-jdbc-12.4.2.jre11.jar     |
| tidb        | com.mysql:mysql-connector-j:8.2.0                            | mysql-connector-j-8.2.0.jar     |
| trino       | io.trino:trino-jdbc:428                                     | trino-jdbc-428.jar              |
| vertica     | com.vertica.jdbc:vertica-jdbc:24.4.0-0                       | vertica-jdbc-24.4.0-0.jar       |

*Not in registry:* **spark** (compute engine; no JDBC driver), **bigquery** (uses Spark BigQuery connector), **dremio** (not on Maven Central).

## Using a local dvt-core build with uv

To use your local dvt-core repo (e.g. with the latest `dvt sync` fixes) in another project via `uv sync` **without** an editable install, see [USING_LOCAL_DVT_WITH_UV.md](USING_LOCAL_DVT_WITH_UV.md).
