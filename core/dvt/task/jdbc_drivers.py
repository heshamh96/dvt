# coding=utf-8
"""
Adapter–JDBC relationship: maps each dbt adapter type to the JDBC driver(s) required
for Spark federation (reading/writing to that target via JDBC).

This is the canonical mapping: when a profile uses adapter X, sync downloads the
related JDBC jar(s) so Spark can connect to that database during cross-target runs.

Supported adapters and JARs (used by sync; all URLs verified):

| Adapter     | Maven coordinates (groupId:artifactId:version)           | JAR filename                    |
|------------|-------------------------------------------------------------|----------------------------------|
| athena     | com.amazonaws:athena-jdbc:2024.51.1                        | athena-jdbc-2024.51.1.jar       |
| clickhouse | com.clickhouse:clickhouse-jdbc:0.6.0                        | clickhouse-jdbc-0.6.0.jar       |
| databricks | com.databricks:databricks-jdbc:2.6.34                      | databricks-jdbc-2.6.34.jar      |
| db2        | com.ibm.db2:jcc:11.5.9.0                                    | jcc-11.5.9.0.jar                |
| duckdb     | org.duckdb:duckdb_jdbc:0.10.0                               | duckdb_jdbc-0.10.0.jar         |
| exasol     | com.exasol:exasol-jdbc:7.1.19                               | exasol-jdbc-7.1.19.jar          |
| fabric     | com.microsoft.sqlserver:mssql-jdbc:12.4.2.jre11            | mssql-jdbc-12.4.2.jre11.jar     |
| firebolt   | io.firebolt:firebolt-jdbc:3.6.5                             | firebolt-jdbc-3.6.5.jar         |
| greenplum  | org.postgresql:postgresql:42.7.3                            | postgresql-42.7.3.jar           |
| materialize| org.postgresql:postgresql:42.7.3                            | postgresql-42.7.3.jar           |
| mysql      | com.mysql:mysql-connector-j:8.2.0                           | mysql-connector-j-8.2.0.jar     |
| oracle     | com.oracle.database.jdbc:ojdbc11:23.3.0.23.09              | ojdbc11-23.3.0.23.09.jar        |
| postgres   | org.postgresql:postgresql:42.7.3                            | postgresql-42.7.3.jar           |
| redshift   | com.amazon.redshift:redshift-jdbc42:2.1.0.21                | redshift-jdbc42-2.1.0.21.jar    |
| singlestore| com.singlestore:singlestore-jdbc-client:1.2.0                | singlestore-jdbc-client-1.2.0.jar |
| snowflake  | net.snowflake:snowflake-jdbc:3.10.3                          | snowflake-jdbc-3.10.3.jar       |
| sqlserver  | com.microsoft.sqlserver:mssql-jdbc:12.4.2.jre11            | mssql-jdbc-12.4.2.jre11.jar     |
| synapse    | com.microsoft.sqlserver:mssql-jdbc:12.4.2.jre11            | mssql-jdbc-12.4.2.jre11.jar     |
| tidb       | com.mysql:mysql-connector-j:8.2.0                           | mysql-connector-j-8.2.0.jar     |
| trino      | io.trino:trino-jdbc:428                                     | trino-jdbc-428.jar              |
| vertica    | com.vertica.jdbc:vertica-jdbc:24.4.0-0                       | vertica-jdbc-24.4.0-0.jar      |

Not in registry: spark (compute engine, no JDBC), bigquery (Spark connector), dremio (not on Maven Central).
"""
from pathlib import Path
from typing import Callable, List, Optional, Tuple

# Maven coordinates: (groupId, artifactId, version)
# Adapter type (dbt adapter name from profiles.yml "type") -> list of (groupId, artifactId, version)
ADAPTER_TO_JDBC_DRIVERS: dict[str, list[tuple[str, str, str]]] = {
    "postgres": [
        ("org.postgresql", "postgresql", "42.7.3"),
    ],
    "redshift": [
        ("com.amazon.redshift", "redshift-jdbc42", "2.1.0.21"),
    ],
    "snowflake": [
        ("net.snowflake", "snowflake-jdbc", "3.10.3"),
    ],
    "mysql": [
        ("com.mysql", "mysql-connector-j", "8.2.0"),
    ],
    "oracle": [
        ("com.oracle.database.jdbc", "ojdbc11", "23.3.0.23.09"),
    ],
    "sqlserver": [
        ("com.microsoft.sqlserver", "mssql-jdbc", "12.4.2.jre11"),
    ],
    "databricks": [
        ("com.databricks", "databricks-jdbc", "2.6.34"),
    ],
    "trino": [
        ("io.trino", "trino-jdbc", "428"),
    ],
    "duckdb": [
        ("org.duckdb", "duckdb_jdbc", "0.10.0"),
    ],
    "exasol": [
        ("com.exasol", "exasol-jdbc", "7.1.19"),
    ],
    "greenplum": [
        # Greenplum is PostgreSQL-compatible; use PostgreSQL driver
        ("org.postgresql", "postgresql", "42.7.3"),
    ],
    "vertica": [
        ("com.vertica.jdbc", "vertica-jdbc", "24.4.0-0"),
    ],
    "clickhouse": [
        ("com.clickhouse", "clickhouse-jdbc", "0.6.0"),
    ],
    "synapse": [
        # Azure Synapse uses MS SQL driver
        ("com.microsoft.sqlserver", "mssql-jdbc", "12.4.2.jre11"),
    ],
    "fabric": [
        ("com.microsoft.sqlserver", "mssql-jdbc", "12.4.2.jre11"),
    ],
    "singlestore": [
        ("com.singlestore", "singlestore-jdbc-client", "1.2.0"),
    ],
    "tidb": [
        # TiDB is MySQL-compatible
        ("com.mysql", "mysql-connector-j", "8.2.0"),
    ],
    "materialize": [
        # Materialize is PostgreSQL-compatible
        ("org.postgresql", "postgresql", "42.7.3"),
    ],
    "firebolt": [
        ("io.firebolt", "firebolt-jdbc", "3.6.5"),
    ],
    "athena": [
        ("com.amazonaws", "athena-jdbc", "2024.51.1"),
    ],
    # dremio: not published to Maven Central; omit until available or use manual install
    "db2": [
        ("com.ibm.db2", "jcc", "11.5.9.0"),
    ],
    # spark: no JDBC driver (Spark is the compute engine)
    # bigquery: typically uses Spark BigQuery connector, not JDBC; omit or add later if needed
}


def get_jdbc_drivers_for_adapters(adapter_types: List[str]) -> List[Tuple[str, str, str]]:
    """
    Return the list of JDBC driver Maven coordinates (groupId, artifactId, version)
    that relate to the given dbt adapter types. Used by sync to know which jars to download.
    Deduplicated (same driver for multiple adapters appears once).
    """
    seen: set[Tuple[str, str, str]] = set()
    out: List[Tuple[str, str, str]] = []
    for adapter_type in adapter_types:
        drivers = ADAPTER_TO_JDBC_DRIVERS.get(adapter_type.strip().lower())
        if not drivers:
            continue
        for coord in drivers:
            if coord not in seen:
                seen.add(coord)
                out.append(coord)
    return out


def _maven_jar_url(group_id: str, artifact_id: str, version: str) -> str:
    path = group_id.replace(".", "/") + f"/{artifact_id}/{version}/{artifact_id}-{version}.jar"
    return "https://repo1.maven.org/maven2/" + path


def _download_jar(
    group_id: str,
    artifact_id: str,
    version: str,
    dest_dir: Path,
    on_event: Optional[Callable[[str], None]] = None,
) -> bool:
    """Download a single JAR from Maven Central. Return True on success."""
    dest_dir.mkdir(parents=True, exist_ok=True)
    jar_name = f"{artifact_id}-{version}.jar"
    dest_path = dest_dir / jar_name
    if dest_path.exists():
        return True
    url = _maven_jar_url(group_id, artifact_id, version)
    try:
        import urllib.request

        req = urllib.request.Request(url, headers={"User-Agent": "DVT-Sync/1.0"})
        with urllib.request.urlopen(req, timeout=120) as resp:
            if resp.status != 200:
                if on_event:
                    on_event(f"JDBC: skipped {jar_name} (HTTP {resp.status})")
                return False
            dest_path.write_bytes(resp.read())
        if on_event:
            on_event(f"JDBC: downloaded {jar_name}")
        return True
    except Exception as e:
        if on_event:
            on_event(f"JDBC: failed to download {jar_name}: {e}")
        return False


def download_jdbc_jars(
    drivers: List[Tuple[str, str, str]],
    dest_dir: Path,
    on_event: Optional[Callable[[str], None]] = None,
) -> int:
    """
    Download JDBC driver JARs for the given Maven coordinates into dest_dir.
    Skips already-present JARs. Returns the number of JARs successfully present after (downloaded or existing).
    """
    dest_dir = Path(dest_dir)
    ok = 0
    for group_id, artifact_id, version in drivers:
        if _download_jar(group_id, artifact_id, version, dest_dir, on_event):
            ok += 1
    return ok
