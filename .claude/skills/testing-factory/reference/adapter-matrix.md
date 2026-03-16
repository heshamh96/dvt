# DVT Adapter Compatibility Matrix

## Database Engines in dvt-adapters

DVT uses the `dvt-adapters` package which bundles all 13 database engine adapters in one package with optional extras for engine-specific drivers.

| # | Engine | Adapter Dir | Optional Extra | Driver Package |
|---|--------|-------------|---------------|----------------|
| 1 | PostgreSQL | `postgres/` | `postgres` | psycopg2-binary |
| 2 | Snowflake | `snowflake/` | `snowflake` | snowflake-connector-python |
| 3 | BigQuery | `bigquery/` | `bigquery` | google-cloud-bigquery |
| 4 | Redshift | `redshift/` | `redshift` | redshift-connector |
| 5 | Databricks | `databricks/` | `databricks` | databricks-sql-connector |
| 6 | DuckDB | `duckdb/` | `duckdb` | duckdb |
| 7 | Spark | `spark/` | `spark` | PyHive |
| 8 | SQL Server | `sqlserver/` | `sqlserver` | pyodbc |
| 9 | Fabric | `fabric/` | `fabric` | pyodbc + azure-identity |
| 10 | MySQL | `mysql/` | `mysql` | mysql-connector-python |
| 11 | MySQL 5 | `mysql5/` | (via mysql) | mysql-connector-python |
| 12 | MariaDB | `mariadb/` | (via mysql) | mysql-connector-python |
| 13 | Oracle | `oracle/` | `oracle` | oracledb |

### Installation

```bash
# Single engine
pip install dvt-adapters[postgres]

# Multiple engines
pip install dvt-adapters[postgres,snowflake,databricks]

# All engines
pip install dvt-adapters[all]
```

## Docker Test Engines (5)

| Engine | Service | Container | Port | Status |
|--------|---------|-----------|------|--------|
| PostgreSQL 16 | postgres | db_postgres | 5432 | Supported |
| MySQL 8.0 | mysql | db_mysql | 3306 | Community |
| SQL Server 2022 | sqlserver | db_sqlserver | 1433 | Community |
| Oracle XE 21c | oracle | db_oracle | 1521 | Future |
| MariaDB 11 | mariadb | db_mariadb | 3307 | Community |

**Note**: Docker engines run in containers on a shared `db_network`. External engines (pg_dev, Snowflake, Databricks) are already running and don't need Docker management.

## Container Credentials (from .env)

| Engine | User | Password | Database |
|--------|------|----------|----------|
| PostgreSQL | devuser | Postgres_Dev_2026! | devdb |
| MySQL | devuser | MySql_Dev_2026! | devdb |
| SQL Server | sa | SqlServer_Dev_2026! | master (+ devdb after init) |
| Oracle | system | Oracle_Dev_2026! | XEPDB1 |
| MariaDB | devuser | MariaDb_Dev_2026! | devdb |

**Note**: Container PG (port 5432) is separate from the development PG (port 5433). Don't confuse them.

## Feature Support Matrix

| Feature | PostgreSQL | MySQL | SQL Server | Oracle | MariaDB | Databricks | Snowflake | BigQuery | Redshift | DuckDB |
|---------|-----------|-------|------------|--------|---------|-----------|-----------|----------|----------|--------|
| **Seeds** | | | | | | | | | | |
| CSV seeds | YES | YES | YES | YES | YES | YES | YES | YES | YES | YES |
| JSON seeds | YES | TBD | TBD | TBD | TBD | YES | YES | TBD | TBD | YES |
| Parquet seeds | YES | TBD | TBD | TBD | TBD | YES | YES | TBD | TBD | YES |
| **Materializations** | | | | | | | | | | |
| table | YES | YES | YES | YES | YES | YES | YES | YES | YES | YES |
| view | YES | YES | YES | YES | YES | YES | YES | YES | YES | YES |
| incremental (append) | YES | TBD | TBD | TBD | TBD | YES | YES | TBD | TBD | YES |
| incremental (merge) | YES | TBD | TBD | TBD | TBD | YES | YES | TBD | TBD | YES |
| incremental (del+ins) | YES | TBD | TBD | TBD | TBD | YES | YES | TBD | TBD | YES |
| ephemeral | YES | YES | YES | YES | YES | YES | YES | YES | YES | YES |
| **DDL Contract** | | | | | | | | | | |
| TRUNCATE + INSERT | YES | YES | YES | TBD | YES | YES | YES | TBD | TBD | YES |
| DROP + CREATE + INSERT | YES | YES | YES | TBD | YES | YES | YES | TBD | TBD | YES |
| **Federation** | | | | | | | | | | |
| Sling extraction | YES | YES | YES | TBD | YES | YES | YES | TBD | TBD | YES |
| Sling loading | YES | YES | YES | TBD | YES | YES | YES | TBD | TBD | YES |

## profiles.yml Template for Containerized Engines

```yaml
AdapterTest:
  outputs:
    pg_docker:
      type: postgres
      host: localhost
      port: 5432
      user: devuser
      password: Postgres_Dev_2026!
      database: devdb
      schema: public
      threads: 4

    mysql_docker:
      type: mysql
      host: localhost
      port: 3306
      user: devuser
      password: MySql_Dev_2026!
      database: devdb
      schema: devdb
      threads: 4

    mssql_docker:
      type: sqlserver
      host: localhost
      port: 1433
      user: sa
      password: SqlServer_Dev_2026!
      database: devdb
      schema: dbo
      threads: 4

    oracle_docker:
      type: oracle
      host: localhost
      port: 1521
      user: system
      password: Oracle_Dev_2026!
      database: XEPDB1
      schema: SYSTEM
      threads: 4

    mariadb_docker:
      type: mysql
      host: localhost
      port: 3307
      user: devuser
      password: MariaDb_Dev_2026!
      database: devdb
      schema: devdb
      threads: 4

  target: pg_docker
```

## Test Sequence Per Engine

### Docker Engines (postgres, mysql, mssql, oracle, mariadb)

```bash
# 1. Start container
docker compose up -d <engine>

# 2. Wait for health check
docker compose ps  # Should show "healthy"

# 3-6. Standard test steps (see below)
```

### External Engines (pg_dev, snowflake, databricks)

```bash
# No Docker steps — engine must already be running
# Skip directly to standard test steps
```

### Standard Test Steps (all engines)

```bash
# 1. Verify connection
dvt debug --target <target> --profiles-dir test_project/Connections --project-dir test_project

# 2. Seed test data
dvt seed --target <target> --profiles-dir test_project/Connections --project-dir test_project

# 3. Run models
dvt run --target <target> --profiles-dir test_project/Connections --project-dir test_project

# 4. Full refresh (DDL contract: DROP+CREATE+INSERT)
dvt run --full-refresh --target <target> --profiles-dir test_project/Connections --project-dir test_project
```

## Cross-Engine Federation Test Models

These models test DVT's core differentiator: reading from one database engine and writing to another via Sling extraction into DuckDB cache for cross-engine SQL, then Sling loading to the target.

**Location**: `Testing_adapters_docker/test_project/models/federation/`

### Federation data flow (Sling + DuckDB)

```
Source DB              Sling              DuckDB Cache           Sling              Target DB
  [PG]  ──extract──>  [CSV/Parquet]  ──>  [Local cache]  ──load──>  [MySQL/MSSQL/Oracle/etc.]
  [MySQL] ──extract──>  [CSV/Parquet]  ──>  [Local cache]  ──load──>  [PG/SF/DBX]
  [SF] ──extract──>  [CSV/Parquet]  ──>  [Local cache]  ──load──>  [PG]
  [DBX] ──extract──>  [CSV/Parquet]  ──>  [Local cache]  ──load──>  [PG]
  [PG+MySQL] ──extract──>  [Both cached]  ──>  [DuckDB JOIN]  ──load──>  [PG]
```

### Federation validation checks

For each federation model:
1. **Row count**: Target table has expected number of rows
2. **Data integrity**: All column values match source data
3. **DDL contract**: TRUNCATE+INSERT on default run, DROP+CREATE+INSERT on --full-refresh
4. **Sling extraction**: Source data correctly extracted to DuckDB cache
5. **DuckDB compute**: Cross-engine SQL executed correctly in DuckDB

### Running federation tests

```bash
# Run only federation tests (starts all required containers)
./run_adapter_tests.sh federation

# Run everything (all engines + federation = full suite)
./run_adapter_tests.sh all

# Run only Docker engines
./run_adapter_tests.sh docker

# Run only external engines (pg_dev, snowflake, databricks)
./run_adapter_tests.sh external

# Run a specific engine
./run_adapter_tests.sh postgres
./run_adapter_tests.sh snowflake
./run_adapter_tests.sh databricks
```

## System Dependencies Per Adapter

| Adapter | Python Package | System Deps | Notes |
|---------|---------------|-------------|-------|
| postgres | dvt-adapters[postgres] | libpq (usually bundled) | pip install just works |
| mysql | dvt-adapters[mysql] | mysqlclient -> libmysqlclient-dev | `apt install libmysqlclient-dev` or `brew install mysql-client` |
| sqlserver | dvt-adapters[sqlserver] | pyodbc -> unixODBC + FreeTDS | `apt install unixodbc-dev freetds-dev` or `brew install freetds` |
| oracle | dvt-adapters[oracle] | oracledb (thin mode) | No system deps in thin mode |
| databricks | dvt-adapters[databricks] | None | pip install just works |
| snowflake | dvt-adapters[snowflake] | None (uses snowflake-connector-python) | pip install just works |
| bigquery | dvt-adapters[bigquery] | None | pip install just works |
| redshift | dvt-adapters[redshift] | None | pip install just works |
| duckdb | dvt-adapters[duckdb] | None | pip install just works |
