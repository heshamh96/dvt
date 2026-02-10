# DVT CLI Command: `dvt seed`

## Phase: 2 (Spark Seeds)
## Type: Full Implementation

---

## Current Implementation Summary

**File**: `core/dvt/task/seed.py`

The current `dvt seed` command uses **agate** library to:
1. Read CSV files from `seeds/` directory
2. Infer column types
3. Create tables in the target database
4. Insert rows using adapter's INSERT statements

**Limitations of current implementation**:
- Slow for large files (row-by-row inserts)
- Limited type inference
- Cannot seed to different targets than the default
- No cross-target seeding support

---

## Required Changes

### Full: Replace agate with Spark-based Seeding

Replace the agate-based implementation with Spark for:
1. Better performance on large files
2. Cross-target seeding support (seed to any target)
3. Batch loading via JDBC or native connectors
4. Configurable batch sizes and parallelism

### Reference Document
- `spark_seed_plan.md` - Full implementation details

---

## Implementation Details

### 1. New Seed Runner

**File**: `core/dvt/task/seed.py`

```python
from typing import Optional
from pyspark.sql import SparkSession, DataFrame
from dvt.federation.spark_manager import SparkManager
from dvt.task.native_connectors import get_native_connector


class SparkSeedRunner(BaseRunner):
    """Spark-based seed runner for DVT.
    
    Uses Spark to load CSV files and write to target databases via JDBC
    or native connectors for better performance.
    """
    
    def __init__(self, config, manifest, node):
        super().__init__(config, manifest, node)
        self.spark_manager = SparkManager(config)
    
    def execute(self, seed: SeedNode, manifest: Manifest) -> RunResult:
        """Execute seed using Spark."""
        
        # Determine target for this seed
        target = self._resolve_target(seed)
        
        # Get Spark session
        spark = self.spark_manager.get_or_create_session()
        
        # Read CSV with Spark
        df = self._read_csv_with_spark(spark, seed)
        
        # Apply schema overrides from seed config
        df = self._apply_schema_overrides(df, seed)
        
        # Write to target database
        result = self._write_to_target(df, seed, target)
        
        return result
    
    def _resolve_target(self, seed: SeedNode) -> str:
        """Resolve which target to seed to.
        
        Priority: seed config target > CLI --target > profiles.yml default
        """
        # Check seed-level config
        if seed.config.get("target"):
            return seed.config["target"]
        
        # Check CLI flag
        if self.config.args.target:
            return self.config.args.target
        
        # Default from profiles.yml
        return self.config.profile.target
    
    def _read_csv_with_spark(self, spark: SparkSession, seed: SeedNode) -> DataFrame:
        """Read CSV file using Spark.
        
        Handles:
        - Header detection
        - Type inference
        - Null value handling
        - Quote character handling
        """
        csv_path = seed.root_path / seed.original_file_path
        
        return spark.read.csv(
            str(csv_path),
            header=True,
            inferSchema=True,
            nullValue="",
            quote='"',
            escape='"',
            multiLine=True,
        )
    
    def _apply_schema_overrides(self, df: DataFrame, seed: SeedNode) -> DataFrame:
        """Apply column type overrides from seed config.
        
        Example seed config:
            seeds:
              my_seed:
                +column_types:
                  id: bigint
                  created_at: timestamp
        """
        column_types = seed.config.get("column_types", {})
        
        for col_name, col_type in column_types.items():
            if col_name in df.columns:
                df = df.withColumn(col_name, df[col_name].cast(col_type))
        
        return df
    
    def _write_to_target(
        self,
        df: DataFrame,
        seed: SeedNode,
        target: str,
    ) -> RunResult:
        """Write DataFrame to target database.
        
        Uses native connector if available, otherwise JDBC.
        """
        adapter_type = self._get_adapter_type(target)
        native_connector = get_native_connector(adapter_type)
        
        # Get connection details
        connection = self._get_connection_for_target(target)
        
        # Determine table name
        table_name = self._get_full_table_name(seed, connection)
        
        if native_connector and self._native_connector_available(native_connector):
            return self._write_with_native_connector(
                df, seed, connection, table_name, native_connector
            )
        else:
            return self._write_with_jdbc(df, seed, connection, table_name)
    
    def _write_with_jdbc(
        self,
        df: DataFrame,
        seed: SeedNode,
        connection: dict,
        table_name: str,
    ) -> RunResult:
        """Write DataFrame using JDBC connector."""
        jdbc_url = self._build_jdbc_url(connection)
        
        # Determine write mode
        write_mode = "overwrite" if seed.config.get("full_refresh", True) else "append"
        
        try:
            df.write.jdbc(
                url=jdbc_url,
                table=table_name,
                mode=write_mode,
                properties={
                    "user": connection.get("user", ""),
                    "password": connection.get("password", ""),
                    "driver": self._get_jdbc_driver(connection),
                    "batchsize": str(seed.config.get("batch_size", 10000)),
                },
            )
            
            row_count = df.count()
            
            return RunResult(
                status=RunStatus.Success,
                message=f"Seeded {row_count} rows to {table_name}",
                adapter_response={"rows_affected": row_count},
            )
            
        except Exception as e:
            return RunResult(
                status=RunStatus.Error,
                message=str(e),
            )
    
    def _write_with_native_connector(
        self,
        df: DataFrame,
        seed: SeedNode,
        connection: dict,
        table_name: str,
        connector: NativeConnectorSpec,
    ) -> RunResult:
        """Write DataFrame using native connector (Snowflake, BigQuery, Redshift).
        
        Native connectors stage data to cloud storage first, then use
        COPY INTO or similar commands for bulk loading.
        """
        adapter_type = connector.adapter
        
        if adapter_type == "snowflake":
            return self._write_snowflake_native(df, seed, connection, table_name)
        elif adapter_type == "bigquery":
            return self._write_bigquery_native(df, seed, connection, table_name)
        elif adapter_type == "redshift":
            return self._write_redshift_native(df, seed, connection, table_name)
        else:
            # Fallback to JDBC
            return self._write_with_jdbc(df, seed, connection, table_name)
    
    def _write_snowflake_native(
        self,
        df: DataFrame,
        seed: SeedNode,
        connection: dict,
        table_name: str,
    ) -> RunResult:
        """Write to Snowflake using native Spark connector."""
        options = {
            "sfURL": f"{connection['account']}.snowflakecomputing.com",
            "sfUser": connection["user"],
            "sfPassword": connection["password"],
            "sfDatabase": connection["database"],
            "sfSchema": connection.get("schema", "public"),
            "sfWarehouse": connection.get("warehouse"),
            "dbtable": table_name,
        }
        
        write_mode = "overwrite" if seed.config.get("full_refresh", True) else "append"
        
        try:
            df.write.format("snowflake").options(**options).mode(write_mode).save()
            
            row_count = df.count()
            return RunResult(
                status=RunStatus.Success,
                message=f"Seeded {row_count} rows to {table_name} (Snowflake native)",
                adapter_response={"rows_affected": row_count},
            )
        except Exception as e:
            return RunResult(status=RunStatus.Error, message=str(e))
    
    def _write_bigquery_native(
        self,
        df: DataFrame,
        seed: SeedNode,
        connection: dict,
        table_name: str,
    ) -> RunResult:
        """Write to BigQuery using native Spark connector."""
        # Get temporary GCS bucket from buckets.yml
        bucket_config = self._get_bucket_config("bigquery")
        temp_bucket = bucket_config.get("bucket") if bucket_config else None
        
        if not temp_bucket:
            fire_event(NativeConnectorSkipped(
                adapter="bigquery",
                reason="No GCS bucket configured in buckets.yml",
            ))
            return self._write_with_jdbc(df, seed, connection, table_name)
        
        write_mode = "overwrite" if seed.config.get("full_refresh", True) else "append"
        
        try:
            df.write.format("bigquery") \
                .option("table", f"{connection['project']}.{connection['dataset']}.{table_name}") \
                .option("temporaryGcsBucket", temp_bucket) \
                .mode(write_mode) \
                .save()
            
            row_count = df.count()
            return RunResult(
                status=RunStatus.Success,
                message=f"Seeded {row_count} rows to {table_name} (BigQuery native)",
                adapter_response={"rows_affected": row_count},
            )
        except Exception as e:
            return RunResult(status=RunStatus.Error, message=str(e))
    
    def _write_redshift_native(
        self,
        df: DataFrame,
        seed: SeedNode,
        connection: dict,
        table_name: str,
    ) -> RunResult:
        """Write to Redshift using native Spark connector with S3 staging."""
        bucket_config = self._get_bucket_config("redshift")
        
        if not bucket_config:
            fire_event(NativeConnectorSkipped(
                adapter="redshift",
                reason="No S3 bucket configured in buckets.yml",
            ))
            return self._write_with_jdbc(df, seed, connection, table_name)
        
        temp_dir = f"s3://{bucket_config['bucket']}/{bucket_config.get('prefix', '')}temp/"
        iam_role = bucket_config.get("iam_role")
        
        write_mode = "overwrite" if seed.config.get("full_refresh", True) else "append"
        
        try:
            writer = df.write.format("io.github.spark_redshift_community.spark.redshift") \
                .option("url", self._build_jdbc_url(connection)) \
                .option("dbtable", table_name) \
                .option("tempdir", temp_dir) \
                .option("forward_spark_s3_credentials", "true")
            
            if iam_role:
                writer = writer.option("aws_iam_role", iam_role)
            
            writer.mode(write_mode).save()
            
            row_count = df.count()
            return RunResult(
                status=RunStatus.Success,
                message=f"Seeded {row_count} rows to {table_name} (Redshift native)",
                adapter_response={"rows_affected": row_count},
            )
        except Exception as e:
            return RunResult(status=RunStatus.Error, message=str(e))
```

### 2. Seed Task Modifications

**File**: `core/dvt/task/seed.py`

```python
class SeedTask(RunTask):
    """DVT Seed Task - uses Spark for loading CSVs."""
    
    def get_runner_type(self, node: SeedNode) -> Type[BaseRunner]:
        """Return SparkSeedRunner for seed nodes."""
        return SparkSeedRunner
    
    def before_run(self, adapter, selected_uids):
        """Initialize Spark before running seeds."""
        # Initialize Spark session
        self.spark_manager = SparkManager(self.config)
        self.spark_manager.get_or_create_session()
        
        return super().before_run(adapter, selected_uids)
    
    def after_run(self, adapter, results):
        """Clean up Spark after running seeds."""
        # Stop Spark session if no longer needed
        if hasattr(self, 'spark_manager'):
            self.spark_manager.stop_session()
        
        return super().after_run(adapter, results)
```

### 3. CLI Enhancements

**File**: `core/dvt/cli/main.py`

```python
@click.command("seed")
@click.option("--target", "-t", help="Target to seed to (overrides default)")
@click.option("--full-refresh", is_flag=True, help="Drop and recreate seed tables")
@click.option("--batch-size", type=int, default=10000, help="Batch size for JDBC writes")
@click.option("--select", "-s", multiple=True, help="Select specific seeds")
@click.pass_context
def seed(ctx, target, full_refresh, batch_size, select):
    """Load CSV files as tables using Spark.
    
    Seeds can be loaded to any target specified in profiles.yml.
    Uses native connectors for Snowflake/BigQuery/Redshift when available.
    """
    # ... implementation
```

### 4. Seed Configuration Schema

**File**: `core/dvt/contracts/graph/model_config.py`

```python
@dataclass
class SeedConfig(NodeConfig):
    """Configuration for seed nodes."""
    
    # Target to seed to (optional, defaults to profile default)
    target: Optional[str] = None
    
    # Column type overrides
    column_types: Dict[str, str] = field(default_factory=dict)
    
    # Batch size for JDBC writes
    batch_size: int = 10000
    
    # Whether to drop and recreate table
    full_refresh: bool = True
    
    # Quote column names (for reserved words)
    quote_columns: bool = False
```

---

## CLI Output Changes

Before (agate-based):
```
$ dvt seed
Running with dbt=1.8.0
Found 3 seeds

Seeding my_seed_1
  Inserting 1000 rows...
  Completed in 45.2s

Seeding my_seed_2
  Inserting 5000 rows...
  Completed in 3m 12s
```

After (Spark-based):
```
$ dvt seed
Running with dvt=1.8.0
Found 3 seeds
Initializing Spark session...

Seeding my_seed_1
  Reading CSV (1,000 rows)...
  Writing to postgres_prod.public.my_seed_1 via JDBC...
  Completed in 2.1s (476 rows/sec)

Seeding my_seed_2 (target: snowflake_dw)
  Reading CSV (50,000 rows)...
  Writing to snowflake_dw.analytics.my_seed_2 via Snowflake native connector...
  Completed in 8.3s (6,024 rows/sec)

Completed 3 seeds in 15.2s
```

---

## Example Seed Configuration

```yaml
# dbt_project.yml
seeds:
  my_project:
    +target: postgres_prod      # Default target for all seeds
    +batch_size: 20000
    
    large_reference_data:
      +target: snowflake_dw     # Override: seed to Snowflake
      +column_types:
        id: bigint
        amount: decimal(18,2)
        created_at: timestamp
    
    small_lookup:
      # Uses default target (postgres_prod)
      +full_refresh: false      # Append mode
```

---

## Testing Requirements

### Unit Tests

**File**: `tests/unit/test_spark_seed_runner.py`

```python
def test_resolve_target_from_seed_config():
    """Test target resolution from seed config."""
    seed = SeedNode(config={"target": "snowflake_dw"})
    runner = SparkSeedRunner(config, manifest, seed)
    
    target = runner._resolve_target(seed)
    assert target == "snowflake_dw"


def test_resolve_target_falls_back_to_default():
    """Test target resolution falls back to profile default."""
    seed = SeedNode(config={})
    runner = SparkSeedRunner(config, manifest, seed)
    
    target = runner._resolve_target(seed)
    assert target == config.profile.target


def test_apply_schema_overrides():
    """Test column type overrides are applied."""
    spark = SparkSession.builder.getOrCreate()
    df = spark.createDataFrame([("1", "100.50")], ["id", "amount"])
    
    seed = SeedNode(config={"column_types": {"id": "bigint", "amount": "decimal(18,2)"}})
    runner = SparkSeedRunner(config, manifest, seed)
    
    result = runner._apply_schema_overrides(df, seed)
    
    assert result.schema["id"].dataType == LongType()
    assert result.schema["amount"].dataType == DecimalType(18, 2)
```

### Integration Tests

**File**: `tests/functional/test_spark_seed.py`

```python
def test_seed_to_default_target(project):
    """Test seeding to default target via JDBC."""
    write_file(project.project_root / "seeds" / "my_seed.csv", "id,name\n1,Alice\n2,Bob")
    
    result = run_dbt(["seed"])
    
    assert result.success
    # Verify data in database
    rows = run_query("SELECT COUNT(*) FROM my_seed")
    assert rows[0][0] == 2


def test_seed_to_specific_target(project):
    """Test seeding to specified target."""
    write_file(project.project_root / "seeds" / "my_seed.csv", "id,name\n1,Alice")
    write_file(
        project.project_root / "dbt_project.yml",
        """
seeds:
  my_project:
    my_seed:
      +target: secondary_target
"""
    )
    
    result = run_dbt(["seed"])
    
    assert result.success
    # Verify data in secondary target
```

---

## Dependencies

- `01_init.md` - buckets.yml for native connector staging
- `02_sync.md` - native connector JARs must be downloaded
- Phase 1 commands must be complete

## Blocked By

- `01_init.md`
- `02_sync.md`

## Blocks

- `08_build.md` - build orchestrates seed + run + test

---

## Implementation Checklist

- [ ] Create `SparkSeedRunner` class
- [ ] Implement `_resolve_target()` for target selection
- [ ] Implement `_read_csv_with_spark()` for CSV reading
- [ ] Implement `_apply_schema_overrides()` for type casting
- [ ] Implement `_write_with_jdbc()` for generic JDBC writes
- [ ] Implement `_write_snowflake_native()` for Snowflake
- [ ] Implement `_write_bigquery_native()` for BigQuery
- [ ] Implement `_write_redshift_native()` for Redshift
- [ ] Update `SeedTask` to use `SparkSeedRunner`
- [ ] Add CLI options for target, batch-size
- [ ] Update `SeedConfig` schema
- [ ] Add unit tests for all methods
- [ ] Add integration tests for JDBC and native connectors
- [ ] Update documentation
