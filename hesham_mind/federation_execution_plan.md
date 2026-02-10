# Federation Execution Plan

> **Document Version:** 1.0  
> **Created:** February 2026  
> **Status:** Implementation Ready

## Executive Summary

This plan details the implementation of DVT's federation engine - the component that enables cross-database queries by using Spark as an intermediary execution layer. When a model references sources from different targets, DVT will intelligently decompose the query, push down what's possible to source databases, transform in Spark, and load to the target.

### Key Design Decisions

| Decision | Choice | Rationale |
|----------|--------|-----------|
| ETL Strategy | Smart Pushdown | Push as much as possible to source DBs for performance |
| Type Loss Handling | Warn + default to STRING | Don't fail, but inform users of precision loss |
| Function Translation | Best effort + warning | Translate what we can, warn about semantic differences |
| Parallel Extraction | Yes (parallel) | Extract from multiple sources concurrently |
| Missing Spark | Fail with clear error | "Run `dvt sync` to install Spark for cross-target models" |
| Predicate Pushdown | Enabled by default | Always try to push predicates for better performance |

---

## 1. Architecture Overview

```
                              USER COMMAND
                         (dvt run --select my_model)
                                   |
                                   v
+-------------------------------------------------------------------------+
|                         DVT CLI (dvt run)                                |
+------------------------------------+------------------------------------+
                                     |
                                     v
+-------------------------------------------------------------------------+
|                      RunTask.execute_nodes()                             |
|                              |                                           |
|                              v                                           |
|                      ModelRunner.execute()                               |
|                              |                                           |
|                              v                                           |
|  +-------------------------------------------------------------------+  |
|  |              FederationResolver.needs_federation()                 |  |
|  |  - Analyze model's upstream sources                                |  |
|  |  - Compare source targets vs model target                          |  |
|  |  - Return: True (federation) or False (adapter pushdown)           |  |
|  +-------------------------------+-----------------------------------+  |
|                                  |                                      |
|              +-------------------+-------------------+                   |
|              |                                       |                   |
|              v                                       v                   |
|  +-----------------------+      +---------------------------------------+
|  |  ADAPTER PUSHDOWN     |      |      FEDERATION PATH                  |
|  |  (existing dbt)       |      |      (new DVT code)                   |
|  |                       |      |                                       |
|  |  Execute via          |      |  +-------------------------------+   |
|  |  materialization      |      |  |  FederationPlanner             |   |
|  |  macro on target      |      |  |  - Query decomposition         |   |
|  |  adapter              |      |  |  - Pushdown analysis           |   |
|  |                       |      |  |  - Type inference              |   |
|  +-----------------------+      |  +---------------+---------------+   |
|                                 |                  |                    |
|                                 |                  v                    |
|                                 |  +-------------------------------+   |
|                                 |  |  DialectTranslator            |   |
|                                 |  |  (SQLGlot)                    |   |
|                                 |  |  - Snowflake -> Postgres      |   |
|                                 |  |  - Snowflake -> Spark         |   |
|                                 |  +---------------+---------------+   |
|                                 |                  |                    |
|                                 |                  v                    |
|                                 |  +-------------------------------+   |
|                                 |  |  SparkExecutor                |   |
|                                 |  |  - JDBC Extract               |   |
|                                 |  |  - Spark Transform            |   |
|                                 |  |  - JDBC Load                  |   |
|                                 |  +-------------------------------+   |
|                                 +---------------------------------------+
+-------------------------------------------------------------------------+
```

---

## 2. The Problem: Cross-Database Data Movement

### Scenario

User writes a model in Snowflake dialect, but the source lives in Postgres:

```sql
-- models/my_model.sql (target: snowflake)
SELECT
  user_id,
  PARSE_JSON(metadata):name::VARCHAR AS user_name,  -- Snowflake syntax!
  created_at::DATE AS signup_date
FROM {{ source('postgres_app', 'users') }}  -- Lives in Postgres!
```

### Challenge

We need THREE different SQL dialects:
1. **Extract Query (Postgres):** Pull data FROM Postgres INTO Spark
2. **Transform Query (Spark):** Transform data IN Spark
3. **Load Query (Snowflake):** Write results FROM Spark INTO Snowflake

---

## 3. Smart Pushdown Federation Flow

### Phase 1: Query Analysis & Classification

SQLGlot parses the AST and classifies each operation:

| OPERATION | PUSHDOWN? | REASON |
|-----------|-----------|--------|
| `SELECT u.user_id, u.email` | YES | Simple column select |
| `PARSE_JSON():preferences` | NO | Snowflake-specific function |
| `DATEDIFF('day', ...)` | NO | Dialect-specific syntax |
| `JOIN ... ON` (same source) | YES | Same source target |
| `WHERE created_at > '...'` | YES | Simple predicate |

**Decision:** Partial pushdown possible!
- Push: column selects, JOIN, WHERE predicates -> Source DB
- Keep: PARSE_JSON, DATEDIFF -> Spark

### Phase 2: Query Decomposition

```
+-----------------------------------------------------------------------+
|  Q1: EXTRACT QUERY (Pushed to Postgres)                               |
+-----------------------------------------------------------------------+
|  -- Generated by DVT, executed via JDBC on Postgres                   |
|  SELECT                                                               |
|    u.user_id,                                                         |
|    u.email,                                                           |
|    u.metadata,              -- Raw JSONB, transform later             |
|    u.created_at,            -- Need for DATEDIFF in Spark             |
|    o.total_amount                                                     |
|  FROM app.users u                                                     |
|  JOIN app.orders o ON u.user_id = o.user_id   <-- JOIN pushed!        |
|  WHERE u.created_at > DATE '2024-01-01'       <-- Predicate pushed!   |
|    AND o.status = 'completed'                  <-- Predicate pushed!   |
+-----------------------------------------------------------------------+

+-----------------------------------------------------------------------+
|  Q2: TRANSFORM QUERY (Executed in Spark)                              |
+-----------------------------------------------------------------------+
|  -- Only the non-pushable transformations                             |
|  SELECT                                                               |
|    user_id,                                                           |
|    email,                                                             |
|    GET_JSON_OBJECT(metadata, '$.preferences') AS prefs,  -- Spark!    |
|    total_amount,                                                      |
|    DATEDIFF(CURRENT_DATE(), created_at) AS days_since_signup          |
|  FROM __extracted_data__                                              |
+-----------------------------------------------------------------------+
```

### Phase 3: Type Inference & Mapping

| COLUMN | POSTGRES | SPARK | SNOWFLAKE | STATUS |
|--------|----------|-------|-----------|--------|
| user_id | INTEGER | IntegerType | INTEGER | OK |
| email | VARCHAR(255) | StringType | VARCHAR | WARN: length lost |
| prefs (derived) | JSONB-> | StringType | VARCHAR | OK |
| total_amount | NUMERIC(10,2) | DecimalType | NUMBER(10,2) | OK |
| days_since_signup | (computed) | IntegerType | INTEGER | OK |

### Phase 4: Execution

```python
# Step 1: Execute Extract on Postgres
spark.read.format("jdbc")
  .option("url", postgres_url)
  .option("dbtable", f"({extract_sql}) AS __src__")  # Pushdown!
  .option("fetchsize", "10000")
  .load()

# Step 2: Execute Transform in Spark
extracted_df.createOrReplaceTempView("__extracted_data__")
result_df = spark.sql(transform_sql)

# Step 3: Load to Snowflake
result_df.write.format("jdbc")
  .option("url", snowflake_url)
  .option("dbtable", "my_schema.my_model")
  .mode("overwrite")
  .save()
```

---

## 4. Type Mapping Chain

```
Postgres Type    ->    Spark Type       ->    Snowflake Type
==============         ==========             ==============

INTEGER          ->    IntegerType      ->    INTEGER
BIGINT           ->    LongType         ->    BIGINT
NUMERIC(p,s)     ->    DecimalType(p,s) ->    NUMBER(p,s)
VARCHAR(n)       ->    StringType       ->    VARCHAR         [!] Length lost!
TEXT             ->    StringType       ->    VARCHAR         (unbounded)
JSONB            ->    StringType       ->    VARIANT         [!] Needs cast!
TIMESTAMP        ->    TimestampType    ->    TIMESTAMP_NTZ
TIMESTAMPTZ      ->    TimestampType    ->    TIMESTAMP_LTZ   [!] TZ handling!
BOOLEAN          ->    BooleanType      ->    BOOLEAN
BYTEA            ->    BinaryType       ->    BINARY
UUID             ->    StringType       ->    VARCHAR         [!] Type lost!
ARRAY            ->    ArrayType        ->    ARRAY

[!] = Potential data loss or semantic change -> Warn and default to STRING
```

---

## 5. Module Structure

```
core/dvt/federation/
|-- __init__.py              # Public API exports
|-- resolver.py              # Federation decision logic (Rule 3)
|-- planner.py               # Query decomposition & pushdown analysis
|-- dialect.py               # SQLGlot-based dialect translation
|-- types.py                 # Cross-database type mapping
|-- spark.py                 # Spark session management
|-- jdbc.py                  # JDBC URL construction from adapters
|-- executor.py              # FederatedModelRunner
|-- warnings.py              # Federation-specific warnings (DVT001-DVT00x)
+-- exceptions.py            # Federation-specific exceptions
```

---

## 6. Implementation Details

### 6.1 Federation Resolver (`resolver.py`)

**Purpose:** Determine if a model requires federation based on source/model target analysis.

```python
from typing import Dict, Optional
from dvt.contracts.graph.manifest import Manifest
from dvt.contracts.graph.nodes import ModelNode
from dvt.config import RuntimeConfig


class FederationResolver:
    """
    Determines whether a model requires federation (cross-target execution)
    or can use adapter pushdown (same-target execution).
    
    Rule 3 from dvt_rules.md:
    - When all upstream models exist in same target -> adapter pushdown
    - When at least 1 upstream model exists in different target -> federation
    """
    
    def __init__(self, config: RuntimeConfig, manifest: Manifest):
        self.config = config
        self.manifest = manifest
        
    def needs_federation(self, model: ModelNode) -> bool:
        """
        Returns True if model's target differs from any upstream source's target.
        
        Algorithm:
        1. Get model's target from config (CLI > model config > profiles.yml default)
        2. Get all upstream nodes (refs + sources)
        3. For each source, get its target from `connection` property
        4. If any source target != model target, return True
        """
        model_target = self.get_model_target(model)
        source_targets = self.get_source_targets(model)
        
        for source_name, source_target in source_targets.items():
            if source_target != model_target:
                return True
        return False
        
    def get_model_target(self, model: ModelNode) -> str:
        """
        Returns the resolved target for the model.
        Priority: CLI --target > model config target > profiles.yml default
        """
        # CLI override (highest priority)
        if self.config.args and hasattr(self.config.args, 'target') and self.config.args.target:
            return self.config.args.target
            
        # Model config override
        if hasattr(model.config, 'target') and model.config.target:
            return model.config.target
            
        # Default from profiles.yml
        return self.config.target_name
        
    def get_source_targets(self, model: ModelNode) -> Dict[str, str]:
        """
        Returns mapping of source_name -> target_name for all upstream sources.
        Uses the `connection` property on sources (DVT extension).
        """
        source_targets = {}
        
        for dep in model.depends_on.nodes:
            node = self.manifest.nodes.get(dep) or self.manifest.sources.get(dep)
            
            if node is None:
                continue
                
            # For sources, check the connection property
            if hasattr(node, 'source_name'):
                source_def = self._get_source_definition(node.source_name)
                if source_def and source_def.connection:
                    source_targets[node.unique_id] = source_def.connection
                else:
                    # Default to profile's default target
                    source_targets[node.unique_id] = self.config.target_name
                    
            # For ref'd models, use their resolved target
            elif hasattr(node, 'config'):
                target = getattr(node.config, 'target', None) or self.config.target_name
                source_targets[node.unique_id] = target
                
        return source_targets
        
    def _get_source_definition(self, source_name: str):
        """Get the source definition from manifest."""
        for source in self.manifest.sources.values():
            if source.source_name == source_name:
                return source
        return None
```

---

### 6.2 JDBC URL Builder (`jdbc.py`)

**Purpose:** Construct JDBC URLs from adapter credentials.

```python
from typing import Dict, Optional
from pathlib import Path


# JDBC URL patterns per adapter type
JDBC_URL_PATTERNS = {
    "postgres": "jdbc:postgresql://{host}:{port}/{database}",
    "redshift": "jdbc:redshift://{host}:{port}/{database}",
    "snowflake": "jdbc:snowflake://{account}.snowflakecomputing.com/?db={database}&schema={schema}&warehouse={warehouse}",
    "mysql": "jdbc:mysql://{host}:{port}/{database}",
    "oracle": "jdbc:oracle:thin:@//{host}:{port}/{database}",
    "sqlserver": "jdbc:sqlserver://{host}:{port};databaseName={database}",
    "databricks": "jdbc:databricks://{host}:{port}/default;transportMode=http;ssl=1;httpPath={http_path}",
    "trino": "jdbc:trino://{host}:{port}/{catalog}/{schema}",
    "duckdb": "jdbc:duckdb:{path}",
    "clickhouse": "jdbc:clickhouse://{host}:{port}/{database}",
    "bigquery": "jdbc:bigquery://https://www.googleapis.com/bigquery/v2:443;ProjectId={project};OAuthType=0",
}

# JDBC driver class names per adapter type
JDBC_DRIVER_CLASSES = {
    "postgres": "org.postgresql.Driver",
    "redshift": "com.amazon.redshift.jdbc42.Driver",
    "snowflake": "net.snowflake.client.jdbc.SnowflakeDriver",
    "mysql": "com.mysql.cj.jdbc.Driver",
    "oracle": "oracle.jdbc.OracleDriver",
    "sqlserver": "com.microsoft.sqlserver.jdbc.SQLServerDriver",
    "databricks": "com.databricks.client.jdbc.Driver",
    "trino": "io.trino.jdbc.TrinoDriver",
    "duckdb": "org.duckdb.DuckDBDriver",
    "clickhouse": "com.clickhouse.jdbc.ClickHouseDriver",
}

# Default ports per adapter type
DEFAULT_PORTS = {
    "postgres": 5432,
    "redshift": 5439,
    "mysql": 3306,
    "oracle": 1521,
    "sqlserver": 1433,
    "trino": 8080,
    "clickhouse": 8123,
}


class JDBCBuilder:
    """
    Builds JDBC URLs and connection properties from DVT adapter credentials.
    """
    
    def build_jdbc_url(self, adapter_type: str, credentials: dict) -> str:
        """
        Build JDBC URL from adapter credentials.
        
        Args:
            adapter_type: The dbt adapter type (postgres, snowflake, etc.)
            credentials: The credentials dict from profiles.yml
            
        Returns:
            JDBC connection URL string
        """
        pattern = JDBC_URL_PATTERNS.get(adapter_type)
        if not pattern:
            raise ValueError(f"No JDBC URL pattern for adapter type: {adapter_type}")
            
        # Add default port if not specified
        if 'port' not in credentials and adapter_type in DEFAULT_PORTS:
            credentials = {**credentials, 'port': DEFAULT_PORTS[adapter_type]}
            
        try:
            return pattern.format(**credentials)
        except KeyError as e:
            raise ValueError(f"Missing credential for JDBC URL: {e}")
            
    def get_jdbc_properties(self, adapter_type: str, credentials: dict) -> dict:
        """
        Get JDBC connection properties (user, password, etc.).
        
        Returns dict suitable for Spark JDBC options.
        """
        props = {}
        
        # Standard properties
        if 'user' in credentials:
            props['user'] = credentials['user']
        if 'password' in credentials:
            props['password'] = credentials['password']
            
        # Driver class
        driver = JDBC_DRIVER_CLASSES.get(adapter_type)
        if driver:
            props['driver'] = driver
            
        # Adapter-specific properties
        if adapter_type == 'snowflake':
            if 'role' in credentials:
                props['role'] = credentials['role']
                
        return props
        
    def get_jdbc_driver_class(self, adapter_type: str) -> str:
        """Get the JDBC driver class name for Spark."""
        driver = JDBC_DRIVER_CLASSES.get(adapter_type)
        if not driver:
            raise ValueError(f"No JDBC driver class for adapter type: {adapter_type}")
        return driver
```

---

### 6.3 Type Mapping System (`types.py`)

**Purpose:** Map types across database dialects.

```python
from dataclasses import dataclass
from typing import Dict, List, Optional, Tuple


@dataclass
class TypeMapping:
    """Represents a type mapping between dialects."""
    source_type: str
    spark_type: str
    target_type: str
    precision_loss: bool
    warning_message: Optional[str]


class TypeMapper:
    """
    Maps data types across database dialects.
    
    Strategy:
    - Map source type -> Spark type -> target type
    - Track precision loss and emit warnings
    - Fall back to STRING for unknown types
    """
    
    # Canonical type mappings: (source_dialect, target_dialect) -> {source_type: TypeMapping}
    TYPE_CHAINS: Dict[Tuple[str, str], Dict[str, TypeMapping]] = {
        ("postgres", "spark"): {
            "INTEGER": TypeMapping("INTEGER", "IntegerType", "IntegerType", False, None),
            "INT": TypeMapping("INT", "IntegerType", "IntegerType", False, None),
            "BIGINT": TypeMapping("BIGINT", "LongType", "LongType", False, None),
            "SMALLINT": TypeMapping("SMALLINT", "ShortType", "ShortType", False, None),
            "NUMERIC": TypeMapping("NUMERIC", "DecimalType", "DecimalType", False, None),
            "DECIMAL": TypeMapping("DECIMAL", "DecimalType", "DecimalType", False, None),
            "REAL": TypeMapping("REAL", "FloatType", "FloatType", False, None),
            "DOUBLE PRECISION": TypeMapping("DOUBLE PRECISION", "DoubleType", "DoubleType", False, None),
            "VARCHAR": TypeMapping("VARCHAR", "StringType", "StringType", True, 
                "VARCHAR length constraint lost through Spark StringType"),
            "CHAR": TypeMapping("CHAR", "StringType", "StringType", True,
                "CHAR length constraint lost through Spark StringType"),
            "TEXT": TypeMapping("TEXT", "StringType", "StringType", False, None),
            "BOOLEAN": TypeMapping("BOOLEAN", "BooleanType", "BooleanType", False, None),
            "DATE": TypeMapping("DATE", "DateType", "DateType", False, None),
            "TIMESTAMP": TypeMapping("TIMESTAMP", "TimestampType", "TimestampType", False, None),
            "TIMESTAMPTZ": TypeMapping("TIMESTAMPTZ", "TimestampType", "TimestampType", True,
                "Timezone information may be handled differently in Spark"),
            "TIME": TypeMapping("TIME", "StringType", "StringType", True,
                "TIME type stored as StringType in Spark"),
            "BYTEA": TypeMapping("BYTEA", "BinaryType", "BinaryType", False, None),
            "JSON": TypeMapping("JSON", "StringType", "StringType", True,
                "JSON stored as StringType, use from_json() for parsing"),
            "JSONB": TypeMapping("JSONB", "StringType", "StringType", True,
                "JSONB stored as StringType, use from_json() for parsing"),
            "UUID": TypeMapping("UUID", "StringType", "StringType", True,
                "UUID type stored as StringType"),
            "ARRAY": TypeMapping("ARRAY", "ArrayType", "ArrayType", False, None),
        },
        ("spark", "snowflake"): {
            "IntegerType": TypeMapping("IntegerType", "IntegerType", "INTEGER", False, None),
            "LongType": TypeMapping("LongType", "LongType", "BIGINT", False, None),
            "ShortType": TypeMapping("ShortType", "ShortType", "SMALLINT", False, None),
            "DecimalType": TypeMapping("DecimalType", "DecimalType", "NUMBER", False, None),
            "FloatType": TypeMapping("FloatType", "FloatType", "FLOAT", False, None),
            "DoubleType": TypeMapping("DoubleType", "DoubleType", "DOUBLE", False, None),
            "StringType": TypeMapping("StringType", "StringType", "VARCHAR", False, None),
            "BooleanType": TypeMapping("BooleanType", "BooleanType", "BOOLEAN", False, None),
            "DateType": TypeMapping("DateType", "DateType", "DATE", False, None),
            "TimestampType": TypeMapping("TimestampType", "TimestampType", "TIMESTAMP_NTZ", False, None),
            "BinaryType": TypeMapping("BinaryType", "BinaryType", "BINARY", False, None),
            "ArrayType": TypeMapping("ArrayType", "ArrayType", "ARRAY", False, None),
        },
        ("spark", "postgres"): {
            "IntegerType": TypeMapping("IntegerType", "IntegerType", "INTEGER", False, None),
            "LongType": TypeMapping("LongType", "LongType", "BIGINT", False, None),
            "ShortType": TypeMapping("ShortType", "ShortType", "SMALLINT", False, None),
            "DecimalType": TypeMapping("DecimalType", "DecimalType", "NUMERIC", False, None),
            "FloatType": TypeMapping("FloatType", "FloatType", "REAL", False, None),
            "DoubleType": TypeMapping("DoubleType", "DoubleType", "DOUBLE PRECISION", False, None),
            "StringType": TypeMapping("StringType", "StringType", "TEXT", False, None),
            "BooleanType": TypeMapping("BooleanType", "BooleanType", "BOOLEAN", False, None),
            "DateType": TypeMapping("DateType", "DateType", "DATE", False, None),
            "TimestampType": TypeMapping("TimestampType", "TimestampType", "TIMESTAMP", False, None),
            "BinaryType": TypeMapping("BinaryType", "BinaryType", "BYTEA", False, None),
            "ArrayType": TypeMapping("ArrayType", "ArrayType", "ARRAY", False, None),
        },
        # Add more dialect pairs as needed...
    }
    
    def map_type(self, source_dialect: str, target_dialect: str, 
                 source_type: str) -> Tuple[str, List[str]]:
        """
        Map a type from source to target dialect.
        
        Args:
            source_dialect: Source database dialect (e.g., "postgres")
            target_dialect: Target database dialect (e.g., "snowflake")
            source_type: The source type to map
            
        Returns:
            Tuple of (target_type, list of warning messages)
            Falls back to STRING/VARCHAR for unknown types.
        """
        warnings = []
        
        # Normalize type name (remove precision/scale for lookup)
        base_type = source_type.split('(')[0].upper().strip()
        
        # Try direct mapping
        key = (source_dialect.lower(), target_dialect.lower())
        if key in self.TYPE_CHAINS:
            mapping = self.TYPE_CHAINS[key].get(base_type)
            if mapping:
                if mapping.precision_loss and mapping.warning_message:
                    warnings.append(mapping.warning_message)
                return mapping.target_type, warnings
                
        # Try via Spark (source -> spark -> target)
        source_to_spark = (source_dialect.lower(), "spark")
        spark_to_target = ("spark", target_dialect.lower())
        
        if source_to_spark in self.TYPE_CHAINS:
            spark_mapping = self.TYPE_CHAINS[source_to_spark].get(base_type)
            if spark_mapping:
                if spark_mapping.precision_loss and spark_mapping.warning_message:
                    warnings.append(spark_mapping.warning_message)
                    
                if spark_to_target in self.TYPE_CHAINS:
                    target_mapping = self.TYPE_CHAINS[spark_to_target].get(spark_mapping.spark_type)
                    if target_mapping:
                        if target_mapping.precision_loss and target_mapping.warning_message:
                            warnings.append(target_mapping.warning_message)
                        return target_mapping.target_type, warnings
        
        # Fallback to STRING
        warnings.append(f"Unknown type '{source_type}' - defaulting to STRING/VARCHAR")
        if target_dialect.lower() == "snowflake":
            return "VARCHAR", warnings
        elif target_dialect.lower() == "postgres":
            return "TEXT", warnings
        else:
            return "STRING", warnings
            
    def infer_output_schema(self, columns: List[dict], 
                           source_dialect: str, 
                           target_dialect: str) -> Tuple[List[dict], List[str]]:
        """
        Infer the output schema with type mappings.
        
        Args:
            columns: List of {"name": str, "type": str} dicts
            source_dialect: Source dialect
            target_dialect: Target dialect
            
        Returns:
            Tuple of (mapped columns list, all warnings)
        """
        all_warnings = []
        mapped_columns = []
        
        for col in columns:
            target_type, warnings = self.map_type(
                source_dialect, target_dialect, col['type']
            )
            for w in warnings:
                all_warnings.append(f"Column '{col['name']}': {w}")
                
            mapped_columns.append({
                'name': col['name'],
                'type': target_type,
                'original_type': col['type']
            })
            
        return mapped_columns, all_warnings
```

---

### 6.4 Federation Warnings (`warnings.py`)

**Purpose:** Define and emit federation-specific warnings.

```python
from dataclasses import dataclass, field
from typing import List, Optional
from enum import Enum

from dvt.events.types import Note
from dbt_common.events.functions import fire_event


class FederationWarningCode(Enum):
    """Federation warning codes."""
    DVT001 = "DVT001"  # Materialization coerced
    DVT002 = "DVT002"  # Type precision loss
    DVT003 = "DVT003"  # Function translation (best effort)
    DVT004 = "DVT004"  # JSON handling differences
    DVT005 = "DVT005"  # Predicate not pushed down
    DVT006 = "DVT006"  # Unsupported function
    DVT007 = "DVT007"  # Cross-source join


@dataclass
class FederationWarning:
    """A single federation warning."""
    code: FederationWarningCode
    message: str
    details: Optional[str] = None
    node_id: Optional[str] = None
    
    def __str__(self):
        s = f"{self.code.value}: {self.message}"
        if self.details:
            s += f"\n    {self.details}"
        return s


class FederationWarningCollector:
    """
    Collects and emits federation-specific warnings.
    
    Usage:
        collector = FederationWarningCollector()
        collector.warn_materialization_coerced(node_id, "view", "table")
        collector.warn_type_loss(node_id, "email", "VARCHAR(255)", "VARCHAR")
        collector.emit_all()  # Fire as DVT events
    """
    
    def __init__(self):
        self._warnings: List[FederationWarning] = []
        
    @property
    def warnings(self) -> List[FederationWarning]:
        return self._warnings.copy()
        
    def add(self, warning: FederationWarning):
        """Add a warning to the collector."""
        self._warnings.append(warning)
        
    def warn_materialization_coerced(self, node_id: str, from_mat: str, to_mat: str):
        """DVT001: View/ephemeral coerced to table on federation path."""
        self.add(FederationWarning(
            code=FederationWarningCode.DVT001,
            message=f"Materialization coerced from '{from_mat}' to '{to_mat}'",
            details=f"Cross-target models cannot be materialized as {from_mat}. "
                    f"Automatically using {to_mat} materialization.",
            node_id=node_id
        ))
        
    def warn_type_loss(self, node_id: str, column: str, from_type: str, to_type: str, 
                       details: Optional[str] = None):
        """DVT002: Type precision loss during federation."""
        self.add(FederationWarning(
            code=FederationWarningCode.DVT002,
            message=f"Type precision loss: {column} ({from_type} -> {to_type})",
            details=details,
            node_id=node_id
        ))
        
    def warn_function_translation(self, node_id: str, original: str, translated: str,
                                  semantic_note: Optional[str] = None):
        """DVT003: Function translated with potential semantic differences."""
        details = f"'{original}' translated to '{translated}'"
        if semantic_note:
            details += f". Note: {semantic_note}"
        self.add(FederationWarning(
            code=FederationWarningCode.DVT003,
            message="Function translated (best effort)",
            details=details,
            node_id=node_id
        ))
        
    def warn_json_handling(self, node_id: str, operation: str):
        """DVT004: JSON handling differences between dialects."""
        self.add(FederationWarning(
            code=FederationWarningCode.DVT004,
            message="JSON handling may differ between dialects",
            details=f"Operation: {operation}. NULL handling for missing keys may differ.",
            node_id=node_id
        ))
        
    def warn_predicate_not_pushed(self, node_id: str, predicate: str, reason: str):
        """DVT005: Predicate could not be pushed to source."""
        self.add(FederationWarning(
            code=FederationWarningCode.DVT005,
            message="Predicate not pushed to source",
            details=f"'{predicate}' executed in Spark. Reason: {reason}",
            node_id=node_id
        ))
        
    def warn_unsupported_function(self, node_id: str, function: str, dialect: str):
        """DVT006: Function not supported in target dialect."""
        self.add(FederationWarning(
            code=FederationWarningCode.DVT006,
            message=f"Function '{function}' not supported in {dialect}",
            details="Function will be executed in Spark instead.",
            node_id=node_id
        ))
        
    def warn_cross_source_join(self, node_id: str, sources: List[str]):
        """DVT007: Join spans multiple sources (executed in Spark)."""
        self.add(FederationWarning(
            code=FederationWarningCode.DVT007,
            message=f"Cross-source JOIN will be executed in Spark",
            details=f"Sources: {', '.join(sources)}. Data from all sources will be "
                    f"transferred to Spark for join execution.",
            node_id=node_id
        ))
        
    def emit_all(self):
        """Fire all collected warnings as DVT events."""
        for warning in self._warnings:
            fire_event(Note(msg=str(warning)))
            
    def get_summary(self) -> str:
        """Get a summary of all warnings."""
        if not self._warnings:
            return "No federation warnings."
            
        by_code = {}
        for w in self._warnings:
            if w.code not in by_code:
                by_code[w.code] = []
            by_code[w.code].append(w)
            
        lines = [f"Federation completed with {len(self._warnings)} warning(s):"]
        for code, warnings in by_code.items():
            lines.append(f"  {code.value}: {len(warnings)} occurrence(s)")
            
        return "\n".join(lines)
```

---

### 6.5 Dialect Translator (`dialect.py`)

**Purpose:** Translate SQL between dialects using SQLGlot.

```python
from typing import Dict, List, Optional, Set
import sqlglot
from sqlglot import parse_one, exp
from sqlglot.optimizer.scope import build_scope
from sqlglot.errors import ParseError, UnsupportedError

from dvt.federation.warnings import FederationWarningCollector


# SQLGlot dialect names mapped to DVT adapter types
ADAPTER_TO_SQLGLOT: Dict[str, str] = {
    "postgres": "postgres",
    "snowflake": "snowflake",
    "mysql": "mysql",
    "redshift": "redshift",
    "bigquery": "bigquery",
    "databricks": "databricks",
    "spark": "spark",
    "trino": "trino",
    "duckdb": "duckdb",
    "clickhouse": "clickhouse",
    "oracle": "oracle",
    "sqlserver": "tsql",
    "hive": "hive",
}


class DialectTranslator:
    """
    Translates SQL between dialects using SQLGlot.
    
    Features:
    - Parse SQL in source dialect
    - Transpile to target dialect
    - Extract metadata (tables, columns, predicates)
    - Track translation warnings
    """
    
    def translate(self, sql: str, from_dialect: str, to_dialect: str,
                  warning_collector: Optional[FederationWarningCollector] = None,
                  node_id: Optional[str] = None) -> str:
        """
        Translate SQL from one dialect to another.
        
        Args:
            sql: SQL string to translate
            from_dialect: Source dialect (DVT adapter type)
            to_dialect: Target dialect (DVT adapter type)
            warning_collector: Optional collector for translation warnings
            node_id: Optional node ID for warning context
            
        Returns:
            Translated SQL string
        """
        from_sqlglot = ADAPTER_TO_SQLGLOT.get(from_dialect.lower(), from_dialect.lower())
        to_sqlglot = ADAPTER_TO_SQLGLOT.get(to_dialect.lower(), to_dialect.lower())
        
        try:
            result = sqlglot.transpile(
                sql,
                read=from_sqlglot,
                write=to_sqlglot,
                pretty=True,
                unsupported_level=sqlglot.ErrorLevel.WARN
            )
            return result[0] if result else sql
        except UnsupportedError as e:
            if warning_collector:
                warning_collector.warn_function_translation(
                    node_id or "unknown",
                    str(e),
                    "best-effort translation applied"
                )
            # Try with IGNORE level to get best-effort result
            try:
                result = sqlglot.transpile(
                    sql,
                    read=from_sqlglot,
                    write=to_sqlglot,
                    pretty=True,
                    unsupported_level=sqlglot.ErrorLevel.IGNORE
                )
                return result[0] if result else sql
            except Exception:
                return sql
        except ParseError as e:
            raise ValueError(f"Failed to parse SQL in {from_dialect} dialect: {e}")
            
    def parse(self, sql: str, dialect: str) -> exp.Expression:
        """
        Parse SQL into AST.
        
        Args:
            sql: SQL string
            dialect: Dialect to parse as
            
        Returns:
            SQLGlot Expression AST
        """
        sqlglot_dialect = ADAPTER_TO_SQLGLOT.get(dialect.lower(), dialect.lower())
        return parse_one(sql, dialect=sqlglot_dialect)
        
    def generate(self, ast: exp.Expression, dialect: str, pretty: bool = True) -> str:
        """
        Generate SQL from AST.
        
        Args:
            ast: SQLGlot Expression AST
            dialect: Target dialect
            pretty: Whether to format output
            
        Returns:
            SQL string
        """
        sqlglot_dialect = ADAPTER_TO_SQLGLOT.get(dialect.lower(), dialect.lower())
        return ast.sql(dialect=sqlglot_dialect, pretty=pretty)
        
    def extract_tables(self, sql: str, dialect: str) -> List[str]:
        """
        Extract all table references from SQL.
        
        Uses scope analysis to distinguish real tables from CTEs.
        """
        ast = self.parse(sql, dialect)
        root = build_scope(ast)
        
        if not root:
            # Fallback to simple find_all
            return [str(t.this) for t in ast.find_all(exp.Table)]
            
        tables = []
        for scope in root.traverse():
            for alias, (node, source) in scope.selected_sources.items():
                if isinstance(source, exp.Table):
                    tables.append(source.sql())
        return tables
        
    def extract_columns(self, sql: str, dialect: str) -> List[str]:
        """Extract all column references from SQL."""
        ast = self.parse(sql, dialect)
        return [col.alias_or_name for col in ast.find_all(exp.Column)]
        
    def extract_predicates(self, sql: str, dialect: str) -> List[exp.Expression]:
        """
        Extract WHERE predicates from SQL.
        
        Returns list of individual predicate expressions (AND-clauses split).
        """
        ast = self.parse(sql, dialect)
        predicates = []
        
        for select in ast.find_all(exp.Select):
            where = select.args.get("where")
            if where:
                condition = where.this
                if isinstance(condition, exp.And):
                    predicates.extend(condition.flatten())
                else:
                    predicates.append(condition)
                    
        return predicates
        
    def get_predicate_tables(self, predicate: exp.Expression) -> Set[str]:
        """Get the set of tables referenced in a predicate."""
        return exp.column_table_names(predicate)
        
    def is_single_source_predicate(self, predicate: exp.Expression) -> bool:
        """Check if predicate references only one source (pushdown candidate)."""
        tables = self.get_predicate_tables(predicate)
        return len(tables) <= 1
```

---

### 6.6 Federation Planner (`planner.py`)

**Purpose:** Decompose queries and analyze pushdown opportunities.

```python
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Set
from sqlglot import exp

from dvt.contracts.graph.manifest import Manifest
from dvt.contracts.graph.nodes import ModelNode
from dvt.config import RuntimeConfig
from dvt.federation.dialect import DialectTranslator
from dvt.federation.types import TypeMapper
from dvt.federation.warnings import FederationWarning, FederationWarningCollector


@dataclass
class ExtractQuery:
    """Query to execute on a source database."""
    source_name: str
    source_id: str
    target: str
    dialect: str
    sql: str
    columns: List[str]
    pushed_predicates: List[str] = field(default_factory=list)


@dataclass
class TransformQuery:
    """Query to execute in Spark."""
    sql: str
    source_views: Dict[str, str]  # view_name -> source_id


@dataclass
class FederationPlan:
    """Complete execution plan for a federated model."""
    model_id: str
    model_target: str
    model_dialect: str
    extract_queries: List[ExtractQuery]
    transform_query: TransformQuery
    output_columns: List[dict]  # column definitions for target
    warnings: List[FederationWarning] = field(default_factory=list)
    
    def get_involved_targets(self) -> Set[str]:
        """Get all targets involved in this plan."""
        targets = {self.model_target}
        for eq in self.extract_queries:
            targets.add(eq.target)
        return targets


class FederationPlanner:
    """
    Creates execution plans for federated models.
    
    Responsibilities:
    - Analyze model SQL to identify sources
    - Determine which operations can be pushed to sources
    - Generate extract queries (source dialects)
    - Generate transform query (Spark dialect)
    - Infer output schema
    """
    
    def __init__(self, translator: DialectTranslator, type_mapper: TypeMapper):
        self.translator = translator
        self.type_mapper = type_mapper
        
    def plan(self, model: ModelNode, manifest: Manifest, 
             config: RuntimeConfig,
             source_targets: Dict[str, str],
             model_target: str,
             warning_collector: FederationWarningCollector) -> FederationPlan:
        """
        Create a federation execution plan for the model.
        
        Steps:
        1. Get compiled SQL (already has refs/sources resolved to table names)
        2. Parse in model's target dialect
        3. Identify all source references and their targets
        4. Analyze pushdown opportunities
        5. Generate extract queries (one per source target)
        6. Generate transform query (Spark)
        7. Infer output schema
        """
        # Get model's target dialect (adapter type)
        model_dialect = self._get_adapter_type(model_target, config)
        
        # Get compiled SQL
        compiled_sql = model.compiled_code
        if not compiled_sql:
            raise ValueError(f"Model {model.unique_id} has no compiled SQL")
            
        # Parse the SQL
        ast = self.translator.parse(compiled_sql, model_dialect)
        
        # Analyze predicates for pushdown
        predicates = self.translator.extract_predicates(compiled_sql, model_dialect)
        pushdown_map = self._analyze_pushdown(predicates, source_targets, warning_collector, model.unique_id)
        
        # Build extract queries
        extract_queries = self._build_extract_queries(
            ast, source_targets, pushdown_map, config, warning_collector, model.unique_id
        )
        
        # Build transform query
        transform_query = self._build_transform_query(
            ast, extract_queries, model_dialect, warning_collector, model.unique_id
        )
        
        # Infer output columns (simplified - would need catalog for full implementation)
        output_columns = self._infer_output_columns(ast, model_dialect, model_target)
        
        return FederationPlan(
            model_id=model.unique_id,
            model_target=model_target,
            model_dialect=model_dialect,
            extract_queries=extract_queries,
            transform_query=transform_query,
            output_columns=output_columns,
            warnings=warning_collector.warnings
        )
        
    def _get_adapter_type(self, target: str, config: RuntimeConfig) -> str:
        """Get the adapter type for a target from profiles."""
        # In real implementation, look up from config.credentials
        # For now, return a default
        return getattr(config.credentials, 'type', 'postgres')
        
    def _analyze_pushdown(self, predicates: List[exp.Expression],
                          source_targets: Dict[str, str],
                          warning_collector: FederationWarningCollector,
                          node_id: str) -> Dict[str, List[exp.Expression]]:
        """
        Analyze which predicates can be pushed to which sources.
        
        Returns: {source_id: [pushable predicates]}
        """
        pushdown_map: Dict[str, List[exp.Expression]] = {}
        
        for pred in predicates:
            tables = self.translator.get_predicate_tables(pred)
            
            if len(tables) == 0:
                # Constant predicate (e.g., 1=1), can push anywhere
                continue
            elif len(tables) == 1:
                # Single-table predicate - can push to that source
                table = list(tables)[0]
                # Find source_id for this table
                source_id = self._find_source_for_table(table, source_targets)
                if source_id:
                    if source_id not in pushdown_map:
                        pushdown_map[source_id] = []
                    pushdown_map[source_id].append(pred)
            else:
                # Multi-table predicate - cannot push, warn
                warning_collector.warn_predicate_not_pushed(
                    node_id,
                    pred.sql(),
                    f"Predicate spans multiple tables: {tables}"
                )
                
        return pushdown_map
        
    def _find_source_for_table(self, table_name: str, 
                               source_targets: Dict[str, str]) -> Optional[str]:
        """Find the source_id for a given table name."""
        # Simplified matching - in real implementation would use manifest
        for source_id in source_targets:
            if table_name.lower() in source_id.lower():
                return source_id
        return None
        
    def _build_extract_queries(self, ast: exp.Expression,
                               source_targets: Dict[str, str],
                               pushdown_map: Dict[str, List[exp.Expression]],
                               config: RuntimeConfig,
                               warning_collector: FederationWarningCollector,
                               node_id: str) -> List[ExtractQuery]:
        """Build extract queries for each source."""
        extract_queries = []
        
        # Group sources by target (same-target sources can share extract)
        by_target: Dict[str, List[str]] = {}
        for source_id, target in source_targets.items():
            if target not in by_target:
                by_target[target] = []
            by_target[target].append(source_id)
            
        for target, source_ids in by_target.items():
            source_dialect = self._get_adapter_type(target, config)
            
            # Build SELECT with pushable predicates
            # Simplified: SELECT * FROM source WHERE pushed_predicates
            for source_id in source_ids:
                # Get table name from source_id
                table_name = source_id.split('.')[-1]  # Simplified
                
                # Get pushed predicates for this source
                pushed_preds = pushdown_map.get(source_id, [])
                
                # Build extract SQL
                if pushed_preds:
                    where_clause = " AND ".join(p.sql() for p in pushed_preds)
                    extract_sql = f"SELECT * FROM {table_name} WHERE {where_clause}"
                else:
                    extract_sql = f"SELECT * FROM {table_name}"
                    
                # Translate to source dialect
                extract_sql = self.translator.translate(
                    extract_sql, source_dialect, source_dialect,  # No translation needed
                    warning_collector, node_id
                )
                
                extract_queries.append(ExtractQuery(
                    source_name=table_name,
                    source_id=source_id,
                    target=target,
                    dialect=source_dialect,
                    sql=extract_sql,
                    columns=["*"],  # Would extract actual columns in real implementation
                    pushed_predicates=[p.sql() for p in pushed_preds]
                ))
                
        return extract_queries
        
    def _build_transform_query(self, original_ast: exp.Expression,
                               extract_queries: List[ExtractQuery],
                               model_dialect: str,
                               warning_collector: FederationWarningCollector,
                               node_id: str) -> TransformQuery:
        """
        Build the Spark transform query.
        
        Replaces source references with temp view names,
        translates functions to Spark equivalents.
        """
        # Build view mapping
        source_views = {}
        for i, eq in enumerate(extract_queries):
            view_name = f"__source_{eq.source_name}__"
            source_views[view_name] = eq.source_id
            
        # Get the original SQL and translate to Spark
        original_sql = original_ast.sql()
        
        # Replace table references with view names
        spark_sql = original_sql
        for eq in extract_queries:
            view_name = f"__source_{eq.source_name}__"
            # Simple replacement - real implementation would use AST transform
            spark_sql = spark_sql.replace(eq.source_name, view_name)
            
        # Translate to Spark dialect
        spark_sql = self.translator.translate(
            spark_sql, model_dialect, "spark",
            warning_collector, node_id
        )
        
        return TransformQuery(
            sql=spark_sql,
            source_views=source_views
        )
        
    def _infer_output_columns(self, ast: exp.Expression, 
                             source_dialect: str,
                             target_dialect: str) -> List[dict]:
        """Infer output columns from the SELECT clause."""
        columns = []
        
        for select in ast.find_all(exp.Select):
            for expr in select.expressions:
                col_name = expr.alias_or_name
                # Type inference would require catalog lookup
                # For now, default to VARCHAR
                columns.append({
                    'name': col_name,
                    'type': 'VARCHAR',
                    'original_type': 'UNKNOWN'
                })
                
        return columns
```

---

### 6.7 Spark Session Manager (`spark.py`)

**Purpose:** Manage Spark session lifecycle and configuration.

```python
import threading
from pathlib import Path
from typing import Dict, Optional

# PySpark import is deferred to runtime to avoid import errors
# when pyspark is not installed


class SparkNotInstalledError(Exception):
    """Raised when Spark/PySpark is not installed."""
    def __init__(self):
        super().__init__(
            "PySpark is not installed. Run 'dvt sync' to install Spark "
            "for cross-target model execution."
        )


class SparkSessionManager:
    """
    Manages Spark session lifecycle and configuration.
    
    Configuration is loaded from computes.yml:
    
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
            spark.sql.adaptive.enabled: "true"
    ```
    """
    
    _instance: Optional["SparkSession"] = None  # type: ignore
    _lock = threading.Lock()
    
    def __init__(self, computes_config: dict, jdbc_jars_dir: Path):
        """
        Initialize SparkSessionManager.
        
        Args:
            computes_config: The 'computes' section from computes.yml
            jdbc_jars_dir: Path to JDBC JAR files (~/.dvt/.jdbc_jars/)
        """
        self.computes_config = computes_config
        self.jdbc_jars_dir = Path(jdbc_jars_dir)
        
    def _check_pyspark_installed(self):
        """Check if PySpark is installed, raise helpful error if not."""
        try:
            import pyspark
            return pyspark
        except ImportError:
            raise SparkNotInstalledError()
            
    def get_or_create(self, compute_name: str = "default") -> "SparkSession":  # type: ignore
        """
        Get or create a Spark session configured from computes.yml.
        
        Args:
            compute_name: Name of compute configuration to use
            
        Returns:
            Configured SparkSession
            
        Raises:
            SparkNotInstalledError: If PySpark is not installed
            ValueError: If compute configuration not found
        """
        pyspark = self._check_pyspark_installed()
        from pyspark.sql import SparkSession
        
        with self._lock:
            if self._instance is None:
                # Get compute configuration
                compute = self.computes_config.get(compute_name)
                if not compute:
                    # Use defaults
                    compute = {
                        "type": "spark",
                        "master": "local[*]",
                        "config": {}
                    }
                    
                # Build Spark session
                builder = SparkSession.builder \
                    .appName(f"DVT-Federation-{compute_name}") \
                    .master(compute.get("master", "local[*]"))
                
                # Add JDBC JARs
                if self.jdbc_jars_dir.exists():
                    jars = list(self.jdbc_jars_dir.glob("*.jar"))
                    if jars:
                        jar_paths = ",".join(str(j) for j in jars)
                        builder = builder.config("spark.jars", jar_paths)
                        
                # Add custom configs from computes.yml
                for key, value in compute.get("config", {}).items():
                    builder = builder.config(key, str(value))
                    
                # Standard DVT configs
                builder = builder \
                    .config("spark.sql.adaptive.enabled", "true") \
                    .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
                    
                self._instance = builder.getOrCreate()
                
            return self._instance
            
    def stop(self):
        """Stop the Spark session."""
        with self._lock:
            if self._instance:
                self._instance.stop()
                self._instance = None
                
    @property
    def is_active(self) -> bool:
        """Check if Spark session is active."""
        return self._instance is not None
```

---

### 6.8 Spark Executor (`executor.py`)

**Purpose:** Execute federation plans via Spark.

```python
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Dict, List, Optional

from dvt.contracts.graph.manifest import Manifest
from dvt.contracts.graph.nodes import ModelNode
from dvt.config import RuntimeConfig
from dvt.artifacts.schemas.run import RunResult
from dvt.artifacts.schemas.results import RunStatus
from dvt.task.run import ModelRunner

from dvt.federation.resolver import FederationResolver
from dvt.federation.planner import FederationPlanner, FederationPlan, ExtractQuery
from dvt.federation.dialect import DialectTranslator
from dvt.federation.types import TypeMapper
from dvt.federation.jdbc import JDBCBuilder
from dvt.federation.spark import SparkSessionManager, SparkNotInstalledError
from dvt.federation.warnings import FederationWarningCollector


class SparkExecutor:
    """
    Executes federation plans via Spark.
    
    Responsibilities:
    - Execute extract queries via JDBC (parallel)
    - Execute transform query in Spark
    - Write results to target via JDBC
    """
    
    def __init__(self, spark_manager: SparkSessionManager, 
                 jdbc_builder: JDBCBuilder,
                 max_parallel_extracts: int = 4):
        self.spark_manager = spark_manager
        self.jdbc_builder = jdbc_builder
        self.max_parallel_extracts = max_parallel_extracts
        
    def execute_plan(self, plan: FederationPlan, 
                     credentials: Dict[str, dict]) -> "DataFrame":  # type: ignore
        """
        Execute a federation plan.
        
        Args:
            plan: The federation execution plan
            credentials: Dict of target_name -> credentials dict
            
        Returns:
            Spark DataFrame with results
        """
        spark = self.spark_manager.get_or_create()
        
        # Execute extracts in parallel
        with ThreadPoolExecutor(max_workers=self.max_parallel_extracts) as executor:
            futures = {
                executor.submit(
                    self._execute_extract, spark, extract, credentials[extract.target]
                ): extract
                for extract in plan.extract_queries
            }
            
            for future in as_completed(futures):
                extract = futures[future]
                try:
                    df = future.result()
                    # Register as temp view
                    view_name = f"__source_{extract.source_name}__"
                    df.createOrReplaceTempView(view_name)
                except Exception as e:
                    raise RuntimeError(
                        f"Failed to extract from {extract.source_name} "
                        f"({extract.target}): {e}"
                    )
                    
        # Execute transform query
        result = spark.sql(plan.transform_query.sql)
        return result
        
    def _execute_extract(self, spark: "SparkSession",  # type: ignore
                         extract: ExtractQuery, 
                         credentials: dict) -> "DataFrame":  # type: ignore
        """Execute a single extract query."""
        jdbc_url = self.jdbc_builder.build_jdbc_url(
            credentials.get("type", extract.dialect),
            credentials
        )
        
        jdbc_props = self.jdbc_builder.get_jdbc_properties(
            credentials.get("type", extract.dialect),
            credentials
        )
        
        # Use pushdown query as subquery
        df = spark.read \
            .format("jdbc") \
            .option("url", jdbc_url) \
            .option("dbtable", f"({extract.sql}) AS __extract__") \
            .option("fetchsize", "10000")
            
        # Add connection properties
        for key, value in jdbc_props.items():
            df = df.option(key, value)
            
        return df.load()
        
    def write_to_target(self, df: "DataFrame",  # type: ignore
                        target_config: dict,
                        table_name: str, 
                        mode: str = "overwrite"):
        """
        Write DataFrame to target database via JDBC.
        
        Args:
            df: Spark DataFrame to write
            target_config: Target credentials
            table_name: Fully qualified table name
            mode: Write mode (overwrite, append, etc.)
        """
        adapter_type = target_config.get("type", "postgres")
        
        jdbc_url = self.jdbc_builder.build_jdbc_url(adapter_type, target_config)
        jdbc_props = self.jdbc_builder.get_jdbc_properties(adapter_type, target_config)
        
        writer = df.write \
            .format("jdbc") \
            .option("url", jdbc_url) \
            .option("dbtable", table_name)
            
        for key, value in jdbc_props.items():
            writer = writer.option(key, value)
            
        writer.mode(mode).save()


class FederatedModelRunner(ModelRunner):
    """
    ModelRunner that executes via Spark federation when needed.
    Falls back to standard adapter execution otherwise.
    """
    
    def __init__(self, config: RuntimeConfig, adapter, node: ModelNode, 
                 node_index: int, num_nodes: int,
                 resolver: FederationResolver,
                 planner: FederationPlanner,
                 spark_executor: SparkExecutor):
        super().__init__(config, adapter, node, node_index, num_nodes)
        self.resolver = resolver
        self.planner = planner
        self.spark_executor = spark_executor
        
    def execute(self, model: ModelNode, manifest: Manifest) -> RunResult:
        """
        Execute model - via federation if needed, otherwise via adapter.
        """
        if self.resolver.needs_federation(model):
            return self._execute_via_federation(model, manifest)
        else:
            # Standard adapter pushdown (existing dbt behavior)
            return super().execute(model, manifest)
            
    def _execute_via_federation(self, model: ModelNode, 
                                manifest: Manifest) -> RunResult:
        """Execute model via Spark federation."""
        warning_collector = FederationWarningCollector()
        
        # Check materialization - coerce view to table
        materialization = model.config.materialized
        if materialization == "view":
            warning_collector.warn_materialization_coerced(
                model.unique_id, "view", "table"
            )
            # Model will be materialized as table
            
        start_time = time.time()
        
        try:
            # Get source targets
            source_targets = self.resolver.get_source_targets(model)
            model_target = self.resolver.get_model_target(model)
            
            # Create execution plan
            plan = self.planner.plan(
                model, manifest, self.config,
                source_targets, model_target, warning_collector
            )
            
            # Get credentials for all involved targets
            credentials = self._get_credentials_for_targets(plan.get_involved_targets())
            
            # Execute via Spark
            result_df = self.spark_executor.execute_plan(plan, credentials)
            
            # Write to target
            target_relation = self._get_target_relation(model)
            self.spark_executor.write_to_target(
                result_df,
                credentials[model_target],
                str(target_relation),
                mode="overwrite"
            )
            
            elapsed = time.time() - start_time
            
            # Emit warnings
            warning_collector.emit_all()
            
            return RunResult(
                node=model,
                status=RunStatus.Success,
                timing=[],
                thread_id=threading.current_thread().name,
                execution_time=elapsed,
                message=f"SUCCESS (federation: {len(plan.extract_queries)} sources)",
                adapter_response={},
                failures=None,
                batch_results=None,
            )
            
        except SparkNotInstalledError as e:
            elapsed = time.time() - start_time
            return RunResult(
                node=model,
                status=RunStatus.Error,
                timing=[],
                thread_id=threading.current_thread().name,
                execution_time=elapsed,
                message=str(e),
                adapter_response={},
                failures=1,
                batch_results=None,
            )
            
        except Exception as e:
            elapsed = time.time() - start_time
            return RunResult(
                node=model,
                status=RunStatus.Error,
                timing=[],
                thread_id=threading.current_thread().name,
                execution_time=elapsed,
                message=f"Federation failed: {e}",
                adapter_response={},
                failures=1,
                batch_results=None,
            )
            
    def _get_credentials_for_targets(self, targets: set) -> Dict[str, dict]:
        """Get credentials for all targets from config."""
        credentials = {}
        for target in targets:
            # In real implementation, look up from profiles
            creds = self._get_target_credentials(target)
            if creds:
                credentials[target] = creds
        return credentials
        
    def _get_target_credentials(self, target: str) -> Optional[dict]:
        """Get credentials for a specific target."""
        # Would look up from self.config.credentials
        # Simplified for now
        return getattr(self.config, 'credentials', {})
        
    def _get_target_relation(self, model: ModelNode) -> str:
        """Get the target relation (schema.table) for the model."""
        schema = model.schema
        name = model.name
        return f"{schema}.{name}"
```

---

## 7. Integration Points

### 7.1 Hook into RunTask

Modify `core/dvt/task/run.py` to use FederatedModelRunner:

```python
# In RunTask or factory method:

def _create_model_runner(self, node: ModelNode) -> ModelRunner:
    """Create appropriate runner for the model."""
    
    # Check if federation infrastructure is available
    if self._federation_enabled():
        resolver = self._get_federation_resolver()
        
        # Check if this specific model needs federation
        if resolver.needs_federation(node):
            return FederatedModelRunner(
                config=self.config,
                adapter=self.adapter,
                node=node,
                node_index=self.node_index,
                num_nodes=self.num_nodes,
                resolver=resolver,
                planner=self._get_federation_planner(),
                spark_executor=self._get_spark_executor(),
            )
            
    # Default to standard ModelRunner
    return ModelRunner(
        config=self.config,
        adapter=self.adapter,
        node=node,
        node_index=self.node_index,
        num_nodes=self.num_nodes,
    )
```

---

## 8. Testing Strategy

### Unit Tests (`tests/unit/federation/`)

```
tests/unit/federation/
|-- test_resolver.py       # Federation decision logic
|-- test_dialect.py        # SQLGlot translation
|-- test_types.py          # Type mapping
|-- test_planner.py        # Query decomposition
|-- test_jdbc.py           # JDBC URL building
+-- test_warnings.py       # Warning emission
```

### Key Test Cases

1. **Resolver Tests:**
   - Same-target sources -> no federation
   - Different-target sources -> federation
   - Mixed sources (some same, some different) -> federation
   - CLI target override forces federation

2. **Type Mapping Tests:**
   - Known type chains (postgres -> spark -> snowflake)
   - Unknown types fall back to STRING
   - Precision loss warnings emitted

3. **Dialect Translation Tests:**
   - Function translation (PARSE_JSON, DATEDIFF)
   - Predicate translation
   - Identifier quoting differences

4. **Planner Tests:**
   - Predicate pushdown analysis
   - Multi-source query decomposition
   - Cross-source join detection

---

## 9. Implementation Timeline

```
Week 1: Foundation
|-- Day 1-2: resolver.py + warnings.py
|-- Day 3-4: jdbc.py + types.py
+-- Day 5: Unit tests for Phase 1

Week 2: Query Analysis  
|-- Day 1-2: dialect.py (SQLGlot integration)
|-- Day 3-4: planner.py (query decomposition)
+-- Day 5: Unit tests for Phase 2

Week 3: Execution Engine
|-- Day 1-2: spark.py (session management)
|-- Day 3-4: executor.py (FederatedModelRunner)
+-- Day 5: Unit tests for Phase 3

Week 4: Integration & Testing
|-- Day 1-2: Hook into RunTask
|-- Day 3-4: Integration tests
+-- Day 5: Documentation, cleanup
```

---

## 10. Success Criteria

The federation engine is complete when:

**Functional:**
- [ ] Models with same-target sources execute via adapter (existing behavior)
- [ ] Models with cross-target sources execute via Spark federation
- [ ] Predicates are pushed down to source databases
- [ ] Types are mapped correctly (with warnings for loss)
- [ ] Functions are translated between dialects

**Observability:**
- [ ] Clear warnings for materialization coercion (DVT001)
- [ ] Clear warnings for type precision loss (DVT002)
- [ ] Clear warnings for function translation (DVT003)
- [ ] Execution time reported correctly

**Error Handling:**
- [ ] Clear error if Spark not installed
- [ ] Clear error if JDBC driver missing
- [ ] Graceful handling of connection failures

**Testing:**
- [ ] Unit tests for all modules (>80% coverage)
- [ ] Integration tests for Postgres -> Snowflake flow
- [ ] Integration tests for multi-source federation

---

## 11. Dependencies

### External Dependencies

| Package | Version | Purpose |
|---------|---------|---------|
| `sqlglot` | `>=20.0.0` | SQL parsing/transpilation |
| `pyspark` | `>=3.5.0` | Spark execution engine |
| JDBC JARs | Various | Database connectivity |

### Internal Dependencies

| Component | Depends On |
|-----------|------------|
| `resolver.py` | manifest, config |
| `jdbc.py` | `jdbc_drivers.py` |
| `types.py` | None |
| `dialect.py` | sqlglot |
| `planner.py` | `dialect.py`, `types.py` |
| `spark.py` | pyspark, computes.yml |
| `executor.py` | `spark.py`, `jdbc.py`, `planner.py` |

---

## 12. References

- DVT Rules: `hesham_mind/dvt_rules.md`
- JDBC Drivers: `core/dvt/task/jdbc_drivers.py`
- SQLGlot Documentation: https://sqlglot.com/
- Spark JDBC Documentation: https://spark.apache.org/docs/latest/sql-data-sources-jdbc.html
