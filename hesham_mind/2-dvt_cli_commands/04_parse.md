# DVT CLI Command: `dvt parse`

## Phase: 1 (Foundation)
## Type: Delta (modifications to existing implementation)

---

## Current Implementation Summary

**File**: `core/dvt/task/parse.py`
**Parser Files**: `core/dvt/parser/`

The current `dvt parse` command:
1. Reads all YAML/SQL files in the project
2. Validates schema and syntax
3. Constructs Python objects for nodes (models, sources, tests, etc.)
4. Builds the manifest.json
5. Reports parsing errors and warnings

---

## Required Changes

### Delta: Validate Sources Have `connection` Property

DVT requires all sources to specify which target they connect to via the `connection` property. The parse command must validate this and error if missing.

### Reference Documents
- `dvt_rules.md` - Section: "Source Connection Requirement"
- `federation_execution_plan.md` - Section: "Source Resolution"

---

## Implementation Details

### 1. Source Schema Validation

**File**: `core/dvt/contracts/graph/unparsed.py`

The `connection` property already exists at line ~360. We need to make it required during parsing.

```python
@dataclass
class UnparsedSourceDefinition(HasYamlMetadata):
    name: str
    description: str = ""
    database: Optional[str] = None
    schema_: Optional[str] = None
    
    # DVT: Connection property - REQUIRED
    connection: Optional[str] = None  # Target name from profiles.yml
    
    tables: List[UnparsedSourceTableDefinition] = field(default_factory=list)
    # ... other fields
```

### 2. Parse-Time Validation

**File**: `core/dvt/parser/sources.py`

Add validation in the source parser:

```python
class SourceParser(ConfiguredParser):
    def parse_source(self, source: UnparsedSourceDefinition) -> ParsedSourceDefinition:
        # Existing parsing logic...
        
        # DVT: Validate connection property
        self._validate_connection(source)
        
        # Continue with existing logic...
        return parsed_source
    
    def _validate_connection(self, source: UnparsedSourceDefinition) -> None:
        """Validate that source has a connection property.
        
        DVT requires all sources to specify which target they connect to.
        This enables the federation resolver to determine execution paths.
        
        Raises:
            ParsingError: If connection is missing or invalid
        """
        if source.connection is None:
            raise ParsingError(
                f"Source '{source.name}' is missing required 'connection' property. "
                f"DVT requires all sources to specify which target they connect to.\n"
                f"Example:\n"
                f"  sources:\n"
                f"    - name: {source.name}\n"
                f"      connection: my_target  # Add this line\n"
                f"      tables:\n"
                f"        - name: my_table"
            )
        
        # Validate connection refers to a valid target
        available_targets = self._get_available_targets()
        if source.connection not in available_targets:
            raise ParsingError(
                f"Source '{source.name}' has connection '{source.connection}' "
                f"but this target does not exist in profiles.yml.\n"
                f"Available targets: {', '.join(available_targets)}"
            )
    
    def _get_available_targets(self) -> List[str]:
        """Get list of available targets from profiles.yml."""
        # Access runtime config or profile
        profile = self.root_project.profile
        return list(profile.targets.keys()) if profile else []
```

### 3. Helpful Error Messages

**File**: `core/dvt/exceptions.py`

Add specific exception for connection validation:

```python
class SourceConnectionMissingError(ParsingError):
    """Raised when a source is missing the required connection property."""
    
    def __init__(self, source_name: str, file_path: str):
        self.source_name = source_name
        self.file_path = file_path
        super().__init__(self._build_message())
    
    def _build_message(self) -> str:
        return (
            f"Source '{self.source_name}' in {self.file_path} is missing "
            f"required 'connection' property.\n\n"
            f"DVT requires all sources to specify which database target they connect to. "
            f"This enables cross-database federation.\n\n"
            f"Fix: Add a 'connection' property to your source:\n\n"
            f"  sources:\n"
            f"    - name: {self.source_name}\n"
            f"      connection: <target_name>  # e.g., 'postgres_prod', 'snowflake_dw'\n"
            f"      tables:\n"
            f"        - name: ...\n\n"
            f"The connection value must match a target defined in your profiles.yml."
        )


class SourceConnectionInvalidError(ParsingError):
    """Raised when a source's connection refers to a non-existent target."""
    
    def __init__(self, source_name: str, connection: str, available_targets: List[str]):
        self.source_name = source_name
        self.connection = connection
        self.available_targets = available_targets
        super().__init__(self._build_message())
    
    def _build_message(self) -> str:
        targets_list = "\n  - ".join(self.available_targets) if self.available_targets else "(none)"
        return (
            f"Source '{self.source_name}' has connection '{self.connection}' "
            f"but this target does not exist.\n\n"
            f"Available targets in profiles.yml:\n  - {targets_list}\n\n"
            f"Fix: Update the connection to use a valid target name."
        )
```

### 4. Event for Validation

**File**: `core/dvt/events/types.py`

```python
@dataclass
class SourceConnectionValidated(DebugLevel):
    source_name: str
    connection: str
    
    def message(self) -> str:
        return f"Source '{self.source_name}' validated: connection='{self.connection}'"


@dataclass
class SourceConnectionMissing(ErrorLevel):
    source_name: str
    file_path: str
    
    def message(self) -> str:
        return (
            f"Source '{self.source_name}' in {self.file_path} is missing "
            f"required 'connection' property"
        )
```

---

## CLI Output Changes

### Success Case (all sources have connections)

```
$ dvt parse
Found 3 sources with valid connections:
  - external_users (connection: postgres_prod)
  - analytics_events (connection: bigquery_analytics)
  - legacy_orders (connection: mysql_legacy)
Parsing complete: 15 models, 3 sources, 8 tests
```

### Error Case (missing connection)

```
$ dvt parse
Parsing error in models/staging/sources.yml

Source 'external_users' is missing required 'connection' property.

DVT requires all sources to specify which database target they connect to.
This enables cross-database federation.

Fix: Add a 'connection' property to your source:

  sources:
    - name: external_users
      connection: <target_name>  # e.g., 'postgres_prod', 'snowflake_dw'
      tables:
        - name: ...

The connection value must match a target defined in your profiles.yml.
```

### Error Case (invalid connection)

```
$ dvt parse
Parsing error in models/staging/sources.yml

Source 'external_users' has connection 'postgres_prd' but this target does not exist.

Available targets in profiles.yml:
  - postgres_prod
  - postgres_dev
  - snowflake_dw

Fix: Update the connection to use a valid target name.
```

---

## Example Valid Source Configuration

```yaml
# models/staging/_sources.yml
version: 2

sources:
  - name: postgres_users
    connection: postgres_prod      # Required: specifies target
    database: user_db
    schema: public
    tables:
      - name: users
      - name: accounts

  - name: bigquery_events
    connection: bigquery_analytics  # Required: specifies target
    database: analytics
    schema: events
    tables:
      - name: page_views
      - name: clicks

  - name: snowflake_orders
    connection: snowflake_dw        # Required: specifies target
    tables:
      - name: orders
      - name: order_items
```

---

## Testing Requirements

### Unit Tests

**File**: `tests/unit/test_source_parser.py`

```python
def test_parse_source_with_connection():
    """Test that sources with connection property parse successfully."""
    source_dict = {
        "name": "my_source",
        "connection": "postgres_prod",
        "tables": [{"name": "users"}],
    }
    
    parser = SourceParser(...)
    parsed = parser.parse_source(UnparsedSourceDefinition(**source_dict))
    
    assert parsed.connection == "postgres_prod"


def test_parse_source_missing_connection_raises():
    """Test that sources without connection property raise error."""
    source_dict = {
        "name": "my_source",
        # connection missing
        "tables": [{"name": "users"}],
    }
    
    parser = SourceParser(...)
    
    with pytest.raises(SourceConnectionMissingError) as exc_info:
        parser.parse_source(UnparsedSourceDefinition(**source_dict))
    
    assert "my_source" in str(exc_info.value)
    assert "connection" in str(exc_info.value)


def test_parse_source_invalid_connection_raises():
    """Test that sources with invalid connection raise error."""
    source_dict = {
        "name": "my_source",
        "connection": "nonexistent_target",
        "tables": [{"name": "users"}],
    }
    
    parser = SourceParser(...)
    # Mock available targets
    parser._get_available_targets = lambda: ["postgres_prod", "postgres_dev"]
    
    with pytest.raises(SourceConnectionInvalidError) as exc_info:
        parser.parse_source(UnparsedSourceDefinition(**source_dict))
    
    assert "nonexistent_target" in str(exc_info.value)
    assert "postgres_prod" in str(exc_info.value)
```

### Functional Tests

**File**: `tests/functional/test_parse_source_connection.py`

```python
def test_parse_fails_on_missing_connection(project):
    """Test that parse command fails when source is missing connection."""
    write_file(
        project.project_root / "models" / "_sources.yml",
        """
version: 2
sources:
  - name: my_source
    tables:
      - name: users
"""
    )
    
    result = run_dbt(["parse"], expect_pass=False)
    
    assert "missing required 'connection' property" in result.output


def test_parse_succeeds_with_valid_connection(project):
    """Test that parse command succeeds with valid connection."""
    write_file(
        project.project_root / "models" / "_sources.yml",
        """
version: 2
sources:
  - name: my_source
    connection: default
    tables:
      - name: users
"""
    )
    
    result = run_dbt(["parse"])
    
    assert result.success
```

---

## Migration Considerations

### For Existing dbt Projects

Projects migrating from dbt to DVT will need to add `connection` properties to all their sources. Provide a helpful migration message:

```python
class MigrationHelper:
    @staticmethod
    def generate_connection_suggestions(sources: List[UnparsedSourceDefinition]) -> str:
        """Generate suggestions for adding connections to sources."""
        suggestions = []
        for source in sources:
            if source.connection is None:
                suggestions.append(
                    f"  - name: {source.name}\n"
                    f"    connection: <add_target_here>  # TODO: Add target from profiles.yml"
                )
        
        if suggestions:
            return (
                "The following sources need 'connection' properties:\n\n"
                + "\n".join(suggestions)
            )
        return ""
```

---

## Dependencies

- None (parse is a foundation command)

## Blocked By

- None

## Blocks

- `05_seed.md` - seed needs parsed sources with connections
- `06_run.md` - run needs parsed sources with connections for federation resolution
- `09_compile.md` - compile needs parsed sources with connections

---

## Implementation Checklist

- [ ] Add `_validate_connection()` method to `SourceParser`
- [ ] Create `SourceConnectionMissingError` exception
- [ ] Create `SourceConnectionInvalidError` exception
- [ ] Add validation events
- [ ] Update source schema documentation
- [ ] Add unit tests for connection validation
- [ ] Add functional tests for parse command
- [ ] Add migration helper for dbt projects
- [ ] Update error messages with helpful examples
