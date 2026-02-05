# Add Configuration Option

Use this prompt when adding a new configuration option to DVT.

## Context

I need to add a new config option `<option_name>` for `<purpose>`.

## Configuration Layers

DVT has multiple configuration layers (in precedence order):

1. **CLI arguments** - Highest priority
2. **Model config** - Per-model in SQL/YAML
3. **Project config** - `dbt_project.yml`
4. **Profile config** - `profiles.yml` / `computes.yml`

## Implementation Checklist

### 1. CLI Parameter (if applicable)
```python
# core/dvt/cli/params.py
@click.option(
    "--my-option",
    type=str,
    default=None,
    help="Description of option",
)
def my_option(f):
    return f
```

### 2. Project Config
```python
# core/dvt/config/project.py
# Add to RuntimeConfig or Project class
```

### 3. Profile Config
```python
# core/dvt/config/profile.py
# Add to Profile class if target/connection related
```

### 4. Model Config
```python
# core/dvt/contracts/graph/model_config.py
# Add to ModelConfig dataclass
```

### 5. Resolution Logic
```python
# Implement resolution per DVT rules
# CLI > model config > project config > profile default
```

## Key Files

- `core/dvt/cli/params.py` - CLI parameters
- `core/dvt/cli/main.py` - CLI commands
- `core/dvt/config/project.py` - Project configuration
- `core/dvt/config/profile.py` - Profile configuration
- `core/dvt/contracts/graph/model_config.py` - Model config contracts
- `core/dvt/flags.py` - Global flags

## DVT Rules

- **Rule 1**: Compute resolution hierarchy
- **Rule 2**: Target resolution hierarchy
- **Rule 10**: File locations

## Agent

Use `dev-team-backend` when adding configuration options.
