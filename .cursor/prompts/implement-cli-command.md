# Implement CLI Command

Use this prompt when implementing a new DVT CLI command.

## Context

I need to implement the `dvt <command>` CLI command.

## Requirements

1. **Follow DVT CLI patterns**:
   - CLI entry in `core/dvt/cli/main.py`
   - Task implementation in `core/dvt/task/<command>.py`
   - Use Click decorators consistent with existing commands

2. **Reference files**:
   - Existing command example: `core/dvt/cli/main.py` (see `@cli.command()` decorators)
   - Task hierarchy: `core/dvt/task/README.md`
   - DVT RULES: `docs/dvt_implementation_plan.md` (especially Rule 7)

3. **Implementation checklist**:
   - [ ] Add Click command in `main.py`
   - [ ] Create Task class extending appropriate base (BaseTask, ConfiguredTask, etc.)
   - [ ] Add CLI parameters per existing patterns
   - [ ] Handle errors with proper exception types
   - [ ] Add unit tests in `tests/unit/`
   - [ ] Update CLAUDE.md if command changes user workflow

4. **DVT-specific considerations**:
   - Respect target resolution hierarchy (CLI > model config > profiles.yml)
   - Respect compute resolution hierarchy (CLI > model config > computes.yml)
   - Preserve dbt-core compatibility where applicable

## Example

```python
# In main.py
@cli.command("mycommand")
@click.pass_context
@p.common_options
def mycommand(ctx, **kwargs):
    """Description of command."""
    return run_task(ctx, MyCommandTask, kwargs)

# In task/mycommand.py
class MyCommandTask(ConfiguredTask):
    def run(self) -> None:
        # Implementation
        pass
```

## Agent

Use `dev-team-backend` rules when implementing CLI commands.
