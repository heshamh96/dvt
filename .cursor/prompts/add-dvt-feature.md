# Add DVT Feature

Use this prompt when implementing a new DVT feature.

## Context

I need to implement `<feature_name>` for DVT.

## Feature Implementation Checklist

### 1. Review Requirements
- [ ] Check `dvt-core-features/<feature>/FEATURE.md` if exists
- [ ] Review relevant DVT RULES in `docs/dvt_implementation_plan.md`
- [ ] Identify affected areas (CLI, config, parser, task, etc.)

### 2. Design Phase
- [ ] Determine which agents are involved (see `.cursor/rules/`)
- [ ] Identify configuration changes needed
- [ ] Plan CLI interface (if any)
- [ ] Consider dbt compatibility

### 3. Implementation

#### Configuration (if needed)
```python
# core/dvt/config/ - Add config loading
# core/dvt/contracts/ - Add dataclass if new structure
```

#### CLI (if needed)
```python
# core/dvt/cli/main.py - Add command
# core/dvt/cli/params.py - Add parameters
```

#### Task (if needed)
```python
# core/dvt/task/<feature>.py - Implement task
# Extend appropriate base: BaseTask, ConfiguredTask, GraphRunnableTask
```

#### Parser (if needed)
```python
# core/dvt/parser/ - Add parsing logic
```

### 4. Testing
- [ ] Add unit tests in `tests/unit/`
- [ ] Add functional tests in `tests/functional/` if DB required
- [ ] Create trial in Testing_Playground

### 5. Documentation
- [ ] Update CLAUDE.md if user-facing
- [ ] Add/update docs in `docs/`
- [ ] Create changie entry: `changie new`

## DVT Rules Compliance

Ensure feature follows:
- **Rule 1**: Compute resolution hierarchy
- **Rule 2**: Target resolution hierarchy
- **Rule 3**: Execution path resolution
- **Rule 10**: File locations

## Testing Strategy

1. **Unit tests**: Test individual functions
2. **Functional tests**: Test end-to-end with DB
3. **Trial validation**: Manual testing in Testing_Playground
4. **Negative testing**: Test error cases

## Agent Coordination

| Phase | Agent |
|-------|-------|
| Design | `dev-team-architecture` |
| CLI | `dev-team-backend` or `dev-team-cli` |
| Parser | `dev-team-parser` |
| Testing | `dev-team-qa` |
| Federation | `dev-team-federation` |
| Docs | `dev-team-docs` |
